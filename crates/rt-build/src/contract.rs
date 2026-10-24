//! The RPC surface of a Deno module as TypeScript declarations: what `21ci types <service>` and `21ci sdk types <name>` download.
//!
//! The surface is read from the source, never run: the module's `export const rpc = { … }`, the
//! `rpc` of an object default export, or the methods of `defineSdk({ methods: { … } })`. Each
//! method (a method, an arrow or function property, or a shorthand naming a top-level function)
//! becomes `name(arg: <its first parameter's type>): Promise<<its return type>>`; nested objects
//! become namespaces. A missing annotation is `unknown`. The module's own interfaces and type
//! aliases (of every TypeScript file of the upload) are copied so the signatures resolve.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use deno_ast::swc::ast::{
    Callee, Decl, Expr, Function, Module, ModuleDecl, ModuleItem, ObjectLit, Pat, Prop, PropName, PropOrSpread, Stmt,
    TsTypeAnn,
};
use deno_ast::swc::common::Spanned;
use deno_ast::{MediaType, ParseParams, SourceRange, SourceTextInfo};
use url::Url;

use crate::builder::UploadLimits;
use crate::extract;

/// One RPC method: its dotted path and its TypeScript signature parts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcMethod {
    /// `invoices.create`
    pub path: String,
    /// The first parameter's type; `None` when the method takes none.
    pub param: Option<String>,
    /// The parameter may be left out (`arg?:` or a default value).
    pub optional: bool,
    /// The resolved type of the result (`Promise<T>` is `T`).
    pub returns: String,
}

/// The RPC surface of a module.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RpcContract {
    pub methods: Vec<RpcMethod>,
    /// Interface and type alias declarations, as written (without `export`).
    pub declarations: Vec<String>,
    /// `defineSdk` methods that never read the caller while the SDK has no `authorize` hook: the
    /// runtime refuses such an SDK (`sdk_unchecked_caller`), so a publish does too.
    pub unchecked: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContractError {
    /// No `rpc` export and no `defineSdk(...)` call.
    #[error("the module has no RPC surface: export `rpc` or `defineSdk({{ methods }})`")]
    NoSurface,
    #[error("{0}")]
    Invalid(String),
}

impl RpcContract {
    /// The top-level names (methods and namespaces), in order.
    pub fn members(&self) -> Vec<String> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for m in &self.methods {
            let top = m.path.split('.').next().unwrap_or_default().to_string();
            if seen.insert(top.clone()) {
                out.push(top);
            }
        }
        out
    }

    fn declared(&self, name: &str) -> bool {
        self.declarations.iter().any(|d| decl_name(d) == Some(name))
    }

    /// The surface as one interface body (`  name(arg: T): Promise<R>;` and nested namespaces).
    fn interface_body(&self, indent: &str) -> String {
        #[derive(Default)]
        struct Node {
            sig: Option<String>,
            children: BTreeMap<String, Node>,
            order: Vec<String>,
        }
        let mut root = Node::default();
        for m in &self.methods {
            let mut node = &mut root;
            for part in m.path.split('.') {
                if !node.children.contains_key(part) {
                    node.order.push(part.to_string());
                }
                node = node.children.entry(part.to_string()).or_default();
            }
            let arg = match &m.param {
                None => String::new(),
                Some(t) => format!("arg{}: {t}", if m.optional { "?" } else { "" }),
            };
            node.sig = Some(format!("({arg}): Promise<{}>;", m.returns));
        }
        fn render(node: &Node, indent: &str, out: &mut String) {
            for name in &node.order {
                let child = &node.children[name];
                match &child.sig {
                    Some(sig) if child.children.is_empty() => out.push_str(&format!("{indent}{name}{sig}\n")),
                    _ => {
                        out.push_str(&format!("{indent}{name}: {{\n"));
                        render(child, &format!("{indent}  "), out);
                        out.push_str(&format!("{indent}}};\n"));
                    }
                }
            }
        }
        let mut out = String::new();
        render(&root, indent, &mut out);
        out
    }

    fn interface_name(&self, base: &str, suffix: &str) -> String {
        let pascal = pascal_case(base);
        let name = format!("{pascal}{suffix}");
        if self.declared(&name) {
            format!("{name}Contract")
        } else {
            name
        }
    }

    /// The `.d.ts` of a service's RPC surface (`21ci types <service>`): its types and an
    /// interface `<Slug>Rpc` to cast `services.bind(slug)` to.
    pub fn render_service(&self, slug: &str) -> String {
        let iface = self.interface_name(slug, "Rpc");
        let mut out = format!(
            "// RPC contract of the service `{slug}`, generated by the runtime from its `rpc` export.\n\
             // Use it as: const {var} = services.bind(\"{slug}\") as {iface};\n\n",
            var = camel_case(slug),
        );
        for d in &self.declarations {
            out.push_str(&format!("export {d}\n\n"));
        }
        out.push_str(&format!(
            "export interface {iface} {{\n{}}}\n",
            self.interface_body("  ")
        ));
        out
    }

    /// The `.d.ts` of an operator SDK release (`21ci sdk types <name>`): ambient modules for
    /// `sdk:<name>` and the `std:sdk` named export.
    pub fn render_sdk(&self, name: &str, version: &str) -> String {
        let iface = self.interface_name(name, "");
        let var = camel_case(name);
        let mut out = format!(
            "// Contract of the operator SDK `{name}` {version}, generated by the runtime. Types only: the SDK\n\
             // runs in the runtime and is bound to the release your grants select when it is called.\n\
             //   import {{ {var} }} from \"std:sdk\";     or     import {var} from \"sdk:{name}\";\n\n\
             declare module \"sdk:{name}\" {{\n"
        );
        for d in &self.declarations {
            for line in format!("export {d}").lines() {
                out.push_str(&format!("  {line}\n"));
            }
            out.push('\n');
        }
        out.push_str(&format!(
            "  export interface {iface} {{\n{}  }}\n",
            self.interface_body("    ")
        ));
        out.push_str(&format!("  const {var}: {iface};\n  export default {var};\n"));
        for m in self.members() {
            out.push_str(&format!("  export const {m}: {iface}[\"{m}\"];\n"));
        }
        out.push_str(&format!(
            "}}\n\ndeclare module \"std:sdk\" {{\n  export const {var}: import(\"sdk:{name}\").{iface};\n}}\n"
        ));
        out
    }
}

/// `acme-billing` / `acme_billing` -> `AcmeBilling`.
fn pascal_case(s: &str) -> String {
    s.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut c = p.chars();
            c.next()
                .map(|f| f.to_ascii_uppercase().to_string() + c.as_str())
                .unwrap_or_default()
        })
        .collect()
}

/// `acme-billing` -> `acmeBilling`.
fn camel_case(s: &str) -> String {
    let p = pascal_case(s);
    let mut c = p.chars();
    c.next()
        .map(|f| f.to_ascii_lowercase().to_string() + c.as_str())
        .unwrap_or_default()
}

/// The name an `interface X` / `type X` declaration declares.
fn decl_name(d: &str) -> Option<&str> {
    let rest = d.strip_prefix("interface ").or_else(|| d.strip_prefix("type "))?;
    let end = rest
        .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .unwrap_or(rest.len());
    Some(&rest[..end])
}

/// The contract of an upload as it is deployed (`content_type` selects the format, like a version
/// upload): the entry module's surface, with the declarations of every TypeScript file.
pub fn extract_upload(
    content_type: Option<&str>,
    body: &[u8],
    limits: &UploadLimits,
) -> Result<RpcContract, ContractError> {
    let ex = extract::extract(content_type, body, limits).map_err(|e| ContractError::Invalid(format!("{e:?}")))?;
    extract_files(&ex.files, &ex.entry)
}

/// The contract of `entry` among `files` (normalized path -> content).
pub fn extract_files(files: &BTreeMap<String, Bytes>, entry: &str) -> Result<RpcContract, ContractError> {
    let text = files
        .get(entry)
        .ok_or_else(|| ContractError::Invalid(format!("no entry module `{entry}`")))?;
    let text = std::str::from_utf8(text).map_err(|_| ContractError::Invalid(format!("`{entry}` is not UTF-8")))?;
    let mut contract = extract_module(entry, text)?;
    let mut names: BTreeSet<String> = contract
        .declarations
        .iter()
        .filter_map(|d| decl_name(d).map(str::to_string))
        .collect();
    for (path, bytes) in files {
        if path == entry
            || !matches!(
                crate::sniff::media_type_of(path),
                MediaType::TypeScript | MediaType::Mts | MediaType::Tsx
            )
        {
            continue;
        }
        let Ok(text) = std::str::from_utf8(bytes) else { continue };
        let Ok(module) = Source::parse(path, text) else {
            continue;
        };
        for d in module.declarations() {
            if decl_name(&d).is_some_and(|n| names.insert(n.to_string())) {
                contract.declarations.push(d);
            }
        }
    }
    Ok(contract)
}

/// The contract of one module's source.
pub fn extract_module(file: &str, text: &str) -> Result<RpcContract, ContractError> {
    let src = Source::parse(file, text)?;
    let (table, check_caller) = src.rpc_table().ok_or(ContractError::NoSurface)?;
    let mut methods = Vec::new();
    let mut unchecked = Vec::new();
    let mut cx = Collect {
        methods: &mut methods,
        unchecked: check_caller.then_some(&mut unchecked),
    };
    src.collect(table, "", &mut cx, 0);
    if methods.is_empty() {
        return Err(ContractError::NoSurface);
    }
    Ok(RpcContract {
        methods,
        declarations: src.declarations(),
        unchecked,
    })
}

struct Collect<'a> {
    methods: &'a mut Vec<RpcMethod>,
    /// `Some` for `defineSdk` without `authorize`: methods that never read `caller` go here.
    unchecked: Option<&'a mut Vec<String>>,
}

/// Does the source text of a method read `caller` (`ctx.caller`, `{ caller }`)?
fn reads_caller(text: &str) -> bool {
    let b = text.as_bytes();
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    text.match_indices("caller")
        .any(|(i, _)| (i == 0 || !ident(b[i - 1])) && b.get(i + 6).is_none_or(|c| !ident(*c)))
}

struct Source {
    parsed: deno_ast::ParsedSource,
    text_info: SourceTextInfo,
}

impl Source {
    fn parse(file: &str, text: &str) -> Result<Self, ContractError> {
        let specifier = Url::parse(&format!("file:///{}", file.trim_start_matches('/')))
            .map_err(|_| ContractError::Invalid(format!("bad file name `{file}`")))?;
        let media_type = match crate::sniff::media_type_of(file) {
            MediaType::Unknown => MediaType::TypeScript,
            m => m,
        };
        let parsed = deno_ast::parse_module(ParseParams {
            specifier,
            text: Arc::from(text),
            media_type,
            capture_tokens: false,
            scope_analysis: false,
            maybe_syntax: None,
        })
        .map_err(|d| ContractError::Invalid(crate::analyze::parse_diag(file, &d).message))?;
        let text_info = parsed.text_info_lazy().clone();
        Ok(Self { parsed, text_info })
    }

    fn module(&self) -> Option<&Module> {
        match self.parsed.program_ref() {
            deno_ast::ProgramRef::Module(m) => Some(m),
            // LCOV_EXCL_START: `Source::parse` uses `parse_module` (`ParseMode::Module`): never a script
            deno_ast::ProgramRef::Script(_) => None,
            // LCOV_EXCL_STOP
        }
    }

    fn text(&self, span: deno_ast::swc::common::Span) -> String {
        let range = SourceRange::unsafely_from_span(span);
        let bytes = range.as_byte_range(self.text_info.range().start);
        self.text_info.text_str().get(bytes).unwrap_or_default().to_string()
    }

    /// Interfaces and type aliases at the top level (exported or not), as written.
    fn declarations(&self) -> Vec<String> {
        let mut out = Vec::new();
        for item in self.module().map(|m| m.body.as_slice()).unwrap_or_default() {
            let decl = match item {
                ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(e)) => &e.decl,
                ModuleItem::Stmt(Stmt::Decl(d)) => d,
                _ => continue,
            };
            match decl {
                Decl::TsInterface(i) => out.push(self.text(i.span)),
                Decl::TsTypeAlias(t) => out.push(self.text(t.span)),
                _ => {}
            }
        }
        out
    }

    /// A top-level function declaration by name.
    fn function(&self, name: &str) -> Option<&Function> {
        for item in self.module().map(|m| m.body.as_slice()).unwrap_or_default() {
            let decl = match item {
                ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(e)) => &e.decl,
                ModuleItem::Stmt(Stmt::Decl(d)) => d,
                _ => continue,
            };
            if let Decl::Fn(f) = decl {
                if f.ident.sym.as_ref() == name {
                    return Some(&f.function);
                }
            }
        }
        None
    }

    /// The object literal of the RPC surface, and whether its methods must read the caller (a
    /// `defineSdk` without `authorize`).
    fn rpc_table(&self) -> Option<(&ObjectLit, bool)> {
        let body = self.module().map(|m| m.body.as_slice()).unwrap_or_default();
        // `defineSdk({ methods: { … } })` anywhere at the top level.
        for item in body {
            let expr: Option<&Expr> = match item {
                ModuleItem::ModuleDecl(ModuleDecl::ExportDefaultExpr(e)) => Some(&e.expr),
                ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(e)) => var_init(&e.decl, None),
                ModuleItem::Stmt(Stmt::Decl(d)) => var_init(d, None),
                _ => None,
            };
            if let Some(found) = expr.and_then(define_sdk_methods) {
                return Some(found);
            }
        }
        // `export const rpc = { … }`.
        for item in body {
            if let ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(e)) = item {
                if let Some(Expr::Object(o)) = var_init(&e.decl, Some("rpc")).map(unparen) {
                    return Some((o, false));
                }
            }
        }
        // `export default { rpc: { … } }`.
        for item in body {
            if let ModuleItem::ModuleDecl(ModuleDecl::ExportDefaultExpr(e)) = item {
                if let Expr::Object(o) = unparen(&e.expr) {
                    if let Some(Expr::Object(rpc)) = prop_value(o, "rpc").map(unparen) {
                        return Some((rpc, false));
                    }
                }
            }
        }
        None
    }

    fn type_text(&self, ann: Option<&TsTypeAnn>) -> Option<String> {
        ann.map(|a| self.text(a.type_ann.span()))
    }

    /// The parameter and result of a method from its first parameter and return annotation.
    fn signature(&self, path: String, param: Option<&Pat>, ret: Option<&TsTypeAnn>) -> RpcMethod {
        let (param, optional) = match param {
            None => (None, false),
            Some(p) => {
                let (ann, optional) = pat_type(p);
                (
                    Some(self.type_text(ann).unwrap_or_else(|| "unknown".to_string())),
                    optional,
                )
            }
        };
        let returns = self
            .type_text(ret)
            .map_or_else(|| "unknown".to_string(), |t| unwrap_promise(&t));
        RpcMethod {
            path,
            param,
            optional,
            returns,
        }
    }

    fn function_signature(&self, path: String, f: &Function) -> RpcMethod {
        self.signature(path, f.params.first().map(|p| &p.pat), f.return_type.as_deref())
    }

    fn collect(&self, obj: &ObjectLit, prefix: &str, cx: &mut Collect<'_>, depth: usize) {
        if depth > 8 {
            return;
        }
        for prop in &obj.props {
            let PropOrSpread::Prop(prop) = prop else { continue };
            // The method, and the source text it runs (to see whether it reads the caller).
            let (name, method, body) = match &**prop {
                Prop::Method(m) => match prop_name(&m.key) {
                    Some(n) => (
                        n.clone(),
                        self.function_signature(format!("{prefix}{n}"), &m.function),
                        self.text(m.function.span),
                    ),
                    None => continue,
                },
                Prop::Shorthand(id) => {
                    let n = id.sym.to_string();
                    match self.function(&n) {
                        Some(f) => (
                            n.clone(),
                            self.function_signature(format!("{prefix}{n}"), f),
                            self.text(f.span),
                        ),
                        None => (
                            n.clone(),
                            self.signature(format!("{prefix}{n}"), None, None),
                            String::new(),
                        ),
                    }
                }
                Prop::KeyValue(kv) => {
                    let Some(n) = prop_name(&kv.key) else { continue };
                    match unparen(&kv.value) {
                        Expr::Arrow(a) => (
                            n.clone(),
                            self.signature(format!("{prefix}{n}"), a.params.first(), a.return_type.as_deref()),
                            self.text(a.span),
                        ),
                        Expr::Fn(f) => (
                            n.clone(),
                            self.function_signature(format!("{prefix}{n}"), &f.function),
                            self.text(f.function.span),
                        ),
                        Expr::Ident(id) => match self.function(id.sym.as_ref()) {
                            Some(f) => (
                                n.clone(),
                                self.function_signature(format!("{prefix}{n}"), f),
                                self.text(f.span),
                            ),
                            None => continue,
                        },
                        Expr::Object(o) => {
                            self.collect(o, &format!("{prefix}{n}."), cx, depth + 1);
                            continue;
                        }
                        _ => continue,
                    }
                }
                _ => continue,
            };
            if valid_name(&name) {
                if let Some(unchecked) = cx.unchecked.as_deref_mut() {
                    if !reads_caller(&body) {
                        unchecked.push(method.path.clone());
                    }
                }
                cx.methods.push(method);
            }
        }
    }
}

fn valid_name(n: &str) -> bool {
    let b = n.as_bytes();
    !b.is_empty()
        && (b[0].is_ascii_alphabetic() || b[0] == b'_' || b[0] == b'$')
        && b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'$')
}

fn unparen(e: &Expr) -> &Expr {
    match e {
        Expr::Paren(p) => unparen(&p.expr),
        Expr::TsAs(a) => unparen(&a.expr),
        Expr::TsSatisfies(s) => unparen(&s.expr),
        Expr::TsConstAssertion(c) => unparen(&c.expr),
        other => other,
    }
}

fn prop_name(k: &PropName) -> Option<String> {
    match k {
        PropName::Ident(i) => Some(i.sym.to_string()),
        PropName::Str(s) => Some(s.value.to_string_lossy().into_owned()),
        _ => None,
    }
}

fn prop_value<'a>(o: &'a ObjectLit, name: &str) -> Option<&'a Expr> {
    o.props.iter().find_map(|p| match p {
        PropOrSpread::Prop(p) => match &**p {
            Prop::KeyValue(kv) if prop_name(&kv.key).as_deref() == Some(name) => Some(&*kv.value),
            _ => None,
        },
        _ => None,
    })
}

/// The initializer of a `const` declaration (of `name`, or any).
fn var_init<'a>(d: &'a Decl, name: Option<&str>) -> Option<&'a Expr> {
    let Decl::Var(v) = d else { return None };
    v.decls.iter().find_map(|decl| {
        let Pat::Ident(id) = &decl.name else { return None };
        if name.is_some_and(|n| id.id.sym.as_ref() != n) {
            return None;
        }
        decl.init.as_deref()
    })
}

/// `defineSdk({ methods: { … } })` -> the methods object, and whether every method must read the
/// caller (no `authorize`).
fn define_sdk_methods(e: &Expr) -> Option<(&ObjectLit, bool)> {
    let Expr::Call(call) = unparen(e) else { return None };
    let Callee::Expr(callee) = &call.callee else {
        return None;
    };
    let named = match unparen(callee) {
        Expr::Ident(i) => i.sym.as_ref() == "defineSdk",
        _ => false,
    };
    if !named {
        return None;
    }
    let Expr::Object(arg) = unparen(&call.args.first()?.expr) else {
        return None;
    };
    let authorize = arg.props.iter().any(|p| match p {
        PropOrSpread::Prop(p) => match &**p {
            Prop::Method(m) => prop_name(&m.key).as_deref() == Some("authorize"),
            Prop::KeyValue(kv) => prop_name(&kv.key).as_deref() == Some("authorize"),
            Prop::Shorthand(id) => id.sym.as_ref() == "authorize",
            _ => false,
        },
        _ => false,
    });
    match prop_value(arg, "methods").map(unparen) {
        Some(Expr::Object(o)) => Some((o, !authorize)),
        _ => None,
    }
}

/// The annotation of a parameter pattern and whether it may be left out.
fn pat_type(p: &Pat) -> (Option<&TsTypeAnn>, bool) {
    match p {
        Pat::Ident(b) => (b.type_ann.as_deref(), b.id.optional),
        Pat::Object(o) => (o.type_ann.as_deref(), o.optional),
        Pat::Array(a) => (a.type_ann.as_deref(), a.optional),
        Pat::Assign(a) => (pat_type(&a.left).0, true),
        _ => (None, false),
    }
}

/// `Promise<T>` -> `T`; anything else as it is.
fn unwrap_promise(t: &str) -> String {
    let t = t.trim();
    match t.strip_prefix("Promise<").and_then(|r| r.strip_suffix('>')) {
        Some(inner) => inner.trim().to_string(),
        None => t.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SDK: &str = r#"
import { defineSdk } from "std:sdk/server";
export interface PlatformInfo { platform: string; version: string }
type Amount = { cents: number };
interface Internal { x: 1 }
async function ping(): Promise<string> { return "pong"; }
export default defineSdk({
  authorize(caller) { return true; },
  methods: {
    ping,
    async getPlatformInfo(_args: void, ctx): Promise<PlatformInfo> { return null as any; },
    invoices: {
      create: async (args: { amount: Amount }, ctx) => ({ id: 1 }),
      list: async function (args?: { limit: number }): Promise<Array<{ id: number }>> { return []; },
    },
    "quoted": async (a: string = "x") => a,
  },
});
"#;

    #[test]
    fn define_sdk_methods_become_the_surface() {
        let c = extract_module("mod.ts", SDK).unwrap();
        let paths: Vec<_> = c.methods.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(
            paths,
            ["ping", "getPlatformInfo", "invoices.create", "invoices.list", "quoted"]
        );
        assert_eq!(
            c.methods[0],
            RpcMethod {
                path: "ping".into(),
                param: None,
                optional: false,
                returns: "string".into()
            }
        );
        assert_eq!(c.methods[1].param.as_deref(), Some("void"));
        assert_eq!(c.methods[1].returns, "PlatformInfo");
        assert_eq!(c.methods[2].param.as_deref(), Some("{ amount: Amount }"));
        assert_eq!(c.methods[2].returns, "unknown", "no annotation");
        assert_eq!(
            (c.methods[3].optional, c.methods[3].returns.as_str()),
            (true, "Array<{ id: number }>")
        );
        assert_eq!(
            (c.methods[4].param.as_deref(), c.methods[4].optional),
            (Some("string"), true)
        );
        assert_eq!(c.members(), ["ping", "getPlatformInfo", "invoices", "quoted"]);
        assert_eq!(c.declarations.len(), 3, "{:?}", c.declarations);
        assert!(c.declarations[0].starts_with("interface PlatformInfo"));
    }

    #[test]
    fn an_rpc_export_is_the_surface_of_a_service() {
        let src = r#"
export const rpc = {
  async add(args: { a: number; b: number }): Promise<{ sum: number }> { return { sum: args.a + args.b }; },
  nested: { async ping() { return "pong"; } },
};
export default { fetch() { return new Response("x"); } };
"#;
        let c = extract_module("main.ts", src).unwrap();
        let d = c.render_service("billing-api");
        assert!(d.contains("export interface BillingApiRpc {"), "{d}");
        assert!(
            d.contains("  add(arg: { a: number; b: number }): Promise<{ sum: number }>;"),
            "{d}"
        );
        assert!(d.contains("  nested: {\n    ping(): Promise<unknown>;\n  };"), "{d}");
        assert!(d.contains("services.bind(\"billing-api\") as BillingApiRpc"), "{d}");

        let default_rpc = "export default { rpc: { async hi(name: string) { return name; } } };";
        assert_eq!(extract_module("main.ts", default_rpc).unwrap().methods[0].path, "hi");
    }

    #[test]
    fn the_sdk_contract_declares_both_modules() {
        let d = extract_module("mod.ts", SDK).unwrap().render_sdk("acme", "1.1.0");
        assert!(d.contains("declare module \"sdk:acme\" {"), "{d}");
        assert!(d.contains("  export interface PlatformInfo {"), "{d}");
        assert!(
            d.contains("  export interface Acme {\n    ping(): Promise<string>;"),
            "{d}"
        );
        assert!(
            d.contains("    getPlatformInfo(arg: void): Promise<PlatformInfo>;"),
            "{d}"
        );
        assert!(d.contains("  export default acme;"), "{d}");
        assert!(d.contains("  export const invoices: Acme[\"invoices\"];"), "{d}");
        assert!(
            d.contains("declare module \"std:sdk\" {\n  export const acme: import(\"sdk:acme\").Acme;\n}"),
            "{d}"
        );
    }

    #[test]
    fn methods_that_never_read_the_caller_are_unchecked() {
        let src = "export default defineSdk({ methods: { async a(x, ctx) { return ctx.caller.tenant; }, \
                   async b({ n }, { caller }) { return caller; }, async c() { return 1; }, nested: { d: async () => 'callers' } } });";
        assert_eq!(extract_module("m.ts", src).unwrap().unchecked, ["c", "nested.d"]);
        let with_hook =
            "export default defineSdk({ authorize(c) { return true; }, methods: { async c() { return 1; } } });";
        assert!(extract_module("m.ts", with_hook).unwrap().unchecked.is_empty());
        let rpc = "export const rpc = { async c() { return 1; } };";
        assert!(
            extract_module("m.ts", rpc).unwrap().unchecked.is_empty(),
            "plain RPC services are not SDKs"
        );
    }

    #[test]
    fn no_surface_is_an_error() {
        assert_eq!(
            extract_module("m.ts", "export default { fetch() {} };").unwrap_err(),
            ContractError::NoSurface
        );
        // Each place the surface is looked for, with a default export that is not an `rpc` object.
        assert_eq!(
            extract_module("m.ts", "export default () => new Response('x');").unwrap_err(),
            ContractError::NoSurface
        );
        assert_eq!(
            extract_module("m.ts", "const a = 1;\nexport default { x: 1 };").unwrap_err(),
            ContractError::NoSurface
        );
        assert_eq!(
            extract_module("m.ts", "export default { rpc: 1 };").unwrap_err(),
            ContractError::NoSurface
        );
        assert!(matches!(
            extract_module("m.ts", "export const = ;").unwrap_err(),
            ContractError::Invalid(_)
        ));
    }

    #[test]
    fn declarations_come_from_every_typescript_file() {
        let mut files = BTreeMap::new();
        files.insert("main.ts".to_string(), Bytes::from_static(b"import type { T } from './types.ts';\nexport const rpc = { async get(a: T): Promise<T> { return a; } };"));
        files.insert(
            "types.ts".to_string(),
            Bytes::from_static(b"export interface T { id: string }"),
        );
        files.insert("readme.md".to_string(), Bytes::from_static(b"# x"));
        let c = extract_files(&files, "main.ts").unwrap();
        assert_eq!(c.declarations, ["interface T { id: string }"]);
        assert_eq!(c.methods[0].param.as_deref(), Some("T"));
    }

    #[test]
    fn a_declared_name_does_not_clash_with_the_interface() {
        let src = "export interface Acme { a: 1 }\nexport default defineSdk({ methods: { async f() {} } });";
        let d = extract_module("m.ts", src).unwrap().render_sdk("acme", "1.0.0");
        assert!(d.contains("export interface AcmeContract {"), "{d}");
    }

    #[test]
    fn a_service_contract_declares_its_types_before_the_rpc_interface() {
        let src = "export interface Item { id: string }\nexport const rpc = { async get(a: Item): Promise<Item> { return a; } };";
        let d = extract_module("main.ts", src).unwrap().render_service("items");
        assert!(
            d.contains("export interface Item { id: string }\n\nexport interface ItemsRpc {"),
            "{d}"
        );
        assert!(d.contains("  get(arg: Item): Promise<Item>;"), "{d}");
    }

    #[test]
    fn an_upload_is_the_surface_of_its_entry_module() {
        let src = b"export const rpc = { async ping(): Promise<string> { return 'pong'; } };";
        let c = extract_upload(
            Some("application/typescript"),
            src,
            &crate::builder::UploadLimits::default(),
        )
        .unwrap();
        assert_eq!(c.members(), ["ping"]);
        let err = extract_upload(Some("image/png"), src, &crate::builder::UploadLimits::default()).unwrap_err();
        assert!(
            matches!(&err, ContractError::Invalid(m) if m.contains("unsupported content type")),
            "{err:?}"
        );
    }

    #[test]
    fn a_module_with_an_empty_surface_has_no_contract() {
        assert_eq!(
            extract_module("m.ts", "export const rpc = {};").unwrap_err(),
            ContractError::NoSurface
        );
    }

    #[test]
    fn a_file_of_unknown_type_is_read_as_typescript() {
        let c = extract_module(
            "service",
            "export const rpc = { async a(): Promise<number> { return 1; } };",
        )
        .unwrap();
        assert_eq!(c.methods[0].returns, "number");
    }

    #[test]
    fn a_shorthand_method_is_the_declared_function_of_that_name() {
        let src = "async function a(): Promise<number> { return 1; }\n\
                   async function b(x: string): Promise<string> { return x; }\n\
                   export default defineSdk({ methods: { b } });";
        let c = extract_module("m.ts", src).unwrap();
        assert_eq!(c.methods.len(), 1);
        assert_eq!(
            (c.methods[0].path.as_str(), c.methods[0].param.as_deref()),
            ("b", Some("string"))
        );
        assert_eq!(c.methods[0].returns, "string");
    }

    #[test]
    fn an_imported_shorthand_method_has_an_unknown_signature_and_is_unchecked() {
        let src = "import { ext } from './ext.ts';\nexport default defineSdk({ methods: { ext } });";
        let c = extract_module("m.ts", src).unwrap();
        assert_eq!(c.methods[0].param, None);
        assert_eq!(c.methods[0].returns, "unknown");
        assert_eq!(
            c.unchecked,
            ["ext"],
            "an imported body cannot be seen to read the caller"
        );
    }

    #[test]
    fn identifiers_name_declared_functions_only() {
        let src = "async function ping(): Promise<string> { return 'p'; }\n\
                   import { remote } from './r.ts';\n\
                   export default defineSdk({ methods: { alias: ping, other: remote } });";
        let c = extract_module("m.ts", src).unwrap();
        assert_eq!(c.members(), ["alias"]);
        assert_eq!(c.methods[0].returns, "string");
    }

    #[test]
    fn values_that_are_not_functions_and_accessors_are_not_methods() {
        let src = "export const rpc = { answer: 42, get computed() { return 1; }, ok: async () => 1 };";
        assert_eq!(extract_module("m.ts", src).unwrap().members(), ["ok"]);
    }

    #[test]
    fn computed_and_invalid_names_are_not_methods() {
        let src = "export const rpc = { async [\"m\"]() { return 1; }, [\"dyn\"]: async () => 1, \"bad-name\": async () => 2, good: async () => 3 };";
        assert_eq!(extract_module("m.ts", src).unwrap().members(), ["good"]);
    }

    #[test]
    fn nesting_stops_at_eight_levels() {
        let nest = |levels: usize| {
            let mut s = String::from("async deep() { return 1; }");
            for i in (1..=levels).rev() {
                s = format!("n{i}: {{ {s} }}");
            }
            format!("export const rpc = {{ top: async () => 1, {s} }};")
        };
        let paths = |levels: usize| -> Vec<String> {
            extract_module("m.ts", &nest(levels))
                .unwrap()
                .methods
                .into_iter()
                .map(|m| m.path)
                .collect()
        };
        assert_eq!(paths(8), ["top", "n1.n2.n3.n4.n5.n6.n7.n8.deep"]);
        assert_eq!(
            paths(9),
            ["top"],
            "methods nested deeper than eight levels are not part of the surface"
        );
    }

    #[test]
    fn casts_and_parentheses_around_the_surface_are_seen_through() {
        let rpc = extract_module("m.ts", "export const rpc = ({ async a() { return 1; } } as const);").unwrap();
        assert_eq!(rpc.members(), ["a"]);
        let sdk = "export default defineSdk({ methods: { b: (async () => 2) as unknown, c: (async () => 3) satisfies unknown } });";
        assert_eq!(extract_module("m.ts", sdk).unwrap().members(), ["b", "c"]);
    }

    #[test]
    fn the_authorize_hook_is_found_however_it_is_written() {
        let src = "const authorize = () => true;\n\
                   const base = {};\n\
                   export default defineSdk({ ...base, get extra() { return 1; }, authorize, methods: { a: async () => 1 } });";
        let c = extract_module("m.ts", src).unwrap();
        assert_eq!(c.members(), ["a"]);
        assert!(c.unchecked.is_empty(), "authorize present: nothing is unchecked");
    }

    #[test]
    fn a_declaration_other_than_rpc_is_not_the_surface() {
        let src = "export const helper = 1;\nexport const rpc = { async a() { return 1; } };";
        assert_eq!(extract_module("m.ts", src).unwrap().members(), ["a"]);
    }

    #[test]
    fn only_a_define_sdk_call_or_an_rpc_object_is_a_surface() {
        for src in [
            "export default makeSdk({ methods: { a: async () => 1 } });",
            "export default sdk.defineSdk({ methods: { a: async () => 1 } });",
            "export default defineSdk({ methods: impl });",
        ] {
            assert_eq!(
                extract_module("m.ts", src).unwrap_err(),
                ContractError::NoSurface,
                "{src}"
            );
        }
    }

    #[test]
    fn parameter_patterns_keep_their_annotations() {
        let src = "export const rpc = {\n\
                   async a([x, y]: [number, string]): Promise<void> {},\n\
                   async b(...rest: string[]) {},\n\
                   async c(p: string = 'x') {},\n\
                   sync(): number { return 1; },\n\
                   };";
        let c = extract_module("m.ts", src).unwrap();
        let by = |p: &str| c.methods.iter().find(|m| m.path == p).unwrap();
        assert_eq!(by("a").param.as_deref(), Some("[number, string]"));
        assert_eq!(
            by("b").param.as_deref(),
            Some("unknown"),
            "a rest parameter has no annotation the contract can use"
        );
        assert_eq!((by("c").param.as_deref(), by("c").optional), (Some("string"), true));
        assert_eq!(
            by("sync").returns,
            "number",
            "a return type without Promise is kept as written"
        );
    }
}
