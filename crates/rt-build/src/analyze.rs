//! `deno_ast` glue: parse, dependency extraction (static imports, re-exports, literal dynamic
//! imports), handler-form detection and TS -> JS transpile with source maps.

use std::sync::Arc;

use deno_ast::swc::ast::{
    CallExpr, Callee, DefaultDecl, ExportAll, Expr, ExprOrSpread, ImportDecl, Lit, Module, ModuleDecl, ModuleItem,
    NamedExport, ObjectLit, Prop, PropName, PropOrSpread, Str,
};
use deno_ast::swc::ecma_visit::{Visit, VisitWith};
use deno_ast::{
    EmitOptions, MediaType, ParseParams, ParsedSource, SourceMapOption, SourceRange, SourceTextInfo,
    TranspileModuleOptions, TranspileOptions,
};
use rt_types::{Diagnostic, HandlerForm};
use url::Url;

/// One import specifier occurrence: its text and the byte range of the string literal.
#[derive(Clone, Debug)]
pub(crate) struct DepRef {
    pub spec: String,
    pub start: usize,
    pub end: usize,
    pub line: u32,
    pub col: u32,
}

pub(crate) struct Parsed {
    pub source: ParsedSource,
    pub deps: Vec<DepRef>,
    pub handler_form: HandlerForm,
}

pub(crate) fn parse_diag(file: &str, d: &deno_ast::ParseDiagnostic) -> Diagnostic {
    use deno_ast::diagnostics::Diagnostic as _;
    let pos = d.display_position();
    Diagnostic {
        file: file.to_string(),
        line: pos.line_number as u32,
        col: pos.column_number as u32,
        message: d.message().to_string(),
    }
}

/// Parse `text` as an ES module and collect its dependencies and handler form.
pub(crate) fn parse(specifier: &Url, file: &str, text: &str, media_type: MediaType) -> Result<Parsed, Diagnostic> {
    let source = deno_ast::parse_module(ParseParams {
        specifier: specifier.clone(),
        text: Arc::from(text),
        media_type,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|d| parse_diag(file, &d))?;
    let text_info = source.text_info_lazy();
    // LCOV_EXCL_START: `parse_module` is `ParseMode::Module`, so its program is never a script
    let deno_ast::ProgramRef::Module(module) = source.program_ref() else {
        unreachable!("parse_module yields a module program")
    };
    // LCOV_EXCL_STOP
    let mut v = DepVisitor {
        text_info,
        deps: Vec::new(),
    };
    module.visit_with(&mut v);
    let deps = v.deps;
    let handler_form = detect_handler_form(module);
    Ok(Parsed {
        source,
        deps,
        handler_form,
    })
}

struct DepVisitor<'a> {
    text_info: &'a SourceTextInfo,
    deps: Vec<DepRef>,
}

impl DepVisitor<'_> {
    fn add(&mut self, s: &Str) {
        let range = SourceRange::unsafely_from_span(s.span);
        let byte = range.as_byte_range(self.text_info.range().start);
        let pos = self.text_info.line_and_column_display(range.start);
        self.deps.push(DepRef {
            spec: s.value.to_string_lossy().into_owned(),
            start: byte.start,
            end: byte.end,
            line: pos.line_number as u32,
            col: pos.column_number as u32,
        });
    }
}

impl Visit for DepVisitor<'_> {
    fn visit_import_decl(&mut self, n: &ImportDecl) {
        if !n.type_only {
            self.add(&n.src);
        }
    }

    fn visit_export_all(&mut self, n: &ExportAll) {
        if !n.type_only {
            self.add(&n.src);
        }
    }

    fn visit_named_export(&mut self, n: &NamedExport) {
        if !n.type_only {
            if let Some(src) = &n.src {
                self.add(src);
            }
        }
    }

    fn visit_call_expr(&mut self, n: &CallExpr) {
        if matches!(n.callee, Callee::Import(_)) {
            if let Some(ExprOrSpread { spread: None, expr }) = n.args.first() {
                if let Expr::Lit(Lit::Str(s)) = &**expr {
                    self.add(s);
                }
            }
        }
        n.visit_children_with(self);
    }
}

/// Static analysis of the default export (function => Form A; object literal with
/// `fetch`/`scheduled`/`queue` => Form B; anything else is detected at runtime).
fn detect_handler_form(m: &Module) -> HandlerForm {
    for item in &m.body {
        match item {
            ModuleItem::ModuleDecl(ModuleDecl::ExportDefaultDecl(d)) => {
                return match &d.decl {
                    DefaultDecl::Fn(_) => HandlerForm::A,
                    _ => HandlerForm::Unknown,
                };
            }
            ModuleItem::ModuleDecl(ModuleDecl::ExportDefaultExpr(e)) => return form_of_expr(&e.expr),
            _ => {}
        }
    }
    HandlerForm::Unknown
}

fn form_of_expr(e: &Expr) -> HandlerForm {
    match e {
        Expr::Arrow(_) | Expr::Fn(_) => HandlerForm::A,
        Expr::Object(o) => {
            if object_has_handler(o) {
                HandlerForm::B
            } else {
                HandlerForm::Unknown
            }
        }
        Expr::Paren(p) => form_of_expr(&p.expr),
        Expr::TsAs(a) => form_of_expr(&a.expr),
        Expr::TsSatisfies(a) => form_of_expr(&a.expr),
        Expr::TsConstAssertion(a) => form_of_expr(&a.expr),
        Expr::TsNonNull(a) => form_of_expr(&a.expr),
        _ => HandlerForm::Unknown,
    }
}

fn object_has_handler(o: &ObjectLit) -> bool {
    let is_handler = |n: &str| matches!(n, "fetch" | "scheduled" | "queue");
    let name_of = |k: &PropName| match k {
        PropName::Ident(i) => Some(i.sym.to_string()),
        PropName::Str(s) => Some(s.value.to_string_lossy().into_owned()),
        _ => None,
    };
    o.props.iter().any(|p| match p {
        PropOrSpread::Prop(p) => match &**p {
            Prop::Shorthand(i) => is_handler(&i.sym),
            Prop::KeyValue(kv) => name_of(&kv.key).is_some_and(|n| is_handler(&n)),
            Prop::Method(m) => name_of(&m.key).is_some_and(|n| is_handler(&n)),
            Prop::Getter(g) => name_of(&g.key).is_some_and(|n| is_handler(&n)),
            Prop::Setter(s) => name_of(&s.key).is_some_and(|n| is_handler(&n)),
            Prop::Assign(_) => false,
        },
        PropOrSpread::Spread(_) => false,
    })
}

/// Emitted JavaScript and its source map.
pub(crate) struct Emitted {
    pub text: String,
    pub source_map: Option<String>,
}

/// TS/TSX/JSX -> JS (no type check) with a separate source map.
pub(crate) fn transpile(source: ParsedSource) -> Result<Emitted, String> {
    let emit = EmitOptions {
        source_map: SourceMapOption::Separate,
        inline_sources: true,
        ..Default::default()
    };
    let out = source
        .transpile(&TranspileOptions::default(), &TranspileModuleOptions::default(), &emit)
        .map_err(|e| e.to_string())?
        .into_source();
    Ok(Emitted {
        text: out.text,
        source_map: out.source_map,
    })
}

/// Replace byte ranges (non-overlapping) of `text` with `replacements`.
pub(crate) fn splice(text: &str, mut edits: Vec<(usize, usize, String)>) -> String {
    edits.sort_by_key(|(s, _, _)| *s);
    let mut out = String::with_capacity(text.len() + 64);
    let mut at = 0usize;
    for (start, end, with) in edits {
        if start < at || end > text.len() || start > end {
            continue;
        }
        out.push_str(&text[at..start]);
        out.push_str(&with);
        at = end;
    }
    out.push_str(&text[at..]);
    out
}

/// A JSON string literal for `s` (used to rewrite specifiers).
pub(crate) fn js_string(s: &str) -> String {
    serde_json::Value::String(s.to_string()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(src: &str) -> Parsed {
        let url = url::Url::parse("file:///main.ts").unwrap();
        parse(&url, "main.ts", src, MediaType::TypeScript).expect("parses")
    }

    fn form(src: &str) -> HandlerForm {
        parsed(src).handler_form
    }

    fn specs(src: &str) -> Vec<String> {
        parsed(src).deps.into_iter().map(|d| d.spec).collect()
    }

    #[test]
    fn a_default_export_function_or_arrow_is_form_a_through_type_wrappers() {
        assert_eq!(
            form("export default function () { return new Response('x'); }"),
            HandlerForm::A
        );
        assert_eq!(form("export default () => new Response('x');"), HandlerForm::A);
        assert_eq!(
            form("export default (() => new Response('x')) as unknown as Handler;"),
            HandlerForm::A
        );
        assert_eq!(
            form("export default (() => new Response('x')) satisfies Handler;"),
            HandlerForm::A
        );
        assert_eq!(
            form("export default (() => new Response('x')) as const;"),
            HandlerForm::A
        );
        assert_eq!(form("export default (() => new Response('x'))!;"), HandlerForm::A);
    }

    #[test]
    fn a_default_class_or_a_plain_value_is_not_detected_statically() {
        assert_eq!(form("export default class Svc {}"), HandlerForm::Unknown);
        assert_eq!(form("export default { answer: 42 };"), HandlerForm::Unknown);
        assert_eq!(form("const x = 1;"), HandlerForm::Unknown);
    }

    #[test]
    fn an_object_default_export_with_a_handler_key_is_form_b() {
        for src in [
            "export default { fetch() { return new Response('x'); } };",
            "export default { get fetch() { return () => 1; } };",
            "export default { set fetch(v) {} };",
            "export default { 'scheduled': () => 1 };",
            "const queue = () => 1; export default { queue };",
            "export default { fetch: () => new Response('x') };",
        ] {
            assert_eq!(form(src), HandlerForm::B, "{src}");
        }
        // Numeric and computed keys, spreads and non-handler names are not handlers.
        assert_eq!(
            form("export default { 1: () => 1, [k]: 2, ...base };"),
            HandlerForm::Unknown
        );
    }

    #[test]
    fn static_and_literal_dynamic_imports_are_dependencies() {
        assert_eq!(
            specs("import a from './a.ts'; export { b } from './b.ts'; export * from './c.ts';"),
            ["./a.ts", "./b.ts", "./c.ts"]
        );
        assert_eq!(specs("const m = await import('./lazy.ts');"), ["./lazy.ts"]);
    }

    #[test]
    fn type_only_imports_and_local_exports_are_not_dependencies() {
        assert!(specs("import type { T } from './t.ts'; export type { U } from './u.ts'; export type * from './v.ts'; export { x };")
            .is_empty());
    }

    #[test]
    fn a_dynamic_import_with_a_computed_specifier_is_not_a_dependency() {
        assert!(specs("const m = import(name); const n = import(`./x.ts`);").is_empty());
    }

    #[test]
    fn a_shorthand_default_is_not_a_handler_key() {
        // `fetch = 1` parses as a pattern default inside the literal; it names no handler.
        assert_eq!(form("export default { fetch = 1 };"), HandlerForm::Unknown);
    }

    #[test]
    fn overlapping_edits_are_skipped_when_splicing() {
        let out = splice(
            "abcdef",
            vec![(0, 3, "X".into()), (1, 2, "Y".into()), (4, 9, "Z".into())],
        );
        assert_eq!(out, "Xdef");
    }
}
