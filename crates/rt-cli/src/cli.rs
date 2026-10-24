//! Command line definition (clap derive).

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

use crate::template::Template;

const ABOUT: &str = "Command line client for the 21 Cloud Functions Runtime control API";

const AFTER_HELP: &str = "\
Environment:
  RT_API          control API base (default http://127.0.0.1:8081)
  RT_INVOKE       invoke listener base (default http://127.0.0.1:8080)
  RT_KEY          Bearer token: root token or tenant key rtk_... (falls back to RT_ROOT_TOKEN)
  RT_CONTEXT      context to use (same as --context)
  RT_TENANT       act for this tenant with an operator token (same as --as-tenant)
  RT_INVOKE_KEY   key for `invoke` against access=key services
  RT_RETRY        retries of a 429/503 that carries Retry-After (same as --retry)

Exit codes: 0 ok, 1 API error or failed command, 2 usage error, 3 network error,
4 busy (429/503 with Retry-After: try again later).
Docs: docs/cli.md";

#[derive(Parser, Debug)]
#[command(name = "21ci", version, about = ABOUT, after_help = AFTER_HELP, propagate_version = true)]
pub struct Cli {
    /// Context from the config file to use (default: the current context)
    #[arg(long, global = true, env = "RT_CONTEXT", value_name = "NAME")]
    pub context: Option<String>,

    /// Output format
    #[arg(short = 'o', long, global = true, value_enum, default_value_t = Output::Table)]
    pub output: Output,

    /// Same as --output json
    #[arg(long, global = true)]
    pub json: bool,

    /// Retry a request answered 429/503 with a Retry-After (of at most 60s) up to N times
    #[arg(long, global = true, env = "RT_RETRY", value_name = "N", default_value_t = 0)]
    pub retry: u32,

    /// Act for this tenant (operator token): sent as the x-rt-tenant header
    #[arg(long, global = true, env = "RT_TENANT", value_name = "TEN_ID")]
    pub as_tenant: Option<String>,

    /// Config file (default: $XDG_CONFIG_HOME/21ci/config.toml or ~/.config/21ci/config.toml)
    #[arg(long, global = true, value_name = "FILE")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Output {
    /// Human-readable tables
    Table,
    /// The API's JSON
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RuntimeArg {
    Deno,
    Wasm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompileArg {
    /// Build on the first request
    Lazy,
    /// Build at upload time
    Eager,
}

impl CompileArg {
    pub fn as_str(self) -> &'static str {
        match self {
            CompileArg::Lazy => "lazy",
            CompileArg::Eager => "eager",
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Manage contexts (API endpoint + token) in the config file
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Scaffold a new function project from a template
    Init(InitArgs),
    /// Who the credentials are: principal, tenant, key, scopes, plan
    Whoami,
    /// The plan limits of your tenant and what is left of them today
    Limits,
    /// Plans configured on the node (operator)
    Plan {
        #[command(subcommand)]
        cmd: PlanCmd,
    },
    /// Tenants (operator; `usage` also for a tenant key on its own tenant)
    Tenant {
        #[command(subcommand)]
        cmd: TenantCmd,
    },
    /// API keys of a tenant (operator)
    Key {
        #[command(subcommand)]
        cmd: KeyCmd,
    },
    /// Your own API keys: create, list and revoke keys of your tenant (scope keys:write)
    Token {
        #[command(subcommand)]
        cmd: TokenCmd,
    },
    /// Projects: groups of services, each with its own environments
    Project {
        #[command(subcommand)]
        cmd: ProjectCmd,
    },
    /// Bridge the MCP endpoint of the control API to stdio (Claude Desktop, Cursor, any MCP client)
    Mcp(McpArgs),
    /// Services
    Service {
        #[command(subcommand)]
        cmd: ServiceCmd,
    },
    /// Upload a version of a service and make it live
    Push(PushArgs),
    /// List the versions of a service, or show one
    Versions(VersionsArgs),
    /// Make an older version live again
    Rollback(RollbackArgs),
    /// Rebuild a version (clears a failed build)
    Rebuild(RebuildArgs),
    /// Stream the build log of a version until the build ends
    BuildLogs(RebuildArgs),
    /// Download the uploaded source of a version
    Source(SourceArgs),
    /// Download the TypeScript declarations of a service's RPC surface (its `rpc` export)
    Types(TypesArgs),
    /// Operator SDKs: publish, promote, roll back, grant (operator); the SDKs you may use and their types
    Sdk {
        #[command(subcommand)]
        cmd: SdkCmd,
    },
    /// Environment variables of a service
    Env {
        #[command(subcommand)]
        cmd: EnvCmd,
    },
    /// Write-only secrets of a service
    Secret {
        #[command(subcommand)]
        cmd: SecretCmd,
    },
    /// Custom hosts of a service (claims, DNS verification)
    Host {
        #[command(subcommand)]
        cmd: HostCmd,
    },
    /// Service data (SQLite export)
    Data {
        #[command(subcommand)]
        cmd: DataCmd,
    },
    /// Interval and cron schedules of a service
    Schedule {
        #[command(subcommand)]
        cmd: ScheduleCmd,
    },
    /// Queue and dead-letter queue of a service
    Queue {
        #[command(subcommand)]
        cmd: QueueCmd,
    },
    /// Stream topics (persistent, replayable logs): create, inspect, append, read
    Topic {
        #[command(subcommand)]
        cmd: TopicCmd,
    },
    /// Stream consumers of a service: cursors, lag, reset, dead letters
    Consumer {
        #[command(subcommand)]
        cmd: ConsumerCmd,
    },
    /// Pub/sub subjects (ephemeral fan-out): publish a message
    Pubsub {
        #[command(subcommand)]
        cmd: PubsubCmd,
    },
    /// Async invocations of a service (`invoke --async`): list, show, redrive, purge
    Invocation {
        #[command(subcommand)]
        cmd: InvocationCmd,
    },
    /// Open WebSockets of a service: list them, close one or several
    Sockets {
        #[command(subcommand)]
        cmd: SocketsCmd,
    },
    /// Objects of a service (WebSockets and durable storage): list them, drop one's storage
    Objects {
        #[command(subcommand)]
        cmd: ObjectsCmd,
    },
    /// Show or follow the logs of a service
    Logs(LogsArgs),
    /// Request statistics of a service
    Stats(StatsArgs),
    /// Control-plane events of a service (deploys, live changes, ...)
    Events(EventsArgs),
    /// Call a deployed service on the invoke listener
    Invoke(InvokeArgs),
    /// Node status and operator controls
    Node {
        #[command(subcommand)]
        cmd: NodeCmd,
    },
    /// Print (or save) the OpenAPI document of the control API
    Openapi(OpenapiArgs),
    /// Print the URL of the API reference (Scalar)
    Docs,
    /// Check the configuration, connectivity, credentials and versions
    Doctor,
    /// Print the version
    Version(VersionArgs),
    /// Print a shell completion script
    Completion(CompletionArgs),
}

// ---- config ----------------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Create or update a context (the first one becomes current)
    SetContext {
        name: String,
        /// Control API base URL, e.g. http://127.0.0.1:8081
        #[arg(long, value_name = "URL")]
        api: Option<String>,
        /// Invoke listener base URL, e.g. http://127.0.0.1:8080
        #[arg(long, value_name = "URL")]
        invoke: Option<String>,
        /// Root token or tenant key (stored in the config file, mode 0600)
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
        /// Tenant to act for (operator token): sent as x-rt-tenant
        #[arg(long, value_name = "TEN_ID")]
        tenant: Option<String>,
    },
    /// Switch the current context
    UseContext { name: String },
    /// List contexts
    GetContexts,
    /// Print the current context
    Current,
}

// ---- init ------------------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct InitArgs {
    /// Directory to create or fill
    pub dir: PathBuf,
    #[arg(long, value_enum, default_value_t = Template::Deno)]
    pub template: Template,
    /// Project / service name (default: the directory name)
    #[arg(long)]
    pub name: Option<String>,
    /// Write into a directory that is not empty
    #[arg(long)]
    pub force: bool,
}

// ---- shared ----------------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct SvcArg {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
}

#[derive(Args, Debug, Clone, Default)]
pub struct ListArgs {
    /// Page size
    #[arg(long)]
    pub limit: Option<u32>,
    /// Continue after this cursor (printed under a page that has more)
    #[arg(long)]
    pub cursor: Option<String>,
    /// Follow every page
    #[arg(long)]
    pub all: bool,
}

pub fn parse_duration_arg(s: &str) -> Result<Duration, String> {
    crate::fmt::parse_duration(s)
}

// ---- tenants and keys ------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum TenantCmd {
    Create {
        name: String,
        /// Plan from the node config
        #[arg(long, default_value = "free")]
        plan: String,
        /// Plan overrides as a JSON object
        #[arg(long, value_name = "JSON")]
        overrides: Option<String>,
    },
    List(ListArgs),
    Get {
        /// ten_… id
        id: String,
    },
    /// Change the name, plan, overrides or the disabled flag of a tenant
    #[command(group(ArgGroup::new("state").multiple(false).args(["disable", "enable"])))]
    Update {
        /// ten_… id
        id: String,
        #[arg(long)]
        name: Option<String>,
        /// Plan from the node config
        #[arg(long)]
        plan: Option<String>,
        /// Plan overrides as a JSON object (replaces the current overrides)
        #[arg(long, value_name = "JSON")]
        overrides: Option<String>,
        /// Disable the tenant: its services stop serving, its keys stop working
        #[arg(long)]
        disable: bool,
        /// Enable a disabled tenant
        #[arg(long)]
        enable: bool,
    },
    Delete {
        id: String,
        /// Delete the tenant's data too
        #[arg(long)]
        purge: bool,
    },
    /// Daily usage per service: invocations, CPU, egress, storage
    Usage {
        /// ten_… id (default: the tenant of the credentials, from /v1/me)
        id: Option<String>,
        /// First day: YYYY-MM-DD, or a duration back from today (7d)
        #[arg(long, value_name = "DAY")]
        from: Option<String>,
        /// Last day: YYYY-MM-DD, or a duration back from today
        #[arg(long, value_name = "DAY")]
        to: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum PlanCmd {
    /// List the plans with their main quotas
    List,
    /// Show every setting of one plan
    Get { name: String },
}

const DEFAULT_SCOPES: &str = "services:read,services:write,deploy,invoke,logs:read,env:write,secrets:write,queue:write";

#[derive(Subcommand, Debug)]
pub enum KeyCmd {
    /// Create a key; the secret is shown once
    Create {
        #[arg(long, value_name = "TEN_ID")]
        tenant: String,
        /// Comma-separated scopes (services:read, services:write, deploy, invoke, logs:read, env:write, secrets:write, queue:write, data:admin)
        #[arg(long, value_delimiter = ',', default_value = DEFAULT_SCOPES)]
        scopes: Vec<String>,
        #[arg(long)]
        name: Option<String>,
        /// Restrict the key to these projects (prj_… ids, comma-separated; default: all projects)
        #[arg(long = "project", value_delimiter = ',', value_name = "PRJ_ID")]
        projects: Vec<String>,
        /// Restrict the key to these environments of its projects (comma-separated; default: all)
        #[arg(long = "environment", value_delimiter = ',', value_name = "ENV")]
        environments: Vec<String>,
        /// RFC 3339 timestamp
        #[arg(long, value_name = "TIMESTAMP")]
        expires_at: Option<String>,
    },
    List {
        #[arg(long, value_name = "TEN_ID")]
        tenant: String,
    },
    Revoke {
        /// key_… id
        key: String,
        #[arg(long, value_name = "TEN_ID")]
        tenant: String,
    },
}

// ---- self-service tokens, projects, MCP ------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum TokenCmd {
    /// Create a key in your own tenant with at most your scopes, projects, environments and lifetime; the secret
    /// is shown once
    Create {
        /// Comma-separated scopes: a subset of the scopes of the key you use (services:read,
        /// services:write, deploy, invoke, logs:read, env:write, secrets:write, queue:write,
        /// data:admin, keys:write)
        #[arg(long, value_delimiter = ',', required = true, value_name = "SCOPE")]
        scopes: Vec<String>,
        #[arg(long)]
        name: Option<String>,
        /// Restrict the key to these projects (prj_… ids, comma-separated); default: your own
        /// restriction
        #[arg(long = "project", value_delimiter = ',', value_name = "PRJ_ID")]
        projects: Vec<String>,
        /// Restrict the key to these environments of its projects (comma-separated); default: your
        /// own restriction
        #[arg(long = "environment", value_delimiter = ',', value_name = "ENV")]
        environments: Vec<String>,
        /// Expire at this RFC 3339 timestamp (not later than your own key expires)
        #[arg(long, value_name = "TIMESTAMP", conflicts_with = "expires_in")]
        expires_at: Option<String>,
        /// Expire this long from now: 30m, 24h, 7d
        #[arg(long, value_parser = parse_duration_arg, value_name = "DURATION")]
        expires_in: Option<Duration>,
    },
    /// List the keys of your tenant (never their secrets)
    List,
    /// Revoke a key of your tenant: it stops working at once
    Revoke {
        /// key_… id
        key: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum ProjectCmd {
    /// Create a project
    Create {
        /// ^[a-z][a-z0-9-]{1,62}$; cannot be changed later
        slug: String,
        /// Display name (default: the slug)
        #[arg(long)]
        name: Option<String>,
        /// Environments of the project, comma-separated or repeated (default: production)
        #[arg(long = "env", value_delimiter = ',', value_name = "ENV")]
        environments: Vec<String>,
    },
    List(ListArgs),
    /// Show a project: its environments and when it was created
    Get {
        /// Project slug or prj_… id
        project: String,
    },
    /// Rename a project or change its environments
    Update {
        /// Project slug or prj_… id
        project: String,
        #[arg(long)]
        name: Option<String>,
        /// Replace the environments (comma-separated or repeated)
        #[arg(
            long = "env",
            value_delimiter = ',',
            value_name = "ENV",
            conflicts_with_all = ["add_env", "remove_env"]
        )]
        environments: Vec<String>,
        /// Add environments to the current ones
        #[arg(long = "add-env", value_delimiter = ',', value_name = "ENV")]
        add_env: Vec<String>,
        /// Remove environments from the current ones (none of them may hold a service)
        #[arg(long = "remove-env", value_delimiter = ',', value_name = "ENV")]
        remove_env: Vec<String>,
    },
    /// Delete a project that has no services
    Delete {
        /// Project slug or prj_… id
        project: String,
    },
}

/// `21ci mcp` takes no arguments: the endpoint and the key come from the current context.
#[derive(Args, Debug)]
#[command(
    after_help = r#"The bridge reads JSON-RPC messages (one per line) from stdin, posts each to <api>/mcp with your
tenant key as a Bearer token, and prints each answer as one line on stdout. Messages are never
logged and the key never appears on a command line: it comes from RT_KEY or the current context
(`21ci config set-context`). Use a key limited to the scopes the assistant needs (`21ci token create`).

Claude Code:    claude mcp add --transport http 21cloud <api>/mcp --header "Authorization: Bearer $RT_KEY"
Claude Desktop, Cursor, others: run `21ci mcp` as a stdio server (docs/mcp.md)."#
)]
pub struct McpArgs {}

// ---- services --------------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum ServiceCmd {
    Create {
        /// ^[a-z][a-z0-9-]{1,62}$
        slug: String,
        #[arg(long, value_enum, default_value_t = RuntimeArg::Deno)]
        runtime: RuntimeArg,
        /// Display name (default: none)
        #[arg(long)]
        name: Option<String>,
        /// max_density | balanced | min_latency (default: the node's)
        #[arg(long)]
        profile: Option<String>,
        /// public | key | internal
        #[arg(long, value_parser = ["public", "key", "internal"])]
        access: Option<String>,
        /// Project to create the service in: slug or prj_… id (default: default)
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
        /// Environment label, one of the project's environments (default: production)
        #[arg(long = "env", value_name = "ENV")]
        environment: Option<String>,
        /// Who may call the service: a service slug, a svc_… id or env:<name> (repeatable;
        /// none = services in the same environment)
        #[arg(long = "allowed-caller", value_name = "CALLER")]
        allowed_callers: Vec<String>,
        /// Deno: reuse isolates between requests (default: node config, then the profile)
        #[arg(long, value_name = "BOOL")]
        reuse_isolates: Option<bool>,
        /// WASM: reuse instances between requests (default: the profile)
        #[arg(long, value_name = "BOOL")]
        reuse_instances: Option<bool>,
        /// Instances kept warm, never scaled to zero (default 0; at most the plan's max_instances)
        #[arg(long, value_name = "N")]
        min_instances: Option<u32>,
        /// Most instances running at once, the excess requests queue (default: the plan's
        /// max_instances; at least 1)
        #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
        max_instances: Option<u32>,
        /// A durable service: its objects are chosen in code (`durable.get(name)`), each with its
        /// own storage and alarm, one event or request at a time per object
        #[arg(long)]
        durable: bool,
        /// Keep a reused instance alive this long after it answered, for more work (500ms, 30s;
        /// 0 = dispose right after the answer unless work waits). Sets scaling.min_run_time_ms
        #[arg(long, value_parser = parse_duration_arg, value_name = "DURATION")]
        min_run_time: Option<Duration>,
        /// Set a field of a config block (repeatable): limits.cpu_ms=500, scaling.max_queued=100,
        /// triggers.cron=false, ingress.allowed_methods='["GET"]', egress.allowed_hosts='["api.x.com"]',
        /// ws.warm=false. The value is JSON when it parses as JSON, else a string
        #[arg(long = "set", value_name = "BLOCK.KEY=VALUE")]
        set: Vec<String>,
    },
    /// Change a service: name, profile, access, environment, allowed callers, reuse knobs, instances, config blocks
    Update {
        /// Service slug or svc_… id
        svc: String,
        /// Display name
        #[arg(long)]
        name: Option<String>,
        /// max_density | balanced | min_latency
        #[arg(long)]
        profile: Option<String>,
        /// public | key | internal
        #[arg(long, value_parser = ["public", "key", "internal"])]
        access: Option<String>,
        /// New environment label
        #[arg(long = "env", value_name = "ENV")]
        environment: Option<String>,
        /// Replace the allowed callers (repeatable): a slug, a svc_… id or env:<name>
        #[arg(
            long = "allowed-caller",
            value_name = "CALLER",
            conflicts_with = "no_allowed_callers"
        )]
        allowed_callers: Vec<String>,
        /// Clear the allowed callers (back to: same-environment services)
        #[arg(long)]
        no_allowed_callers: bool,
        /// Deno: reuse isolates between requests (true | false)
        #[arg(long, value_name = "BOOL")]
        reuse_isolates: Option<bool>,
        /// WASM: reuse instances between requests (true | false)
        #[arg(long, value_name = "BOOL")]
        reuse_instances: Option<bool>,
        /// Instances kept warm, never scaled to zero (0 = scale to zero)
        #[arg(long, value_name = "N")]
        min_instances: Option<u32>,
        /// Most instances running at once, the excess requests queue (at least 1)
        #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
        max_instances: Option<u32>,
        /// Make the service durable or not (true | false)
        #[arg(long, value_name = "BOOL")]
        durable: Option<bool>,
        /// Keep a reused instance alive this long after it answered (0 = dispose unless work
        /// waits); `--unset scaling.min_run_time_ms` goes back to inherit
        #[arg(long, value_parser = parse_duration_arg, value_name = "DURATION")]
        min_run_time: Option<Duration>,
        /// Set a field of a config block (repeatable), merged into the current block:
        /// limits.cpu_ms=500, scaling.max_queued=100, triggers.http=false, ingress.rate_limit.rps=50,
        /// ws.allowed_origins='["https://app.example"]'
        #[arg(long = "set", value_name = "BLOCK.KEY=VALUE")]
        set: Vec<String>,
        /// Remove a field of a config block (repeatable), back to its default: scaling.max_instances
        #[arg(long = "unset", value_name = "BLOCK.KEY")]
        unset: Vec<String>,
    },
    List {
        #[command(flatten)]
        page: ListArgs,
        /// Only services of this project (slug or prj_… id)
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
        /// Only services in this environment
        #[arg(long = "env", value_name = "ENV")]
        environment: Option<String>,
    },
    Get(SvcArg),
    /// Request statistics plus the queueing / autoscaler state (same as `21ci stats`)
    Stats(StatsArgs),
    Delete {
        /// Service slug or svc_… id
        svc: String,
        /// Keep the service's data (kv, sql, blobs)
        #[arg(long)]
        keep_data: bool,
    },
}

#[derive(Args, Debug)]
pub struct PushArgs {
    /// .ts/.js/.mjs file, .wasm component, .zip/.tar archive, or a directory (rt.json optional)
    pub path: PathBuf,
    /// Service slug (default: derived from the file or directory name)
    #[arg(long, value_name = "SLUG")]
    pub service: Option<String>,
    /// Create the service when it does not exist
    #[arg(long)]
    pub create: bool,
    /// Project of a service created by --create: slug or prj_… id (default: default)
    #[arg(long, value_name = "PROJECT", requires = "create")]
    pub project: Option<String>,
    /// Environment of a service created by --create: one of the project's (default: production)
    #[arg(long = "env", value_name = "ENV", requires = "create")]
    pub environment: Option<String>,
    /// Runtime of a created service (default: from the artifact)
    #[arg(long, value_enum)]
    pub runtime: Option<RuntimeArg>,
    /// Upload without making the version live
    #[arg(long)]
    pub no_activate: bool,
    /// Close the service's WebSockets (code 1012) once the version is live; by default they stay
    /// open and their next event runs the new version
    #[arg(long, conflicts_with = "no_activate")]
    pub close_sockets: bool,
    /// lazy: build on the first request; eager: build now
    #[arg(long, value_enum)]
    pub compile: Option<CompileArg>,
    /// Wait for the build (implies --compile eager)
    #[arg(long)]
    pub wait: bool,
    /// Give up waiting after this long
    #[arg(long, value_parser = parse_duration_arg, default_value = "120s", value_name = "DURATION")]
    pub wait_timeout: Duration,
    /// Poll interval of --wait
    #[arg(long, value_parser = parse_duration_arg, default_value = "1s", hide = true)]
    pub wait_interval: Duration,
}

#[derive(Args, Debug)]
pub struct RollbackArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Version to make live (default: the newest usable version older than the live one)
    #[arg(long, value_name = "N")]
    pub to: Option<u32>,
    /// Close the service's WebSockets (code 1012) once the version is live
    #[arg(long)]
    pub close_sockets: bool,
}

#[derive(Args, Debug)]
pub struct RebuildArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Version number
    #[arg(value_name = "VERSION")]
    pub number: u32,
}

#[derive(Args, Debug)]
pub struct VersionsArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Show this version (build state, digests, failure) instead of the list
    #[arg(value_name = "VERSION")]
    pub number: Option<u32>,
}

#[derive(Args, Debug)]
pub struct SourceArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Version number
    #[arg(value_name = "VERSION")]
    pub number: u32,
    /// Write the source to this file (default: stdout, refused for binary data on a terminal)
    #[arg(long, value_name = "FILE")]
    pub out: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct TypesArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Version number (default: the live version)
    #[arg(value_name = "VERSION")]
    pub number: Option<u32>,
    /// Write `<DIR>/<slug>.d.ts` (default: stdout)
    #[arg(short = 'd', long = "out-dir", visible_alias = "out", value_name = "DIR")]
    pub out: Option<PathBuf>,
}

// ---- operator SDKs ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum SdkCmd {
    /// Publish a release of an SDK from a file or directory (operator)
    Publish(SdkPublishArgs),
    /// Operator: every SDK and its channels. Tenant key: the SDKs your grants allow and the release each runs
    #[command(visible_alias = "list")]
    Ls(SdkLsArgs),
    /// The releases of an SDK (operator)
    Releases { name: String },
    /// Point a channel at a release (operator)
    Promote {
        name: String,
        #[arg(value_name = "VERSION")]
        release: String,
        #[arg(long, default_value = "stable")]
        channel: String,
    },
    /// Move a channel back to the release it pointed to before (operator)
    Rollback {
        name: String,
        #[arg(long, default_value = "stable")]
        channel: String,
    },
    /// Remove an SDK: calls answer sdk_unavailable (operator)
    #[command(visible_alias = "remove")]
    Rm { name: String },
    /// Grants: what a scope (node, plan, tenant, project, environment) may use (operator)
    Grants {
        #[command(subcommand)]
        cmd: SdkGrantsCmd,
    },
    /// Download the TypeScript contract of an SDK (`<name>.d.ts`) for your editor and tsc
    Types(SdkTypesArgs),
}

#[derive(Args, Debug)]
pub struct SdkPublishArgs {
    /// SDK name (`^[a-z][a-z0-9_]{1,31}$`): what tenants import
    pub name: String,
    /// Semver version of the release
    #[arg(value_name = "VERSION")]
    pub release: String,
    /// .ts/.js file, .wasm component, archive or directory: the SDK service (`defineSdk` from std:sdk/server)
    pub path: PathBuf,
    /// Point this channel at it (default: stable for the first release)
    #[arg(long)]
    pub channel: Option<String>,
    /// The .d.ts to publish instead of the generated one (required for a WASM SDK, with --member)
    #[arg(long, value_name = "FILE")]
    pub contract: Option<PathBuf>,
    /// A top-level contract name (repeat); default: the `defineSdk` methods
    #[arg(long = "member", value_name = "NAME")]
    pub members: Vec<String>,
    #[arg(long)]
    pub description: Option<String>,
}

#[derive(Args, Debug)]
pub struct SdkLsArgs {
    /// Tenant key: the project to resolve grants for (default: default)
    #[arg(long)]
    pub project: Option<String>,
    /// Tenant key: the environment (default: production)
    #[arg(long)]
    pub environment: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum SdkGrantsCmd {
    /// List the grants
    Get {
        /// Only grants of this SDK (and `*`)
        #[arg(long)]
        sdk: Option<String>,
    },
    /// Write the grant of SCOPE for SDK (a field not given inherits; nothing given deletes it)
    Set {
        /// node | plan:<name> | tenant:ten_… | project:prj_… | environment:prj_…/<env>
        scope: String,
        /// SDK name or *
        sdk: String,
        #[arg(long, conflicts_with = "deny")]
        allow: bool,
        #[arg(long)]
        deny: bool,
        /// Semver requirement (^1, ~1.4, 1.4.2, *); an allow without one pins the stable major
        #[arg(long, value_name = "REQ")]
        version_req: Option<String>,
        /// Channel to follow (default stable)
        #[arg(long)]
        channel: Option<String>,
    },
}

#[derive(Args, Debug)]
pub struct SdkTypesArgs {
    pub name: String,
    /// Write `<DIR>/<name>.d.ts` (default: stdout)
    #[arg(short = 'd', long = "out-dir", visible_alias = "out", value_name = "DIR")]
    pub out: Option<PathBuf>,
    /// Tenant key: resolve grants for this project (default: default)
    #[arg(long)]
    pub project: Option<String>,
    /// Tenant key: and this environment (default: production)
    #[arg(long)]
    pub environment: Option<String>,
    /// Operator: a release instead of stable
    #[arg(long = "release", value_name = "VERSION")]
    pub release: Option<String>,
}

// ---- hosts, data -----------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum HostCmd {
    /// Claim a custom host for a service; prints the DNS TXT record that verifies the claim
    #[command(alias = "add")]
    Attach {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// Host name, e.g. shop.example.com
        host: String,
    },
    /// List the hosts of a service
    List(SvcArg),
    /// Show one host: status, expiry of a pending claim, the DNS record to create
    #[command(alias = "status")]
    Get {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        host: String,
        /// Poll until the host is active
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this long
        #[arg(long, value_parser = parse_duration_arg, default_value = "15m", value_name = "DURATION")]
        wait_timeout: Duration,
        /// Poll interval of --wait
        #[arg(long, value_parser = parse_duration_arg, default_value = "10s", value_name = "DURATION")]
        wait_interval: Duration,
    },
    /// Release a host (an active mapping or a pending claim)
    #[command(alias = "rm", alias = "remove", alias = "delete")]
    Detach {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        host: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum DataCmd {
    /// Download a consistent SQLite snapshot of the service's database (scope data:admin)
    Export {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// Write the database to this file (default: stdout when it is not a terminal)
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
    },
}

// ---- env, secrets ----------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum EnvCmd {
    /// Show the variables
    Get(SvcArg),
    /// Set variables (merged into the current map)
    Set {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// KEY=VALUE pairs
        #[arg(value_name = "KEY=VALUE", required = true, num_args = 1..)]
        pairs: Vec<String>,
    },
    /// Remove variables
    Unset {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        #[arg(value_name = "KEY", required = true, num_args = 1..)]
        keys: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum SecretCmd {
    /// Write a secret; the value comes from --from-file or stdin
    Set {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        name: String,
        /// Read the value from this file (verbatim). Without it the value is read from stdin
        /// (one trailing newline is dropped)
        #[arg(long, value_name = "FILE")]
        from_file: Option<PathBuf>,
    },
    /// List secret names (values are never shown)
    List(SvcArg),
    #[command(alias = "rm")]
    Delete {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        name: String,
    },
}

// ---- schedules, queue ------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum ScheduleCmd {
    /// Add an interval or cron schedule
    Add(ScheduleAddArgs),
    /// Interval, cron and pending one-off schedules
    List(SvcArg),
    /// Change a schedule: its timing, payload, or enabled flag
    Update(ScheduleUpdateArgs),
    #[command(alias = "remove", alias = "delete")]
    Rm {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// sch_… id
        id: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum SocketsCmd {
    /// Open sockets of one object of a service
    #[command(alias = "list")]
    Ls(SocketsLsArgs),
    /// Close one socket, or every socket of an object with a tag
    Close(SocketsCloseArgs),
}

#[derive(Args, Debug)]
pub struct SocketsLsArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Object name
    #[arg(long, default_value = "default")]
    pub object: String,
    /// Only sockets with this tag
    #[arg(long)]
    pub tag: Option<String>,
}

#[derive(Args, Debug)]
#[command(group(ArgGroup::new("which").required(true).multiple(false).args(["socket", "tag"])))]
pub struct SocketsCloseArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// ws_… id of the socket
    #[arg(value_name = "SOCKET")]
    pub socket: Option<String>,
    /// Object name
    #[arg(long, default_value = "default")]
    pub object: String,
    /// Close every socket of the object with this tag instead
    #[arg(long)]
    pub tag: Option<String>,
    /// Close code: 1000 or 3000-4999
    #[arg(long)]
    pub code: Option<u16>,
    /// Close reason (at most 123 bytes)
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum ObjectsCmd {
    /// Objects with open sockets or durable storage
    #[command(alias = "list")]
    Ls(ObjectsLsArgs),
    /// Drop the storage (keys and alarm) of an object of a durable service
    #[command(alias = "remove", alias = "delete")]
    Rm {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// Object name
        name: String,
    },
}

#[derive(Args, Debug)]
pub struct ObjectsLsArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Only objects whose name starts with this
    #[arg(long)]
    pub prefix: Option<String>,
}

#[derive(Args, Debug)]
#[command(group(ArgGroup::new("when").required(true).multiple(false).args(["every", "cron"])))]
pub struct ScheduleAddArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Fixed interval: 60s, 5m, 1h (a bare number is seconds)
    #[arg(long, value_name = "DURATION")]
    pub every: Option<String>,
    /// Cron expression, e.g. "*/5 * * * *"
    #[arg(long)]
    pub cron: Option<String>,
    /// IANA timezone of the cron expression (default UTC)
    #[arg(long, requires = "cron", value_name = "ZONE")]
    pub tz: Option<String>,
    /// JSON payload handed to the scheduled handler
    #[arg(long, value_name = "JSON")]
    pub payload: Option<String>,
    /// Create the schedule disabled
    #[arg(long)]
    pub disabled: bool,
}

#[derive(Args, Debug)]
#[command(group(ArgGroup::new("when").multiple(false).args(["every", "cron"])))]
#[command(group(ArgGroup::new("state").multiple(false).args(["enable", "disable"])))]
pub struct ScheduleUpdateArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// sch_… id
    pub id: String,
    /// New fixed interval (makes it an interval schedule)
    #[arg(long, value_name = "DURATION")]
    pub every: Option<String>,
    /// New cron expression (makes it a cron schedule)
    #[arg(long)]
    pub cron: Option<String>,
    /// IANA timezone of a cron schedule
    #[arg(long, value_name = "ZONE")]
    pub tz: Option<String>,
    /// New JSON payload
    #[arg(long, value_name = "JSON")]
    pub payload: Option<String>,
    /// Enable the schedule
    #[arg(long)]
    pub enable: bool,
    /// Disable the schedule (it stays, but does not fire)
    #[arg(long)]
    pub disable: bool,
}

#[derive(Subcommand, Debug)]
pub enum QueueCmd {
    /// Enqueue a message
    Send {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// JSON body, `@file`, or `-` for stdin
        #[arg(long, value_name = "JSON")]
        body: String,
        /// Deliver after this delay (60s, 5m; max 7 days)
        #[arg(long, value_parser = parse_duration_arg, value_name = "DURATION")]
        delay: Option<Duration>,
        /// De-duplication id
        #[arg(long, value_name = "ID")]
        dedup: Option<String>,
    },
    /// Depth, in-flight, oldest message age, DLQ size
    Stats(SvcArg),
    /// List the dead-letter queue, or redrive / purge it
    Dlq {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// Move every dead letter back to the queue
        #[arg(long, conflicts_with = "purge")]
        redrive: bool,
        /// Delete every dead letter
        #[arg(long)]
        purge: bool,
    },
}

// ---- topics, consumers, pub/sub, invocations ------------------------------------------------

/// `--project` / `--env`: the namespace a topic name (or a subject) resolves in.
#[derive(Args, Debug, Clone, Default)]
pub struct NsArgs {
    /// Project: slug or prj_… id (default: default)
    #[arg(long, value_name = "PROJECT")]
    pub project: Option<String>,
    /// Environment of the project (default: production)
    #[arg(long = "env", value_name = "ENV")]
    pub environment: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum TopicCmd {
    /// Create a topic
    Create {
        /// [a-z0-9][a-z0-9._-]{0,62}, not starting with tpc_
        name: String,
        #[command(flatten)]
        ns: NsArgs,
        /// Partitions (default 1; at most the plan's)
        #[arg(long, value_name = "N")]
        partitions: Option<u32>,
        /// How long records are kept, in whole hours: 24h, 7d (default 24h)
        #[arg(long, value_parser = parse_duration_arg, value_name = "DURATION")]
        retention: Option<Duration>,
        /// Size cap in MiB, oldest records go first (0 = only the plan's cap)
        #[arg(long, value_name = "M")]
        max_mb: Option<u64>,
    },
    /// List topics
    List {
        #[command(flatten)]
        ns: NsArgs,
    },
    /// Show a topic: partitions, heads, consumers
    Get {
        /// Topic name or tpc_… id
        topic: String,
        #[command(flatten)]
        ns: NsArgs,
    },
    /// Change the retention or the size cap of a topic
    Update {
        /// Topic name or tpc_… id
        topic: String,
        #[command(flatten)]
        ns: NsArgs,
        /// How long records are kept, in whole hours
        #[arg(long, value_parser = parse_duration_arg, value_name = "DURATION")]
        retention: Option<Duration>,
        /// Size cap in MiB (0 = only the plan's cap)
        #[arg(long, value_name = "M")]
        max_mb: Option<u64>,
    },
    /// Delete a topic with its records, cursors and dead letters
    #[command(alias = "rm")]
    Delete {
        /// Topic name or tpc_… id
        topic: String,
        #[command(flatten)]
        ns: NsArgs,
        /// Do not ask (required when stdin is not a terminal)
        #[arg(long)]
        yes: bool,
    },
    /// Append one record
    Append {
        /// Topic name or tpc_… id
        topic: String,
        #[command(flatten)]
        ns: NsArgs,
        /// JSON body, `@file`, or `-` for stdin
        #[arg(long, value_name = "JSON")]
        body: String,
        /// Partition key (records with the same key keep their order)
        #[arg(long, value_name = "K")]
        key: Option<String>,
        /// Record header NAME=VALUE (repeatable)
        #[arg(long = "header", value_name = "NAME=VALUE")]
        headers: Vec<String>,
        /// De-duplication id
        #[arg(long, value_name = "ID")]
        dedup: Option<String>,
    },
    /// Read records of one partition
    Read {
        /// Topic name or tpc_… id
        topic: String,
        #[command(flatten)]
        ns: NsArgs,
        /// Partition (required when the topic has more than one)
        #[arg(long, value_name = "N")]
        partition: Option<u32>,
        /// Start at this offset, or earliest | latest
        #[arg(long, value_name = "OFFSET")]
        from: Option<String>,
        /// Start at the first record at or after this RFC 3339 time
        #[arg(long, value_name = "RFC3339", conflicts_with = "from")]
        from_time: Option<String>,
        /// Most records to return (default 100)
        #[arg(long, value_name = "N")]
        limit: Option<u32>,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConsumerCmd {
    /// The stream consumers of a service: cursor, lag and state per partition
    #[command(alias = "ls")]
    List(SvcArg),
    /// Move a consumer's cursor (also unblocks a blocked partition)
    Reset {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        topic: String,
        group: String,
        /// earliest | latest | an offset | an RFC 3339 time
        #[arg(long, value_name = "WHERE")]
        to: String,
        /// Only this partition (default: all)
        #[arg(long, value_name = "N")]
        partition: Option<u32>,
    },
    /// Dead letters of a consumer
    Dlq {
        #[command(subcommand)]
        cmd: ConsumerDlqCmd,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConsumerDlqCmd {
    /// List the dead letters
    List {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        topic: String,
        group: String,
        #[arg(long, value_name = "N")]
        limit: Option<u32>,
    },
    /// Delete every dead letter
    Purge {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        topic: String,
        group: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum PubsubCmd {
    /// Publish a message to a subject: every subscriber (one per group) gets it, at most once
    Publish {
        /// Subject, e.g. orders.eu.created
        subject: String,
        /// JSON body, `@file`, or `-` for stdin
        #[arg(long, value_name = "JSON")]
        body: String,
        /// Message header NAME=VALUE (repeatable)
        #[arg(long = "header", value_name = "NAME=VALUE")]
        headers: Vec<String>,
        #[command(flatten)]
        ns: NsArgs,
    },
}

#[derive(Subcommand, Debug)]
pub enum InvocationCmd {
    /// Async invocations of a service, newest first, with the pending / failed counts
    #[command(alias = "ls")]
    List {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        #[arg(long, value_parser = ["pending", "succeeded", "failed"])]
        state: Option<String>,
        #[command(flatten)]
        page: ListArgs,
    },
    /// Show one invocation (never its headers or body)
    Get {
        /// Service slug or svc_… id
        #[arg(value_name = "SVC")]
        svc: String,
        /// inv_… id
        id: String,
    },
    /// Move every failed invocation back to pending
    Redrive(SvcArg),
    /// Delete every failed invocation
    Purge(SvcArg),
}

// ---- observability ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Stream new records (Server-Sent Events) until interrupted
    #[arg(short = 'f', long)]
    pub follow: bool,
    /// Only records after this time: RFC 3339 or a duration back from now (15m, 2h, 1d)
    #[arg(long, value_name = "TIME")]
    pub since: Option<String>,
    /// Only records before this time (not with --follow)
    #[arg(long, value_name = "TIME")]
    pub until: Option<String>,
    /// Minimum level
    #[arg(long, value_parser = ["debug", "info", "warn", "error"])]
    pub level: Option<String>,
    /// Only records of this task (task_…)
    #[arg(long, value_name = "TASK_ID")]
    pub task: Option<String>,
    #[arg(long, value_parser = ["runtime", "build", "system"])]
    pub kind: Option<String>,
    /// Page size (not with --follow)
    #[arg(long)]
    pub limit: Option<u32>,
    #[arg(long)]
    pub cursor: Option<String>,
}

#[derive(Args, Debug)]
pub struct StatsArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Time window
    #[arg(long, value_parser = ["5m", "1h", "24h"])]
    pub window: Option<String>,
}

#[derive(Args, Debug)]
pub struct EventsArgs {
    /// Service slug or svc_… id
    #[arg(value_name = "SVC")]
    pub svc: String,
    #[arg(long)]
    pub limit: Option<u32>,
    #[arg(long)]
    pub cursor: Option<String>,
}

// ---- invoke ----------------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct InvokeArgs {
    /// Service slug, svc_… id, or a base URL (http://host/s/slug)
    #[arg(value_name = "SVC")]
    pub svc: String,
    /// Path and query handed to the handler (default /)
    pub path: Option<String>,
    /// HTTP method (default GET, POST with --data)
    #[arg(short = 'X', long = "request", value_name = "METHOD")]
    pub method: Option<String>,
    /// Request body: inline text or @file
    #[arg(short = 'd', long, value_name = "DATA")]
    pub data: Option<String>,
    /// Extra request header, "Name: value" (repeatable)
    #[arg(short = 'H', long = "header", value_name = "NAME:VALUE")]
    pub headers: Vec<String>,
    /// Key (rtk_…) for access=key services; never the control token
    #[arg(long, env = "RT_INVOKE_KEY", value_name = "TOKEN", hide_env_values = true)]
    pub key: Option<String>,
    /// Exit 1 when the status is >= 400
    #[arg(long)]
    pub fail: bool,
    /// Print only the response body
    #[arg(long)]
    pub body_only: bool,
    /// Run asynchronously (sends `Prefer: respond-async`): prints the inv_… id of the accepted
    /// invocation; a refusal prints its code
    #[arg(long = "async", conflicts_with = "body_only")]
    pub async_: bool,
    /// Idempotency-Key of an async invocation: the same key returns the same id
    #[arg(long, value_name = "KEY", requires = "async_")]
    pub idempotency_key: Option<String>,
}

// ---- node and misc ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum NodeCmd {
    /// Capacity, memory, profile, mode
    Status,
    /// Scheduler snapshot: processors, run queues, waiting tasks (raw JSON with -o json)
    Scheduler,
    /// Cache tiers and hit rates (JSON)
    Cache,
    /// Stop taking new work and drain
    Drain,
    /// Live node configuration
    Config {
        #[command(subcommand)]
        cmd: NodeConfigCmd,
    },
}

#[derive(Subcommand, Debug)]
pub enum NodeConfigCmd {
    /// Show the running configuration (credentials masked): every key, or one section / key
    Get {
        /// A section or dotted key, e.g. sched or sched.max_inflight
        #[arg(value_name = "KEY")]
        key: Option<String>,
    },
    /// Apply key=value settings: degradation.mode, default_profile, rate_limit.rps, rate_limit.burst, rate_limit={json}
    Set {
        #[arg(value_name = "KEY=VALUE", required = true, num_args = 1..)]
        pairs: Vec<String>,
    },
}

#[derive(Args, Debug)]
pub struct OpenapiArgs {
    /// Write the document to this file instead of stdout
    #[arg(long, value_name = "FILE")]
    pub out: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct VersionArgs {
    /// Also ask the node for its version (GET /v1/version: any valid token)
    #[arg(long)]
    pub server: bool,
}

#[derive(Args, Debug)]
pub struct CompletionArgs {
    #[arg(value_enum)]
    pub shell: clap_complete::Shell,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    #[test]
    fn the_command_line_definition_is_consistent() {
        super::Cli::command().debug_assert();
    }
}
