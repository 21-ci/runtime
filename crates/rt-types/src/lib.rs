//! rt-types (L0): IDs, `TaskSpec`, `Outcome`, `Limits`, `PreparedKey`, reason codes, the
//! shared domain model and the `ControlPlane` trait.
//!
//! Everything here is plain data (serde) or a small trait; no I/O, no runtime dependencies
//! beyond `tokio::sync` for body channels. Every other crate depends on this one.

pub mod body;
pub mod control;
pub mod digest;
pub mod ids;
pub mod kinds;
pub mod limits;
pub mod model;
pub mod outcome;
pub mod plan;
pub mod project;
pub mod sched;
pub mod sdk;
pub mod sockets;
pub mod task;
pub mod upload;

pub use body::{body_channel, Body, BodyError, BodySender};
pub use control::{
    BuildReport, ConfigDelta, ConfigEvent, ConfigSnapshot, ConfigStream, ControlPlane, CpError, CONTROL_PLANE_VERSION,
};
pub use digest::{Digest, DigestError};
pub use ids::{
    EventId, IdError, KeyId, MachineId, MsgId, ProcId, ProjectId, ScheduleId, SecretsVersion, ServiceId, TaskId,
    TenantId, UploadId, VersionNo,
};
pub use kinds::{DegradationMode, Listener, Profile, RuntimeKind, TaskKind};
pub use limits::{CompileMode, Limits, ScalingKnobs, ServiceKnobs, MAX_CALL_DEPTH};
pub use model::{
    default_environment, scope_admits, valid_environment, valid_service_slug, Access, ApiKey, BuildState, CallAccess,
    CallTarget, Caller, DenoServiceConfig, EgressRules, EnvBundle, Etag, HostMapping, HostStatus, IngressRules,
    KeySecret, LimitsConfig, NewKey, NewSchedule, NewService, NewTenant, NewVersion, NodeEvent, RateLimit, RouteEntry,
    RouteSnapshot, ScalingConfig, Schedule, ScheduleKind, Scope, Scopes, Service, ServiceConfig, ServicePatch, Tenant,
    TriggerSwitches, UnixMs, UsageBatch, UsageDelta, Version, WasmServiceConfig, WsServiceConfig, DEFAULT_ENVIRONMENT,
    MAX_ALLOWED_CALLERS, MAX_ENVIRONMENT_LEN,
};
pub use outcome::{BuildFailReason, BuildFailure, Diagnostic, KillReason, Outcome, RejectReason, Reply, TaskTimings};
pub use plan::{EgressDefault, Plan, PlanLimitsMax, TriggerKind};
pub use project::{
    check_project_environments, default_project, default_project_environments, default_project_id, valid_project_slug,
    NewProject, Project, ProjectPatch, DEFAULT_PROJECT, MAX_PROJECTS_PER_TENANT, MAX_PROJECT_ENVIRONMENTS,
    MAX_PROJECT_NAME_LEN,
};
pub use sched::{GrqLens, MachineSnapshot, ProcSnapshot, ProcStatus, SchedSnapshot};
pub use sdk::{
    is_system_tenant, sdk_service_slug, system_tenant, valid_channel, valid_sdk_name, EffectiveGrant, GrantEffect,
    GrantScope, NewSdkRelease, Sdk, SdkCaller, SdkChannel, SdkDenial, SdkGrant, SdkRelease, SdkSnapshot, SdkState,
    SdkTarget, DEFAULT_CHANNEL, SYSTEM_PLAN, SYSTEM_SLUG_PREFIX, SYSTEM_TENANT_NAME,
};
pub use sockets::{
    valid_app_close_code, valid_object_name, valid_socket_id, valid_tag, ObjectRef, SocketEvent, SocketEventKind,
    SocketInfo, WsData, DEFAULT_OBJECT, MAX_ATTACHMENT_BYTES, MAX_CLOSE_REASON_BYTES, MAX_OBJECT_NAME_BYTES,
    MAX_SOCKET_TAGS, MAX_TAG_BYTES, TICKET_HEADER,
};
pub use task::{
    CallerStamp, DelayedEvent, HttpRequest, Payload, PreparedKey, QueueMessage, ScheduledEvent, SdkStamp, ServiceRef,
    TaskSpec, TraceCtx, Trigger,
};
pub use upload::{ContractTarget, HandlerForm, RpcContractText, UploadValidator, ValidatedUpload, ValidationError};
