// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// `EnumKind` unconditionally introduces a lifetime. TODO: remove this once
// https://github.com/rust-lang/rust-clippy/pull/9037 makes it into stable
#![allow(clippy::extra_unused_lifetimes)]

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use derivative::Derivative;
use enum_kinds::EnumKind;
use mz_ore::str::StrExt;
use mz_pgcopy::CopyFormatParams;
use mz_repr::statement_logging::StatementEndedExecutionReason;
use mz_repr::{ColumnType, GlobalId, Row, ScalarType};
use mz_sql::ast::{FetchDirection, Raw, Statement};
use mz_sql::catalog::ObjectType;
use mz_sql::plan::{ExecuteTimeout, PlanKind};
use mz_sql::session::vars::Var;
use mz_storage_client::controller::MonotonicAppender;
use tokio::sync::{oneshot, watch};

use crate::client::{ConnectionId, ConnectionIdType};
use crate::coord::peek::PeekResponseUnary;
use crate::coord::ExecuteContextExtra;
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, RowBatchStream, Session};
use crate::util::Transmittable;

#[derive(Debug)]
pub enum Command {
    Startup {
        session: Session,
        cancel_tx: Arc<watch::Sender<Canceled>>,
        tx: oneshot::Sender<Response<StartupResponse>>,
    },

    Declare {
        name: String,
        stmt: Statement<Raw>,
        sql: String,
        param_types: Vec<Option<ScalarType>>,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
    },

    Prepare {
        name: String,
        stmt: Option<Statement<Raw>>,
        sql: String,
        param_types: Vec<Option<ScalarType>>,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
    },

    VerifyPreparedStatement {
        name: String,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
    },

    Execute {
        portal_name: String,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
        outer_ctx_extra: Option<ExecuteContextExtra>,
        span: tracing::Span,
    },

    Commit {
        action: EndTransactionAction,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
    },

    CancelRequest {
        conn_id: ConnectionIdType,
        secret_key: u32,
    },

    PrivilegedCancelRequest {
        conn_id: ConnectionId,
    },

    DumpCatalog {
        session: Session,
        tx: oneshot::Sender<Response<CatalogDump>>,
    },

    CopyRows {
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
        ctx_extra: ExecuteContextExtra,
    },

    AppendWebhook {
        database: String,
        schema: String,
        name: String,
        conn_id: ConnectionId,
        tx: oneshot::Sender<Result<Option<AppendWebhookResponse>, AdapterError>>,
    },

    GetSystemVars {
        session: Session,
        tx: oneshot::Sender<Response<GetVariablesResponse>>,
    },

    SetSystemVars {
        vars: BTreeMap<String, String>,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
    },

    Terminate {
        session: Session,
        tx: Option<oneshot::Sender<Response<()>>>,
    },

    /// Performs any cleanup and logging actions necessary for
    /// finalizing a statement execution.
    ///
    /// Only used for cases that terminate in the protocol layer and
    /// otherwise have no reason to hand control back to the coordinator.
    /// In other cases, we piggy-back on another command.
    RetireExecute {
        data: ExecuteContextExtra,
        reason: StatementEndedExecutionReason,
    },
}

impl Command {
    pub fn session(&self) -> Option<&Session> {
        match self {
            Command::Startup { session, .. }
            | Command::Declare { session, .. }
            | Command::Prepare { session, .. }
            | Command::VerifyPreparedStatement { session, .. }
            | Command::Execute { session, .. }
            | Command::Commit { session, .. }
            | Command::DumpCatalog { session, .. }
            | Command::CopyRows { session, .. }
            | Command::GetSystemVars { session, .. }
            | Command::SetSystemVars { session, .. }
            | Command::Terminate { session, .. } => Some(session),
            Command::CancelRequest { .. }
            | Command::PrivilegedCancelRequest { .. }
            | Command::AppendWebhook { .. }
            | Command::RetireExecute { .. } => None,
        }
    }

    pub fn session_mut(&mut self) -> Option<&mut Session> {
        match self {
            Command::Startup { session, .. }
            | Command::Declare { session, .. }
            | Command::Prepare { session, .. }
            | Command::VerifyPreparedStatement { session, .. }
            | Command::Execute { session, .. }
            | Command::Commit { session, .. }
            | Command::DumpCatalog { session, .. }
            | Command::CopyRows { session, .. }
            | Command::GetSystemVars { session, .. }
            | Command::SetSystemVars { session, .. }
            | Command::Terminate { session, .. } => Some(session),
            Command::CancelRequest { .. }
            | Command::PrivilegedCancelRequest { .. }
            | Command::AppendWebhook { .. }
            | Command::RetireExecute { .. } => None,
        }
    }
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, AdapterError>,
    pub session: Session,
}

pub type RowsFuture = Pin<Box<dyn Future<Output = PeekResponseUnary> + Send>>;

/// The response to [`Client::startup`](crate::Client::startup).
#[derive(Debug)]
pub struct StartupResponse {
    /// Notifications associated with session startup.
    pub messages: Vec<StartupMessage>,
}

// Facile implementation for `StartupResponse`, which does not use the `allowed`
// feature of `ClientTransmitter`.
impl Transmittable for StartupResponse {
    type Allowed = bool;
    fn to_allowed(&self) -> Self::Allowed {
        true
    }
}

/// Messages in a [`StartupResponse`].
#[derive(Debug)]
pub enum StartupMessage {
    /// The database specified in the initial session does not exist.
    UnknownSessionDatabase(String),
}

impl StartupMessage {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        None
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            StartupMessage::UnknownSessionDatabase(_) => Some(
                "Create the database with CREATE DATABASE \
                 or pick an extant database with SET DATABASE = name. \
                 List available databases with SHOW DATABASES."
                    .into(),
            ),
        }
    }
}

impl fmt::Display for StartupMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StartupMessage::UnknownSessionDatabase(name) => {
                write!(f, "session database {} does not exist", name.quoted())
            }
        }
    }
}

/// The response to [`SessionClient::dump_catalog`](crate::SessionClient::dump_catalog).
#[derive(Debug, Clone)]
pub struct CatalogDump(String);

impl CatalogDump {
    pub fn new(raw: String) -> Self {
        CatalogDump(raw)
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl Transmittable for CatalogDump {
    type Allowed = bool;
    fn to_allowed(&self) -> Self::Allowed {
        true
    }
}

/// The response to [`SessionClient::get_system_vars`](crate::SessionClient::get_system_vars).
#[derive(Debug, Clone)]
pub struct GetVariablesResponse(BTreeMap<String, String>);

impl GetVariablesResponse {
    pub fn new<'a>(vars: impl Iterator<Item = &'a dyn Var>) -> Self {
        GetVariablesResponse(
            vars.map(|var| (var.name().to_string(), var.value()))
                .collect(),
        )
    }
}

impl Transmittable for GetVariablesResponse {
    type Allowed = bool;
    fn to_allowed(&self) -> Self::Allowed {
        true
    }
}

impl IntoIterator for GetVariablesResponse {
    type Item = (String, String);
    type IntoIter = std::collections::btree_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub struct AppendWebhookResponse {
    pub tx: MonotonicAppender,
    pub body_ty: ColumnType,
    pub header_ty: Option<ColumnType>,
}

impl fmt::Debug for AppendWebhookResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendWebhookResponse")
            .field("tx", &"(...)")
            .field("body_ty", &self.body_ty)
            .field("header_ty", &self.header_ty)
            .finish()
    }
}

/// The response to [`SessionClient::execute`](crate::SessionClient::execute).
#[derive(EnumKind, Derivative)]
#[derivative(Debug)]
#[enum_kind(ExecuteResponseKind)]
pub enum ExecuteResponse {
    /// The default privileges were altered.
    AlteredDefaultPrivileges,
    /// The requested object was altered.
    AlteredObject(ObjectType),
    /// The index was altered.
    AlteredIndexLogicalCompaction,
    /// The role was altered.
    AlteredRole,
    /// The system configuration was altered.
    AlteredSystemConfiguration,
    /// The query was canceled.
    Canceled,
    /// The requested cursor was closed.
    ClosedCursor,
    CopyTo {
        format: mz_sql::plan::CopyFormat,
        resp: Box<ExecuteResponse>,
    },
    CopyFrom {
        id: GlobalId,
        columns: Vec<usize>,
        params: CopyFormatParams<'static>,
        ctx_extra: ExecuteContextExtra,
    },
    /// The requested connection was created.
    CreatedConnection,
    /// The requested database was created.
    CreatedDatabase,
    /// The requested schema was created.
    CreatedSchema,
    /// The requested role was created.
    CreatedRole,
    /// The requested cluster was created.
    CreatedCluster,
    /// The requested cluster replica was created.
    CreatedClusterReplica,
    /// The requested index was created.
    CreatedIndex,
    /// The requested secret was created.
    CreatedSecret,
    /// The requested sink was created.
    CreatedSink,
    /// The requested HTTP source was created.
    CreatedWebhookSource,
    /// The requested source was created.
    CreatedSource,
    /// The requested sources were created.
    CreatedSources,
    /// The requested table was created.
    CreatedTable,
    /// The requested view was created.
    CreatedView,
    /// The requested views were created.
    CreatedViews,
    /// The requested materialized view was created.
    CreatedMaterializedView,
    /// The requested type was created.
    CreatedType,
    /// The requested prepared statement was removed.
    Deallocate { all: bool },
    /// The requested cursor was declared.
    DeclaredCursor,
    /// The specified number of rows were deleted from the requested table.
    Deleted(usize),
    /// The temporary objects associated with the session have been discarded.
    DiscardedTemp,
    /// All state associated with the session has been discarded.
    DiscardedAll,
    /// The requested object was dropped.
    DroppedObject(ObjectType),
    /// The requested objects were dropped.
    DroppedOwned,
    /// The provided query was empty.
    EmptyQuery,
    /// Fetch results from a cursor.
    Fetch {
        /// The name of the cursor from which to fetch results.
        name: String,
        /// The number of results to fetch.
        count: Option<FetchDirection>,
        /// How long to wait for results to arrive.
        timeout: ExecuteTimeout,
        ctx_extra: ExecuteContextExtra,
    },
    /// The requested privilege was granted.
    GrantedPrivilege,
    /// The requested role was granted.
    GrantedRole,
    /// The specified number of rows were inserted into the requested table.
    Inserted(usize),
    /// The specified prepared statement was created.
    Prepare,
    /// A user-requested warning was raised.
    Raised,
    /// The requested objects were reassigned.
    ReassignOwned,
    /// The requested privilege was revoked.
    RevokedPrivilege,
    /// The requested role was revoked.
    RevokedRole,
    /// Rows will be delivered via the specified future.
    SendingRows {
        #[derivative(Debug = "ignore")]
        future: RowsFuture,
        #[derivative(Debug = "ignore")]
        span: tracing::Span,
    },
    /// Like `SendingRows`, but the rows are known to be available
    /// immediately, and thus the execution is considered ended in the coordinator.
    SendingRowsImmediate {
        rows: Vec<Row>,
        #[derivative(Debug = "ignore")]
        span: tracing::Span,
    },
    /// The specified variable was set to a new value.
    SetVariable {
        name: String,
        /// Whether the operation was a `RESET` rather than a set.
        reset: bool,
    },
    /// A new transaction was started.
    StartedTransaction,
    /// Updates to the requested source or view will be streamed to the
    /// contained receiver.
    Subscribing {
        rx: RowBatchStream,
        ctx_extra: ExecuteContextExtra,
    },
    /// The active transaction committed.
    TransactionCommitted {
        /// Session parameters that changed because the transaction ended.
        params: BTreeMap<&'static str, String>,
    },
    /// The active transaction rolled back.
    TransactionRolledBack {
        /// Session parameters that changed because the transaction ended.
        params: BTreeMap<&'static str, String>,
    },
    /// The specified number of rows were updated in the requested table.
    Updated(usize),
    /// A connection was validated.
    ValidatedConnection,
}

impl ExecuteResponse {
    pub fn tag(&self) -> Option<String> {
        use ExecuteResponse::*;
        match self {
            AlteredDefaultPrivileges => Some("ALTER DEFAULT PRIVILEGES".into()),
            AlteredObject(o) => Some(format!("ALTER {}", o)),
            AlteredIndexLogicalCompaction => Some("ALTER INDEX".into()),
            AlteredRole => Some("ALTER ROLE".into()),
            AlteredSystemConfiguration => Some("ALTER SYSTEM".into()),
            Canceled => None,
            ClosedCursor => Some("CLOSE CURSOR".into()),
            CopyTo { .. } => None,
            CopyFrom { .. } => None,
            CreatedConnection { .. } => Some("CREATE CONNECTION".into()),
            CreatedDatabase { .. } => Some("CREATE DATABASE".into()),
            CreatedSchema { .. } => Some("CREATE SCHEMA".into()),
            CreatedRole => Some("CREATE ROLE".into()),
            CreatedCluster { .. } => Some("CREATE CLUSTER".into()),
            CreatedClusterReplica { .. } => Some("CREATE CLUSTER REPLICA".into()),
            CreatedIndex { .. } => Some("CREATE INDEX".into()),
            CreatedSecret { .. } => Some("CREATE SECRET".into()),
            CreatedSink { .. } => Some("CREATE SINK".into()),
            CreatedWebhookSource { .. } => Some("CREATE SOURCE".into()),
            CreatedSource { .. } => Some("CREATE SOURCE".into()),
            CreatedSources => Some("CREATE SOURCES".into()),
            CreatedTable { .. } => Some("CREATE TABLE".into()),
            CreatedView { .. } => Some("CREATE VIEW".into()),
            CreatedViews { .. } => Some("CREATE VIEWS".into()),
            CreatedMaterializedView { .. } => Some("CREATE MATERIALIZED VIEW".into()),
            CreatedType => Some("CREATE TYPE".into()),
            Deallocate { all } => Some(format!("DEALLOCATE{}", if *all { " ALL" } else { "" })),
            DeclaredCursor => Some("DECLARE CURSOR".into()),
            Deleted(n) => Some(format!("DELETE {}", n)),
            DiscardedTemp => Some("DISCARD TEMP".into()),
            DiscardedAll => Some("DISCARD ALL".into()),
            DroppedObject(o) => Some(format!("DROP {o}")),
            DroppedOwned => Some("DROP OWNED".into()),
            EmptyQuery => None,
            Fetch { .. } => None,
            GrantedPrivilege => Some("GRANT".into()),
            GrantedRole => Some("GRANT ROLE".into()),
            Inserted(n) => {
                // "On successful completion, an INSERT command returns a
                // command tag of the form `INSERT <oid> <count>`."
                //     -- https://www.postgresql.org/docs/11/sql-insert.html
                //
                // OIDs are a PostgreSQL-specific historical quirk, but we
                // can return a 0 OID to indicate that the table does not
                // have OIDs.
                Some(format!("INSERT 0 {}", n))
            }
            Prepare => Some("PREPARE".into()),
            Raised => Some("RAISE".into()),
            ReassignOwned => Some("REASSIGN OWNED".into()),
            RevokedPrivilege => Some("REVOKE".into()),
            RevokedRole => Some("REVOKE ROLE".into()),
            SendingRows { .. } | SendingRowsImmediate { .. } => None,
            SetVariable { reset: true, .. } => Some("RESET".into()),
            SetVariable { reset: false, .. } => Some("SET".into()),
            StartedTransaction { .. } => Some("BEGIN".into()),
            Subscribing { .. } => None,
            TransactionCommitted { .. } => Some("COMMIT".into()),
            TransactionRolledBack { .. } => Some("ROLLBACK".into()),
            Updated(n) => Some(format!("UPDATE {}", n)),
            ValidatedConnection => Some("VALIDATE CONNECTION".into()),
        }
    }

    /// Expresses which [`PlanKind`] generate which set of
    /// [`ExecuteResponseKind`].
    pub fn generated_from(plan: PlanKind) -> Vec<ExecuteResponseKind> {
        use ExecuteResponseKind::*;
        use PlanKind::*;

        match plan {
            AbortTransaction => vec![TransactionRolledBack],
            AlterClusterRename
            | AlterCluster
            | AlterClusterReplicaRename
            | AlterOwner
            | AlterItemRename
            | AlterNoop
            | AlterSecret
            | AlterSink
            | AlterSource
            | RotateKeys => {
                vec![AlteredObject]
            }
            AlterDefaultPrivileges => vec![AlteredDefaultPrivileges],
            AlterIndexSetOptions | AlterIndexResetOptions => {
                vec![AlteredObject, AlteredIndexLogicalCompaction]
            }
            AlterRole => vec![AlteredRole],
            AlterSystemSet | AlterSystemReset | AlterSystemResetAll => {
                vec![AlteredSystemConfiguration]
            }
            Close => vec![ClosedCursor],
            PlanKind::CopyFrom => vec![ExecuteResponseKind::CopyFrom],
            CommitTransaction => vec![TransactionCommitted, TransactionRolledBack],
            CreateConnection => vec![CreatedConnection],
            CreateDatabase => vec![CreatedDatabase],
            CreateSchema => vec![CreatedSchema],
            CreateRole => vec![CreatedRole],
            CreateCluster => vec![CreatedCluster],
            CreateClusterReplica => vec![CreatedClusterReplica],
            CreateWebhookSource => vec![CreatedWebhookSource],
            CreateSource | CreateSources => vec![CreatedSource, CreatedSources],
            CreateSecret => vec![CreatedSecret],
            CreateSink => vec![CreatedSink],
            CreateTable => vec![CreatedTable],
            CreateView => vec![CreatedView],
            CreateMaterializedView => vec![CreatedMaterializedView],
            CreateIndex => vec![CreatedIndex],
            CreateType => vec![CreatedType],
            PlanKind::Deallocate => vec![ExecuteResponseKind::Deallocate],
            Declare => vec![DeclaredCursor],
            DiscardTemp => vec![DiscardedTemp],
            DiscardAll => vec![DiscardedAll],
            DropObjects => vec![DroppedObject],
            DropOwned => vec![DroppedOwned],
            PlanKind::EmptyQuery => vec![ExecuteResponseKind::EmptyQuery],
            Explain | Select | ShowAllVariables | ShowCreate | ShowVariable | InspectShard => {
                vec![CopyTo, SendingRows, SendingRowsImmediate]
            }
            Execute | ReadThenWrite => vec![
                Deleted,
                Inserted,
                SendingRows,
                SendingRowsImmediate,
                Updated,
            ],
            PlanKind::Fetch => vec![ExecuteResponseKind::Fetch],
            GrantPrivileges => vec![GrantedPrivilege],
            GrantRole => vec![GrantedRole],
            CopyRows => vec![Inserted],
            Insert => vec![Inserted, SendingRows, SendingRowsImmediate],
            PlanKind::Prepare => vec![ExecuteResponseKind::Prepare],
            PlanKind::Raise => vec![ExecuteResponseKind::Raised],
            PlanKind::ReassignOwned => vec![ExecuteResponseKind::ReassignOwned],
            RevokePrivileges => vec![RevokedPrivilege],
            RevokeRole => vec![RevokedRole],
            PlanKind::SetVariable | ResetVariable | PlanKind::SetTransaction => {
                vec![ExecuteResponseKind::SetVariable]
            }
            PlanKind::Subscribe => vec![Subscribing, CopyTo],
            StartTransaction => vec![StartedTransaction],
            SideEffectingFunc => vec![SendingRows],
            ValidateConnection => vec![ExecuteResponseKind::ValidatedConnection],
        }
    }
}

/// This implementation is meant to ensure that we maintain updated information
/// about which types of `ExecuteResponse`s are permitted to be sent, which will
/// be a function of which plan we're executing.
impl Transmittable for ExecuteResponse {
    type Allowed = ExecuteResponseKind;
    fn to_allowed(&self) -> Self::Allowed {
        ExecuteResponseKind::from(self)
    }
}

/// The state of a cancellation request.
#[derive(Debug, Clone, Copy)]
pub enum Canceled {
    /// A cancellation request has occurred.
    Canceled,
    /// No cancellation request has yet occurred, or a previous request has been
    /// cleared.
    NotCanceled,
}
