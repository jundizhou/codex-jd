//! Local identities read from the host app-server thread manager.

/// Immutable identity used by one slot in the proxy session pool.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct SessionIdentity {
    pub(crate) installation_id: String,
    pub(crate) session_id: String,
    pub(crate) thread_id: String,
    pub(crate) window_id: String,
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) turn_id: Option<String>,
    pub(crate) root_turn_id: Option<String>,
    pub(crate) parent_turn_id: Option<String>,
}
