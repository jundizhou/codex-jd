use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadModelIdentityListParams;
use codex_app_server_protocol::ThreadModelIdentityListResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

#[tokio::test]
async fn thread_model_identity_list_returns_loaded_thread_identity() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let started = app
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.2".to_string()),
            ..Default::default()
        })
        .await?;

    let response: ThreadModelIdentityListResponse = app
        .request(|request_id| ClientRequest::ThreadModelIdentityList {
            request_id,
            params: ThreadModelIdentityListParams::default(),
        })
        .await?;

    assert_eq!(response.next_cursor, None);
    assert_eq!(response.data.len(), 1);
    let identity = &response.data[0];
    assert_eq!(identity.thread_id, started.thread.id);
    assert_eq!(identity.session_id, started.thread.session_id);
    assert_eq!(identity.window_id, format!("{}:0", started.thread.id));
    assert!(!identity.installation_id.is_empty());
    assert_eq!(identity.parent_thread_id, None);
    assert_eq!(identity.turn_id, None);
    assert_eq!(identity.root_turn_id, None);
    assert_eq!(identity.parent_turn_id, None);

    let completed = app
        .start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: started.thread.id.clone(),
            input: vec![UserInput::Text {
                text: "Hello".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let response: ThreadModelIdentityListResponse = app
        .request(|request_id| ClientRequest::ThreadModelIdentityList {
            request_id,
            params: ThreadModelIdentityListParams::default(),
        })
        .await?;

    assert_eq!(response.next_cursor, None);
    assert_eq!(response.data.len(), 1);
    let identity = &response.data[0];
    assert_eq!(
        identity.turn_id.as_deref(),
        Some(completed.turn.id.as_str())
    );
    assert_eq!(
        identity.root_turn_id.as_deref(),
        Some(completed.turn.id.as_str())
    );
    assert_eq!(identity.parent_turn_id, None);

    Ok(())
}
