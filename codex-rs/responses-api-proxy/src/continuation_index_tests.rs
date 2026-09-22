use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

fn recorder(path: &std::path::Path, conversation: &str) -> Recorder {
    Recorder {
        index: Arc::new(Mutex::new(Index::open(path.to_path_buf()).unwrap())),
        account: "account-a".into(),
        key: Key("caller-a".into(), conversation.into()),
    }
}

#[test]
fn observed_references_survive_restart_without_storing_ciphertext() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("index.json");
    let recorder = recorder(&path, "original");
    recorder.observe(&json!({"type":"response.created", "response":{"id":"resp-a"}}));
    let item = json!({"type":"function_call", "id":"item-a", "call_id":"call-a", "encrypted_content":"secret-reasoning", "encrypted_function_args":"secret-arguments"});
    recorder.observe(&json!({"type":"response.output_item.done", "item":item}));
    recorder.flush().unwrap();
    let index = Index::open(path.clone()).unwrap();
    for body in [
        json!({"previous_response_id":"resp-a"}),
        json!({"input":[{"type":"item_reference", "id":"item-a"}]}),
        json!({"input":[{"type":"function_call_output", "call_id":"call-a"}]}),
        json!({"input":[{"type":"reasoning", "encrypted_content":"secret-reasoning"}]}),
        json!({"input":[{"type":"function_call", "encrypted_function_args":"secret-arguments"}]}),
        json!({"previous_response_id":"resp-a", "input":[item]}),
    ] {
        assert_eq!(
            index.resolve("caller-a", "account-a", &body),
            Ok(Some("original".into()))
        );
        assert_eq!(
            index.resolve("caller-b", "account-a", &body),
            Err(Rejection::BindingLost)
        );
        assert_eq!(
            index.resolve("caller-a", "account-b", &body),
            Err(Rejection::BindingLost)
        );
    }
    let disk = std::fs::read_to_string(path).unwrap();
    assert!(!disk.contains("secret-") && !disk.contains("call-a") && !disk.contains("resp-a"));
}

#[test]
fn mixed_and_ambiguous_references_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let first = recorder(&directory.path().join("index.json"), "first");
    let second = Recorder {
        key: Key("caller-a".into(), "second".into()),
        ..first.clone()
    };
    first.observe(&json!({"id":"resp-a"}));
    second.observe(&json!({"id":"resp-b"}));
    first.observe(&json!({"output":[{"id":"item-a"}]}));
    assert_eq!(
        first.index.lock().unwrap().resolve(
            "caller-a",
            "account-a",
            &json!({"previous_response_id":"resp-b", "input":[{"id":"item-a"}]})
        ),
        Err(Rejection::ContinuationConflict)
    );
    second.observe(&json!({"id":"resp-a"}));
    let mut index = first.index.lock().unwrap();
    index
        .retain_bindings(&HashSet::from([second.key.digest()]))
        .unwrap();
    assert_eq!(
        index.resolve(
            "caller-a",
            "account-a",
            &json!({"previous_response_id":"resp-a"})
        ),
        Err(Rejection::ContinuationConflict)
    );
}

#[test]
fn request_data_cannot_create_ownership_and_expired_or_deleted_bindings_are_lost() {
    let directory = tempfile::tempdir().unwrap();
    let recorder = recorder(&directory.path().join("index.json"), "original");
    let plain = json!({"input":[{"role":"user", "content":"hello", "metadata":{"id":"fake"}}]});
    let previous = json!({"previous_response_id":"resp-a"});
    assert_eq!(
        recorder
            .index
            .lock()
            .unwrap()
            .resolve("caller-a", "account-a", &plain),
        Ok(None)
    );
    assert_eq!(
        recorder
            .index
            .lock()
            .unwrap()
            .resolve("caller-a", "account-a", &previous),
        Err(Rejection::BindingLost)
    );
    recorder.observe(&json!({"id":"resp-a"}));
    let mut index = recorder.index.lock().unwrap();
    for record in index.state.records.values_mut() {
        record.observed_at = now() - RETENTION;
    }
    assert_eq!(
        index.resolve("caller-a", "account-a", &previous),
        Err(Rejection::BindingLost)
    );
    index.retain_bindings(&HashSet::new()).unwrap();
    assert!(index.state.records.is_empty());
}

#[test]
fn input_and_disk_limits_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("index.json");
    let index = Index::open(path.clone()).unwrap();
    let input: Vec<_> = (0..=MAX_REFERENCES)
        .map(|id| json!({"id":id.to_string()}))
        .collect();
    assert_eq!(
        index.resolve("caller-a", "account-a", &json!({"input":input})),
        Err(Rejection::TooLarge)
    );
    std::fs::write(&path, b"{truncated").unwrap();
    assert!(Index::open(path.clone()).is_err());
    let file = File::create(&path).unwrap();
    file.set_len(MAX_BYTES + 1).unwrap();
    assert!(Index::open(path).is_err());
}
