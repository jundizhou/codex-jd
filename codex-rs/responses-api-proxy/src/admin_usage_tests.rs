use super::*;
use pretty_assertions::assert_eq;

#[test]
fn weekly_primary_is_not_relabelled_as_five_hours_and_identity_is_not_exposed() -> Result<()> {
    let value = normalize(
        &json!({
            "plan_type":"pro","account_id":"private-account","email":"private@example.com",
            "rate_limit":{"allowed":true,"limit_reached":false,
                "primary_window":{"used_percent":83,"limit_window_seconds":604800,"reset_at":2000},
                "secondary_window":null},
            "credits":{"has_credits":false,"unlimited":false,"balance":null},
            "spend_control":{"reached":false}
        }),
        /*now*/ 1000,
    )?;
    assert_eq!(
        value,
        json!({
            "plan_type":"pro","fetched_at":1000,"limits":[{"name":"Codex","allowed":true,"limit_reached":false,
                "windows":[{"seconds":604800,"used_percent":83.0,"remaining_percent":17.0,"reset_at":2000}]}],
            "credits":{"has_credits":false,"unlimited":false,"balance":null},
            "reset_credits":null,"spend_control_reached":false
        })
    );
    Ok(())
}

#[test]
fn missing_percent_is_unknown_and_extra_limits_and_credit_precision_are_preserved() -> Result<()> {
    let value = normalize(
        &json!({
            "plan_type":"plus","rate_limit":{"primary_window":{"limit_window_seconds":18000,"reset_after_seconds":90}},
            "additional_rate_limits":[{"limit_name":"Spark","rate_limit":{"allowed":false,"limit_reached":true,
                "primary_window":{"used_percent":110,"limit_window_seconds":604800}}}],
            "credits":{"has_credits":true,"unlimited":false,"balance":"12.340000001"},
            "rate_limit_reset_credits":{"available_count":2}
        }),
        /*now*/ 1000,
    )?;
    assert_eq!(
        value,
        json!({
            "plan_type":"plus","fetched_at":1000,"limits":[
                {"name":"Codex","allowed":null,"limit_reached":null,
                 "windows":[{"seconds":18000,"used_percent":null,"remaining_percent":null,"reset_at":1090}]},
                {"name":"Spark","allowed":false,"limit_reached":true,
                 "windows":[{"seconds":604800,"used_percent":110.0,"remaining_percent":0.0,"reset_at":null}]}],
            "credits":{"has_credits":true,"unlimited":false,"balance":"12.340000001"},
            "reset_credits":2,"spend_control_reached":null
        })
    );
    assert!(normalize(&json!({"error":"unexpected response"}), /*now*/ 1000).is_err());
    Ok(())
}

#[test]
fn quota_get_uses_selected_credentials_and_does_not_expose_upstream_error_body() -> Result<()> {
    for status in [200, 401, 429] {
        let server =
            tiny_http::Server::http("127.0.0.1:0").map_err(|error| anyhow::anyhow!("{error}"))?;
        let url = format!("http://{}/usage", server.server_addr());
        let receiver = std::thread::spawn(move || {
            let request = server
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap();
            assert_eq!(
                (request.method(), request.url()),
                (&tiny_http::Method::Get, "/usage")
            );
            let received = ["authorization", "chatgpt-account-id"].map(|name| {
                request
                    .headers()
                    .iter()
                    .find(|header| header.field.equiv(name))
                    .unwrap()
                    .value
                    .to_string()
            });
            assert_eq!(received, ["Bearer selected-token", "selected-account"]);
            request.respond(Response::from_string(r#"{"plan_type":"plus","rate_limit":null,"error":"private-upstream-detail"}"#)
                .with_status_code(StatusCode(status))).unwrap();
        });
        let result = query(
            &json!({"tokens":{"access_token":"selected-token","account_id":"selected-account"}}),
            &url,
        );
        receiver.join().unwrap();
        if status == 200 {
            assert_eq!(result?["limits"], json!([]));
        } else {
            let message = result.unwrap_err().to_string();
            assert!(
                !message.contains("private-upstream-detail") && !message.contains("selected-token")
            );
            assert_eq!(
                message,
                if status == 401 {
                    "账号认证已失效，请重新授权后查询"
                } else {
                    "上游额度查询过于频繁，请稍后重试"
                }
            );
        }
    }
    Ok(())
}
