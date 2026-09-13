use crate::error_code::internal_error;
use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingMessageSender;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RawResponseStreamEvent;
use codex_app_server_protocol::RawResponseStreamNotification;
use codex_app_server_protocol::ServerNotification;

pub(super) async fn send(
    outgoing: &OutgoingMessageSender,
    request: &ConnectionRequestId,
    event: RawResponseStreamEvent,
) -> Result<(), JSONRPCErrorError> {
    let written = outgoing
        .send_server_notification_to_connection_and_wait(
            request.connection_id,
            ServerNotification::RawResponseStream(RawResponseStreamNotification {
                request_id: request.request_id.clone(),
                event,
            }),
        )
        .await;
    if written {
        Ok(())
    } else {
        Err(internal_error("raw Responses client disconnected"))
    }
}
