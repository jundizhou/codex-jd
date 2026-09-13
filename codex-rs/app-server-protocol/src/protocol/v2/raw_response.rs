use crate::JsonSchema;
use crate::RequestId;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

/// Opaque upstream transport data for one streaming `turn/start.rawResponses` RPC.
/// Sent only to the requesting connection. The terminal RPC result/error ends the stream.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct RawResponseStreamNotification {
    pub request_id: RequestId,
    pub event: RawResponseStreamEvent,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(tag = "type", rename_all = "camelCase", export_to = "v2/")]
pub enum RawResponseStreamEvent {
    Started {
        status: u16,
        headers: HashMap<String, String>,
    },
    Chunk {
        /// Exact upstream octets, at most 16 KiB per notification. No UTF-8/SSE decoding.
        data: Vec<u8>,
    },
}
