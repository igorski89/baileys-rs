use baileys_rs::{
    binary::{self, BinaryNode},
    privacy_settings,
    receipts::{self, Group, Type},
};
#[path = "support/receipt_helpers.rs"]
mod support;
use support::{fixture, hex, keys, node};
fn kind(s: Option<&str>) -> Option<Type> {
    s.map(|s| match s {
        "read" => Type::Read,
        "read-self" => Type::ReadSelf,
        "hist_sync" => Type::HistorySync,
        "peer_msg" => Type::PeerMessage,
        "sender" => Type::Sender,
        "inactive" => Type::Inactive,
        "played" => Type::Played,
        _ => panic!("unknown type"),
    })
}
#[test]
fn receipt_grouping_routing_lists_and_errors_match_original() {
    let fixture = fixture();
    assert_eq!(
        serde_json::to_value(receipts::aggregate(&keys(&fixture["keys"]))).unwrap(),
        fixture["aggregated"]
    );
    for case in fixture["receiptCases"].as_array().unwrap() {
        let config = &case["config"];
        let group = Group {
            jid: config["jid"].as_str().map(str::to_owned),
            participant: config["participant"].as_str().map(str::to_owned),
            message_ids: config["ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().map(str::to_owned))
                .collect(),
        };
        let node = receipts::receipt(
            &group,
            kind(config["type"].as_str()),
            fixture["timestamp"].as_u64().unwrap(),
        );
        if case["error"] == true {
            assert_eq!(node.unwrap_err().status_code(), 500);
        } else {
            assert_eq!(
                binary::encode(&node.unwrap()).unwrap(),
                hex(case["wire"].as_str().unwrap()),
                "{config}"
            );
        }
    }
    for case in fixture["readCases"].as_array().unwrap() {
        let kind = if case["value"] == "all" {
            Type::Read
        } else {
            Type::ReadSelf
        };
        let groups = receipts::aggregate(&keys(&fixture["keys"])[..5]);
        let wires: Vec<_> = groups
            .iter()
            .map(|g| {
                binary::encode(
                    &receipts::receipt(g, Some(kind), fixture["timestamp"].as_u64().unwrap())
                        .unwrap(),
                )
                .unwrap()
            })
            .collect();
        assert_eq!(
            wires,
            case["wires"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| hex(v.as_str().unwrap()))
                .collect::<Vec<_>>()
        );
    }
}
#[test]
fn ack_attributes_and_call_rejection_match_original() {
    let fixture = fixture();
    for case in fixture["ackCases"].as_array().unwrap() {
        let ack = receipts::acknowledgement(
            &node(&case["node"]),
            case["error"].as_u64().map(|n| n as u32),
            case["me"].as_str(),
        );
        assert_eq!(
            binary::encode(&ack).unwrap(),
            hex(case["wire"].as_str().unwrap()),
            "{case}"
        );
    }
    assert_eq!(
        binary::encode(&receipts::reject_call(
            "100:7@s.whatsapp.net",
            "CALL",
            "800@lid"
        ))
        .unwrap(),
        hex(fixture["rejectCall"].as_str().unwrap())
    );
}
#[test]
fn privacy_dictionary_queries_and_updates_match_original() {
    let fixture = fixture();
    assert_eq!(
        serde_json::to_value(privacy_settings::extract(&node(
            &fixture["privacyResponse"]
        )))
        .unwrap(),
        fixture["settings"]
    );
    for wire in fixture["privacyQueries"].as_array().unwrap() {
        assert_eq!(
            binary::encode(&privacy_settings::query()).unwrap(),
            hex(wire.as_str().unwrap())
        );
    }
    for case in fixture["updates"].as_array().unwrap() {
        let name = match case["method"].as_str().unwrap() {
            "updateMessagesPrivacy" => "messages",
            "updateCallPrivacy" => "calladd",
            "updateLastSeenPrivacy" => "last",
            "updateOnlinePrivacy" => "online",
            "updateProfilePicturePrivacy" => "profile",
            "updateStatusPrivacy" => "status",
            "updateReadReceiptsPrivacy" => "readreceipts",
            "updateGroupsAddPrivacy" => "groupadd",
            _ => unreachable!(),
        };
        assert_eq!(
            binary::encode(&privacy_settings::update(
                name,
                case["value"].as_str().unwrap()
            ))
            .unwrap(),
            hex(case["wire"].as_str().unwrap())
        );
    }
    assert!(privacy_settings::extract(&BinaryNode::new("iq")).is_empty());
}
