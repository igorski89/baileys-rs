use baileys_rs::{
    app_state::*,
    auth::{AccountSettings, Contact},
    proto,
};
use prost::Message as _;
use serde_json::{Value, json};
#[path = "support/app_state_actions.rs"]
mod support;
use support::{normalized, unhex};
#[path = "support/json_numbers.rs"]
mod json_numbers;
#[test]
fn all_actions_precedence_defaults_and_conditions_match_original() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/app_state_actions.json")).unwrap();
    for (i, case) in cases.as_array().unwrap().iter().enumerate() {
        let action =
            proto::SyncActionValue::decode(unhex(case["action"].as_str().unwrap()).as_slice())
                .unwrap();
        let mutation = ChatMutation {
            sync_action: proto::SyncActionData {
                value: Some(action),
                ..Default::default()
            },
            index: case["index"].clone(),
        };
        let me: Contact = serde_json::from_value(case["me"].clone()).unwrap();
        let initial = case["mode"] != "normal";
        let mut settings = AccountSettings {
            unarchive_chats: case["mode"] == "initialUnarchive",
            ..Default::default()
        };
        let events = process_action(
            &mutation,
            &mut ActionContext {
                me: Some(&me),
                initial_sync: initial,
                account_settings: initial.then_some(&mut settings),
            },
        )
        .unwrap();
        assert_eq!(
            json_numbers::canonical(&json!(
                events
                    .iter()
                    .map(|e| normalized(e, &me))
                    .collect::<Vec<_>>()
            )),
            json_numbers::canonical(&case["events"]),
            "case {i} {:?}",
            mutation.index
        );
        assert_eq!(
            settings.unarchive_chats,
            case["unarchive"].as_bool().unwrap(),
            "case {i}"
        );
    }
}
#[test]
fn contact_empty_ids_native_index_boundaries_and_initial_setting_sequences() {
    let mutation = |index: Value, action: proto::SyncActionValue| ChatMutation {
        index,
        sync_action: proto::SyncActionData {
            value: Some(action),
            ..Default::default()
        },
    };
    let contact = proto::SyncActionValue {
        contact_action: Some(Default::default()),
        ..Default::default()
    };
    let mut context = ActionContext {
        me: None,
        initial_sync: false,
        account_settings: None,
    };
    assert!(
        process_action(&mutation(json!([]), contact.clone()), &mut context)
            .unwrap()
            .is_empty()
    );
    assert!(
        process_action(
            &mutation(json!(["contact", ""]), contact.clone()),
            &mut context
        )
        .unwrap()
        .is_empty()
    );
    assert!(matches!(
        process_action(&mutation(json!(null), contact.clone()), &mut context),
        Err(Error::Input(_))
    ));
    assert!(matches!(
        process_action(&mutation(json!(["contact", 1]), contact), &mut context),
        Err(Error::Input(_))
    ));
    let mut settings = AccountSettings::default();
    let mut context = ActionContext {
        me: None,
        initial_sync: true,
        account_settings: Some(&mut settings),
    };
    let unarchive = proto::SyncActionValue {
        unarchive_chats_setting: Some(proto::sync_action_value::UnarchiveChatsSetting {
            unarchive_chats: Some(true),
        }),
        ..Default::default()
    };
    process_action(&mutation(json!(["setting"]), unarchive), &mut context).unwrap();
    let archive = proto::SyncActionValue {
        archive_chat_action: Some(proto::sync_action_value::ArchiveChatAction {
            archived: Some(true),
            message_range: Some(proto::sync_action_value::SyncActionMessageRange {
                last_message_timestamp: Some(10),
                ..Default::default()
            }),
        }),
        ..Default::default()
    };
    let event =
        process_action(&mutation(json!(["archive", "chat"]), archive), &mut context).unwrap();
    let ActionEvent::Chat(chat) = &event[0] else {
        panic!()
    };
    assert_eq!(
        chat.conditional
            .as_ref()
            .unwrap()
            .evaluate(Some(Some(11)), None),
        Some(false)
    );
}
