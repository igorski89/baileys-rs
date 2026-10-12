#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
#[allow(dead_code)]
#[path = "support/receipt_helpers.rs"]
mod support;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    messages, proto,
    socket::{Config, Event, Socket},
};
use prost::Message as _;
use socket_peer::{Peer, ROOT_PRIVATE};
use support::hex;
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
fn store(temp: &tempfile::TempDir) -> SqliteAuthStore {
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    let mut creds: Credentials = serde_json::from_value(fixture["empty"].clone()).unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/connection.json")).unwrap();
    let node =
        baileys_rs::binary::decode(&hex(fixture["pair"][0]["stanza"].as_str().unwrap())).unwrap();
    baileys_rs::connection::configure_successful_pairing(&node, &creds)
        .unwrap()
        .apply(&mut creds);
    store.save_credentials(&creds).unwrap();
    store
}
async fn listener() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        fire_init_queries: false,
        url: format!("ws://{}", listener.local_addr().unwrap()),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        initialize_session: false,
        query_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    (listener, config)
}
use messages::group_effects::{Author, Change, GroupEvent};
use serde_json::{Value, json};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/group_effects.json")).unwrap()
}
fn wire(bytes: Vec<u8>) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn author(out: &mut Value, author: Author) {
    if let Some(v) = author.jid {
        out["author"] = json!(v);
    }
    if let Some(v) = author.pn {
        out["authorPn"] = json!(v);
    }
    if let Some(v) = author.username {
        out["authorUsername"] = json!(v);
    }
}
fn convert(event: GroupEvent) -> Value {
    match event {
        GroupEvent::Participants {
            id,
            author: a,
            action,
            participants,
        } => {
            let mut v = json!({"id":id,"action":action.as_str(),"participants":participants});
            author(&mut v, a);
            json!({"name":"group-participants.update","data":v})
        }
        GroupEvent::Changed {
            id,
            author: a,
            change,
        } => {
            let mut v = json!({"id":id});
            author(&mut v, a);
            match change {
                Change::Announce(x) => v["announce"] = json!(x),
                Change::Restrict(x) => v["restrict"] = json!(x),
                Change::Subject(Some(x)) => v["subject"] = json!(x),
                Change::Description(Some(x)) => v["desc"] = json!(x),
                Change::InviteCode(Some(x)) => v["inviteCode"] = json!(x),
                Change::MemberAddMode(x) => v["memberAddMode"] = json!(x),
                Change::JoinApprovalMode(x) => v["joinApprovalMode"] = json!(x),
                _ => {}
            };
            json!({"name":"groups.update","data":[v]})
        }
        GroupEvent::JoinRequest {
            id,
            author: a,
            participant,
            participant_pn,
            action,
            method,
        } => {
            let mut v = json!({"id":id});
            author(&mut v, a);
            if let Some(x) = participant {
                v["participant"] = x;
            }
            if let Some(x) = participant_pn {
                v["participantPn"] = x;
            }
            if let Some(x) = action {
                v["action"] = json!(x);
            }
            if let Some(x) = method {
                v["method"] = json!(x);
            }
            json!({"name":"group.join-request","data":v})
        }
        GroupEvent::MemberTag {
            group_id,
            label,
            participant,
            participant_alt,
            message_timestamp,
        } => {
            let mut v = json!({"groupId":group_id,"label":label,"messageTimestamp":message_timestamp as i64});
            if let Some(x) = participant {
                v["participant"] = json!(x);
            }
            if let Some(x) = participant_alt {
                v["participantAlt"] = json!(x);
            }
            json!({"name":"group.member-tag.update","data":v})
        }
    }
}
fn chat(c: messages::own::ChatUpdate) -> Value {
    let mut v = json!({"id":c.id});
    if let Some(Some(x)) = c.name {
        v["name"] = json!(x);
    }
    if let Some(Some(x)) = c.description {
        v["description"] = json!(x);
    }
    if let Some(x) = c.messages {
        v["messages"] = json!(
            x.into_iter()
                .map(|m| wire(m.encode_to_vec()))
                .collect::<Vec<_>>()
        );
    }
    if let Some(x) = c.conversation_timestamp {
        v["conversationTimestamp"] = json!(x as i64);
    }
    if let Some(x) = c.unread_count {
        v["unreadCount"] = json!(x);
    }
    if let Some(x) = c.archived {
        v["archived"] = json!(x);
    }
    if let Some(x) = c.read_only {
        v["readOnly"] = json!(x);
    }
    if let Some(x) = c.ephemeral_setting_timestamp {
        v["ephemeralSettingTimestamp"] = json!(x as i64);
    }
    if let Some(x) = c.ephemeral_expiration {
        v["ephemeralExpiration"] = json!(x);
    }
    v
}
#[tokio::test]
async fn group_stub_and_member_label_events_match_original_including_partial_failures() {
    timeout(Duration::from_secs(15),async{
        let temp=tempfile::tempdir().unwrap();let auth=store(&temp);let mut credentials=auth.load_credentials().unwrap();credentials.me.as_mut().unwrap().id="100:7@s.whatsapp.net".into();credentials.account_settings.unarchive_chats=true;auth.save_credentials(&credentials).unwrap();
        let(listener,config)=listener().await;let server=tokio::spawn(async move{let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;peer.send(BinaryNode::new("success")).await;assert_eq!(peer.read().await.tag,"ib");let finish=peer.read().await;assert!(finish.child("finish").is_some());peer.result(&finish).await;peer.closed().await;});let(socket,mut events)=Socket::connect(config,auth).await.unwrap();socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        for case in fixture()["cases"].as_array().unwrap(){let message=proto::WebMessageInfo::decode(hex(case["input"].as_str().unwrap()).as_slice()).unwrap();let envelope=messages::Envelope{message,key_metadata:messages::KeyMetadata{participant_alt:case["metadata"]["participantAlt"].as_str().map(str::to_owned),participant_username:case["metadata"]["participantUsername"].as_str().map(str::to_owned),..Default::default()},category:None,retry_count:None,author:"300@lid".into(),sender:"group@g.us".into()};
            socket.buffer_events();let result=socket.upsert_message(&envelope,messages::own::UpsertKind::Append).await;assert_eq!(result.is_err(),case.get("bufferedError").is_some(),"{case}");socket.flush_events().unwrap();let mut actual=vec![];while let Ok(event)=events.try_recv(){match event{
                Event::GroupEffects(values)=>actual.extend(values.into_iter().map(convert)),Event::ChatsUpdated(values)=>actual.push(json!({"name":"chats.update","data":values.into_iter().map(chat).collect::<Vec<_>>()})),Event::MessageReactions(values)=>actual.push(json!({"name":"messages.reaction","data":values.into_iter().map(|r|json!({"key":r.key.map(|k|wire(k.encode_to_vec())),"reaction":wire(r.reaction.encode_to_vec())})).collect::<Vec<_>>()})),_=>{}
            }}assert_eq!(json!(actual),case["bufferedEvents"],"{case}");
        }
        socket.query(BinaryNode::new("iq").attr("type","get").with_content(NodeContent::Children(vec![BinaryNode::new("finish")])),None).await.unwrap();socket.close().await.unwrap();server.await.unwrap();
    }).await.unwrap();
}
#[tokio::test]
async fn parsed_upserts_update_contacts_self_names_and_reject_missing_keys_without_panicking() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut envelope = messages::Envelope {
            message: proto::WebMessageInfo {
                key: Some(proto::MessageKey {
                    remote_jid: Some("group@g.us".into()),
                    participant: Some("300@lid".into()),
                    id: Some("ID".into()),
                    from_me: Some(false),
                }),
                push_name: Some("Alice".into()),
                ..Default::default()
            },
            key_metadata: Default::default(),
            category: None,
            retry_count: None,
            author: "300@lid".into(),
            sender: "group@g.us".into(),
        };
        socket
            .upsert_message(&envelope, messages::own::UpsertKind::Append)
            .await
            .unwrap();
        let contact = loop {
            if let Event::ContactsUpdated(values) = events.recv().await.unwrap() {
                break values;
            }
        };
        assert_eq!(contact[0].id, "300@lid");
        assert_eq!(contact[0].notify.as_deref(), Some("Alice"));
        envelope.message.key.as_mut().unwrap().from_me = Some(true);
        envelope.message.push_name = Some("Self Name".into());
        socket
            .upsert_message(&envelope, messages::own::UpsertKind::Append)
            .await
            .unwrap();
        assert_eq!(
            auth.load_credentials()
                .unwrap()
                .me
                .as_ref()
                .unwrap()
                .name
                .as_deref(),
            Some("Self Name")
        );
        envelope.message.key = None;
        assert!(
            socket
                .upsert_message(&envelope, messages::own::UpsertKind::Append)
                .await
                .is_err()
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn incoming_and_own_member_tags_use_shared_publication_after_receipt_and_relay() {
    timeout(Duration::from_secs(8),async{
        let fixture=fixture();let content=proto::WebMessageInfo::decode(hex(fixture["cases"].as_array().unwrap().iter().find(|c|c["name"]=="MEMBER_LABEL"&&c["events"].as_array().unwrap().first().is_some_and(|e|e["data"]["label"]=="Member")).unwrap()["input"].as_str().unwrap()).as_slice()).unwrap().message.unwrap();
        let temp=tempfile::tempdir().unwrap();let auth=store(&temp);let(listener,config)=listener().await;let incoming=BinaryNode::new("message").attr("id","TAG").attr("from","group@g.us").attr("participant","300:2@lid").attr("participant_pn","300@s.whatsapp.net").attr("t","1700000000").with_content(NodeContent::Children(vec![BinaryNode::new("plaintext").with_content(NodeContent::Bytes(content.encode_to_vec()))]));
        let server=tokio::spawn(async move{let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;peer.send(BinaryNode::new("success")).await;assert_eq!(peer.read().await.tag,"ib");peer.send(incoming).await;let receipt=peer.read().await;assert_eq!(receipt.tag,"receipt");assert_eq!(receipt.attrs["participant"],"300:2@lid");let own=peer.read().await;assert_eq!(own.tag,"message");assert_eq!(own.attrs["id"],"OWN_TAG");peer.closed().await;});let(socket,mut events)=Socket::connect(config,auth).await.unwrap();socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let update=loop{if let Event::GroupEffects(updates)=events.recv().await.unwrap(){break updates;}};let GroupEvent::MemberTag{group_id,label,participant,participant_alt,..}=&update[0]else{panic!()};assert_eq!(group_id,"group@g.us");assert_eq!(label,"Member");assert_eq!(participant.as_deref(),Some("300@lid"));assert_eq!(participant_alt.as_deref(),Some("300@s.whatsapp.net"));
        let message=proto::Message{protocol_message:Some(Box::new(proto::message::ProtocolMessage{r#type:Some(proto::message::protocol_message::Type::GroupMemberLabelChange as i32),member_label:Some(Default::default()),..Default::default()})),..Default::default()};let mut message=message;message.protocol_message.as_mut().unwrap().member_label.as_mut().unwrap().label=Some("Owner".into());socket.send_message("channel@newsletter",&messages::generation::Content::PreparedMedia(Box::new(message)),&messages::generation::Modifiers::default(),baileys_rs::socket::SendOptions{message_id:Some("OWN_TAG".into()),..Default::default()}).await.unwrap();
        let update=loop{if let Event::GroupEffects(updates)=events.recv().await.unwrap(){break updates;}};assert!(matches!(&update[0],GroupEvent::MemberTag{group_id,label,..}if group_id=="channel@newsletter"&&label=="Owner"));socket.close().await.unwrap();server.await.unwrap();
    }).await.unwrap();
}
