use baileys_rs::jid::{self, Jid};

#[test]
fn parses_domains_and_devices_and_normalizes_legacy_users() {
    for (server, domain) in [
        ("s.whatsapp.net", 0),
        ("lid", 1),
        ("hosted", 128),
        ("hosted.lid", 129),
    ] {
        let jid = Jid::parse(&format!("123:42@{server}")).unwrap();
        assert_eq!(jid.user, "123");
        assert_eq!(jid.device, Some(42));
        assert_eq!(jid.domain_type, domain);
        assert_eq!(jid.normalized(), format!("123@{server}"));
    }
    assert_eq!(jid::normalized_user("123_1:2@c.us"), "123@s.whatsapp.net");
    assert_eq!(Jid::parse("123_1:2@s.whatsapp.net").unwrap().domain_type, 1);
    assert_eq!(jid::encode("123", "lid", Some(0), Some(0)), "123@lid");
    assert_eq!(
        jid::transfer_device("123:42@s.whatsapp.net", "456@lid").unwrap(),
        "456:42@lid"
    );
    assert!(jid::are_same_user("123:2@lid", "123@s.whatsapp.net"));
    assert!(Jid::parse("invalid").is_none());
    assert!(Jid::parse("123:bad@lid").is_none());
}

#[test]
fn classifies_status_bot_and_user_identifiers() {
    assert!(
        Jid::parse("status@broadcast")
            .unwrap()
            .is_status_broadcast()
    );
    assert!(Jid::parse("13135551234@c.us").unwrap().is_bot());
    assert!(Jid::parse("13165550012@c.us").unwrap().is_bot());
    assert!(!Jid::parse("13165550012@lid").unwrap().is_bot());
    assert!(Jid::parse("123@hosted.lid").unwrap().is_hosted_lid_user());
}
