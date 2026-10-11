use baileys_rs::{
    binary::{BinaryNode, NodeContent},
    media::*,
    proto,
};
use prost::Message as _;
use serde_json::Value;
use std::io::{self, Read, Write};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/media.json")).unwrap()
}
fn hex(value: &Value) -> Vec<u8> {
    value
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn root_key() -> [u8; 32] {
    std::array::from_fn(|i| i as u8)
}
struct Fragmented<'a> {
    bytes: &'a [u8],
    size: usize,
}
impl Read for Fragmented<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = out.len().min(self.size).min(self.bytes.len());
        out[..n].copy_from_slice(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Ok(n)
    }
}
fn node(value: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(value["tag"].as_str().unwrap());
    if let Some(attrs) = value["attrs"].as_object() {
        for (key, value) in attrs {
            n = n.attr(key, value.as_str().unwrap());
        }
    }
    if let Some(c) = value.get("content") {
        n = n.with_content(if let Some(children) = c.as_array() {
            NodeContent::Children(children.iter().map(node).collect())
        } else {
            NodeContent::Bytes(hex(&c["hex"]))
        });
    }
    n
}
fn key(value: &Value) -> proto::MessageKey {
    proto::MessageKey {
        id: value["id"].as_str().map(str::to_owned),
        remote_jid: value["remoteJid"].as_str().map(str::to_owned),
        participant: value["participant"].as_str().map(str::to_owned),
        from_me: value["fromMe"].as_bool(),
    }
}
#[test]
fn all_media_keys_and_encoded_inputs_match_upstream() {
    let f = fixture();
    assert_eq!(f["keys"].as_array().unwrap().len(), MediaType::ALL.len());
    for case in f["keys"].as_array().unwrap() {
        let kind: MediaType = case["type"].as_str().unwrap().parse().unwrap();
        assert_eq!(kind.hkdf_info(), case["info"].as_str().unwrap());
        assert_eq!(kind.upload_path(), case["path"].as_str());
        let k = Keys::derive(&root_key(), kind).unwrap();
        assert_eq!(k.iv.as_slice(), hex(&case["iv"]));
        assert_eq!(k.cipher_key().as_slice(), hex(&case["cipher"]));
        assert_eq!(k.mac_key().as_slice(), hex(&case["mac"]));
    }
    for case in f["encodedKeys"].as_array().unwrap() {
        let result = Keys::derive_base64(case["input"].as_str().unwrap(), MediaType::Image);
        if case.get("error").is_some() {
            assert!(result.is_err());
            continue;
        }
        let k = result.unwrap();
        assert_eq!(k.iv.as_slice(), hex(&case["iv"]), "{case}");
        assert_eq!(k.cipher_key().as_slice(), hex(&case["cipher"]));
        assert_eq!(k.mac_key().as_slice(), hex(&case["mac"]));
    }
}
#[test]
fn streaming_ciphertext_hashes_and_mac_match_upstream() {
    for case in fixture()["encrypted"].as_array().unwrap() {
        let kind = case["type"].as_str().unwrap().parse().unwrap();
        let keys = Keys::derive(&root_key(), kind).unwrap();
        let plain = hex(&case["plain"]);
        for size in [1, 7, 65536] {
            let mut output = Vec::new();
            let mut original = Vec::new();
            let metadata = encrypt_stream(
                Fragmented {
                    bytes: &plain,
                    size,
                },
                &mut output,
                &keys,
                Some(&mut original),
            )
            .unwrap();
            assert_eq!(output, hex(&case["encrypted"]), "{kind:?} {}", plain.len());
            assert_eq!(original, plain);
            assert_eq!(metadata.file_length, case["length"].as_u64().unwrap());
            assert_eq!(metadata.mac.as_slice(), hex(&case["mac"]));
            assert_eq!(metadata.file_sha256.as_slice(), hex(&case["plainHash"]));
            assert_eq!(
                metadata.file_enc_sha256.as_slice(),
                hex(&case["encryptedHash"])
            );
            assert_eq!(decrypt_verified(&output, &keys).unwrap(), plain);
        }
    }
}
#[test]
fn download_ranges_and_host_selection_match_upstream() {
    let f = fixture();
    let case = f["encrypted"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["type"] == "image" && e["length"] == 257)
        .unwrap();
    let upload = hex(&case["encrypted"]);
    let keys = Keys::derive(&root_key(), MediaType::Image).unwrap();
    for case in f["ranges"].as_array().unwrap() {
        let plan = DownloadPlan::new(Range {
            start_byte: case["range"]["startByte"].as_u64(),
            end_byte: case["range"]["endByte"].as_u64(),
        })
        .unwrap();
        assert_eq!(
            plan.range_header().as_deref(),
            case["request"]["headers"]["Range"].as_str(),
            "{case}"
        );
        let body = if plan.range_header().is_some() {
            let start = (plan.fetch_start as usize).min(upload.len());
            let end = plan
                .fetch_end
                .map(|e| (e as usize + 1).min(upload.len()))
                .unwrap_or(upload.len());
            &upload[start.min(end)..end]
        } else {
            &upload
        };
        for size in [1, 13, 65536] {
            let mut output = Vec::new();
            let result =
                decrypt_download(Fragmented { bytes: body, size }, &mut output, &keys, &plan);
            if case.get("error").is_some() {
                assert!(result.is_err(), "{case}");
            } else {
                assert_eq!(result.unwrap(), output.len() as u64);
                assert_eq!(output, hex(&case["output"]), "{case}");
            }
        }
    }
    for case in f["urls"].as_array().unwrap() {
        let input = &case["input"];
        let result = download_url(
            input["directPath"].as_str(),
            input["url"].as_str(),
            input["host"].as_str(),
        );
        if case.get("error").is_some() {
            assert_eq!(result.unwrap_err().status_code(), Some(400));
        } else {
            assert_eq!(result.unwrap(), case["url"].as_str().unwrap(), "{case}");
        }
    }
    assert!(
        DownloadPlan::new(Range {
            end_byte: Some(u64::MAX),
            start_byte: None,
        })
        .is_err()
    );
    for case in f["tokens"].as_array().unwrap() {
        assert_eq!(
            upload_token(case["input"].as_str().unwrap()),
            case["output"].as_str().unwrap()
        );
    }
}
#[test]
fn media_retry_wires_notifications_and_errors_match_upstream() {
    let f = fixture();
    let iv = std::array::from_fn(|i| i as u8);
    for case in f["retryRequests"].as_array().unwrap() {
        let actual =
            retry_request_with_iv(&key(&case["key"]), &root_key(), "100:7@s.whatsapp.net", &iv)
                .unwrap();
        let expected = node(&case["node"]);
        assert_eq!(actual, expected);
        assert_eq!(
            actual
                .child("rmr")
                .unwrap()
                .attrs
                .keys()
                .collect::<Vec<_>>(),
            expected
                .child("rmr")
                .unwrap()
                .attrs
                .keys()
                .collect::<Vec<_>>()
        );
    }
    for case in f["retryEvents"].as_array().unwrap() {
        let result = decode_retry(&node(&case["node"]));
        if case.get("error").is_some() {
            assert!(result.is_err());
            continue;
        }
        let event = result.unwrap();
        assert_eq!(event.key, key(&case["key"]));
        assert_eq!(
            event.error.as_ref().map(|e| u64::from(e.status)),
            case["status"].as_u64()
        );
        if let Some(media) = event.media {
            assert_eq!(media.ciphertext, hex(&case["media"]["ciphertext"]));
            assert_eq!(media.iv, hex(&case["media"]["iv"]));
        }
        if let Some(error) = event.error
            && let Some(code) = case["node"]["content"][1]["attrs"]["code"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
        {
            assert_eq!(error.code, Some(code));
        }
    }
    for case in f["notifications"].as_array().unwrap() {
        let mut data = RetryCiphertext {
            ciphertext: hex(&case["ciphertext"]),
            iv: hex(&case["iv"]),
        };
        assert_eq!(
            decrypt_retry(&data, &root_key(), case["id"].as_str().unwrap())
                .unwrap()
                .encode_to_vec(),
            hex(&case["output"])
        );
        assert!(decrypt_retry(&data, &root_key(), "wrong-id").is_err());
        data.ciphertext[0] ^= 1;
        assert!(decrypt_retry(&data, &root_key(), case["id"].as_str().unwrap()).is_err());
    }
    assert!(decode_retry(&BinaryNode::new("notification")).is_err());
    assert_eq!(retry_status(f64::NAN), None);
    assert_eq!(
        upload_url("host", MediaType::Image, "+/==", "a b+"),
        "https://host/mms/image/-_?auth=a%20b%2B&token=-_"
    );
}
#[test]
fn authenticated_files_reject_tampering_and_temporary_files_are_owned() {
    let file = EncryptedFile::encrypt_with(
        &b"sensitive synthetic plaintext"[..],
        MediaType::Image,
        root_key(),
        true,
    )
    .unwrap();
    let encrypted_path = file.encrypted.path().to_owned();
    let original_path = file.original.as_ref().unwrap().path().to_owned();
    let bytes = std::fs::read(&encrypted_path).unwrap();
    let keys = Keys::derive(&root_key(), MediaType::Image).unwrap();
    assert_eq!(
        std::fs::read(&original_path).unwrap(),
        b"sensitive synthetic plaintext"
    );
    assert_eq!(
        decrypt_verified(&bytes, &keys).unwrap(),
        b"sensitive synthetic plaintext"
    );
    for i in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[i] ^= 1;
        assert!(decrypt_verified(&changed, &keys).is_err());
    }
    for length in 0..bytes.len() {
        assert!(decrypt_verified(&bytes[..length], &keys).is_err());
    }
    assert!(decrypt_verified(&bytes, &Keys::derive(&[99; 32], MediaType::Image).unwrap()).is_err());
    let mut changed = bytes.clone();
    *changed.last_mut().unwrap() ^= 1;
    let mut out = Vec::new();
    decrypt_download(
        &changed[..],
        &mut out,
        &keys,
        &DownloadPlan::new(Range::default()).unwrap(),
    )
    .unwrap();
    assert_eq!(out, b"sensitive synthetic plaintext"); // Legacy download does not check the MAC.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&encrypted_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    drop(file);
    assert!(!encrypted_path.exists());
    assert!(!original_path.exists());
}
struct FailingReader;
impl Read for FailingReader {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("read failure"))
    }
}
struct PartialWriter(Vec<u8>);
impl Write for PartialWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = bytes.len().min(3);
        self.0.extend_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[test]
fn streaming_handles_large_input_partial_writes_and_io_errors() {
    let plain: Vec<u8> = (0..1_048_593).map(|i| (i % 251) as u8).collect();
    let keys = Keys::derive(&root_key(), MediaType::Video).unwrap();
    let mut writer = PartialWriter(Vec::new());
    let meta = encrypt_stream(&plain[..], &mut writer, &keys, None).unwrap();
    assert_eq!(meta.file_length, plain.len() as u64);
    assert_eq!(decrypt_verified(&writer.0, &keys).unwrap(), plain);
    assert!(matches!(
        encrypt_stream(FailingReader, io::sink(), &keys, None),
        Err(Error::Io(_))
    ));
    assert!(matches!(
        EncryptedFile::encrypt(FailingReader, MediaType::Audio, true),
        Err(Error::Io(_))
    ));
    assert!(matches!(
        encrypt_stream(&plain[..], io::sink(), &keys, Some(&mut FailingWriter)),
        Err(Error::Io(_))
    ));
    assert!(matches!(
        encrypt_stream(&plain[..], FailingWriter, &keys, None),
        Err(Error::Io(_))
    ));
}
struct FailingWriter;
impl Write for FailingWriter {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("write failure"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("flush failure"))
    }
}
