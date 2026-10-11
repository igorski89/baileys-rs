use super::{Error, Keys, MediaType};
use crate::crypto;
use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use zeroize::Zeroizing;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub mac: [u8; 10],
    pub file_sha256: [u8; 32],
    pub file_enc_sha256: [u8; 32],
    pub file_length: u64,
}
/// Stream to any writer without retaining the full input. Writers may contain partial data on error.
pub fn encrypt_stream(
    mut input: impl Read,
    mut output: impl Write,
    keys: &Keys,
    mut original: Option<&mut dyn Write>,
) -> Result<Metadata, Error> {
    let mut aes = cbc::Encryptor::<aes::Aes256>::new(keys.cipher_key().into(), (&keys.iv).into());
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(keys.mac_key()).expect("HMAC key");
    mac.update(&keys.iv);
    let mut plain_hash = Sha256::new();
    let mut enc_hash = Sha256::new();
    let mut count = 0u64;
    let mut block = [0u8; 16];
    let mut used = 0usize;
    let mut buffer = Zeroizing::new(vec![0u8; 64 * 1024]);
    let mut write_block = |block: &mut [u8; 16]| -> Result<(), Error> {
        aes.encrypt_block_mut(block.into());
        mac.update(block);
        enc_hash.update(*block);
        output.write_all(block)?;
        Ok(())
    };
    loop {
        let n = match input.read(&mut buffer) {
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if n == 0 {
            break;
        }
        count = count.checked_add(n as u64).ok_or(Error::Range)?;
        plain_hash.update(&buffer[..n]);
        if let Some(original) = original.as_mut() {
            original.write_all(&buffer[..n])?;
        }
        for byte in &buffer[..n] {
            block[used] = *byte;
            used += 1;
            if used == 16 {
                write_block(&mut block)?;
                used = 0;
            }
        }
    }
    block[used..].fill((16 - used) as u8);
    write_block(&mut block)?;
    let tag: [u8; 10] = mac.finalize().into_bytes()[..10].try_into().unwrap();
    enc_hash.update(tag);
    output.write_all(&tag)?;
    output.flush()?;
    if let Some(original) = original {
        original.flush()?;
    }
    Ok(Metadata {
        mac: tag,
        file_sha256: plain_hash.finalize().into(),
        file_enc_sha256: enc_hash.finalize().into(),
        file_length: count,
    })
}
/// Owned temporary files are deleted on failure/drop; callers may persist them explicitly.
pub struct EncryptedFile {
    pub encrypted: tempfile::NamedTempFile,
    pub original: Option<tempfile::NamedTempFile>,
    pub media_key: Zeroizing<[u8; 32]>,
    pub metadata: Metadata,
}
impl std::fmt::Debug for EncryptedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedFile")
            .field("file_length", &self.metadata.file_length)
            .finish_non_exhaustive()
    }
}
impl EncryptedFile {
    pub fn encrypt(input: impl Read, kind: MediaType, save_original: bool) -> Result<Self, Error> {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).map_err(|_| crypto::Error::Random)?;
        Self::encrypt_with(input, kind, key, save_original)
    }
    pub fn encrypt_with(
        input: impl Read,
        kind: MediaType,
        key: [u8; 32],
        save_original: bool,
    ) -> Result<Self, Error> {
        let key = Zeroizing::new(key);
        let keys = Keys::derive(key.as_ref(), kind)?;
        let mut encrypted = tempfile::Builder::new()
            .prefix(kind.as_str())
            .suffix("-enc")
            .tempfile()?;
        let mut original = if save_original {
            Some(
                tempfile::Builder::new()
                    .prefix(kind.as_str())
                    .suffix("-original")
                    .tempfile()?,
            )
        } else {
            None
        };
        let metadata = {
            let mut encrypted_writer = std::io::BufWriter::with_capacity(64 * 1024, &mut encrypted);
            let mut original_writer = original
                .as_mut()
                .map(|f| std::io::BufWriter::with_capacity(64 * 1024, f));
            encrypt_stream(
                input,
                &mut encrypted_writer,
                &keys,
                original_writer.as_mut().map(|f| f as &mut dyn Write),
            )?
        };
        Ok(Self {
            encrypted,
            original,
            media_key: key,
            metadata,
        })
    }
}
/// Full-file decryption with truncated HMAC verification, unlike the legacy download helper.
pub fn decrypt_verified(upload: &[u8], keys: &Keys) -> Result<Vec<u8>, Error> {
    let offset = upload
        .len()
        .checked_sub(10)
        .ok_or(crypto::Error::Authentication)?;
    let (ciphertext, tag) = upload.split_at(offset);
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(keys.mac_key()).expect("HMAC key");
    mac.update(&keys.iv);
    mac.update(ciphertext);
    mac.verify_truncated_left(tag)
        .map_err(|_| crypto::Error::Authentication)?;
    Ok(crypto::aes_decrypt_cbc_with_iv(
        ciphertext,
        keys.cipher_key(),
        &keys.iv,
    )?)
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Range {
    pub start_byte: Option<u64>,
    pub end_byte: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadPlan {
    pub range: Range,
    pub fetch_start: u64,
    /// Inclusive HTTP range endpoint, as upstream emits it.
    pub fetch_end: Option<u64>,
    pub previous_block_iv: bool,
    pub plaintext_offset: u64,
    pub auto_padding: bool,
}
impl DownloadPlan {
    pub fn new(range: Range) -> Result<Self, Error> {
        let chunk = range.start_byte.unwrap_or(0) / 16 * 16;
        let end = range
            .end_byte
            .filter(|e| *e != 0)
            .map(|e| (e / 16 * 16).checked_add(16).ok_or(Error::Range))
            .transpose()?;
        Ok(Self {
            range,
            fetch_start: chunk.saturating_sub(16),
            fetch_end: end,
            previous_block_iv: chunk != 0,
            plaintext_offset: chunk,
            auto_padding: end.is_none(),
        })
    }
    pub fn range_header(&self) -> Option<String> {
        (self.fetch_start != 0 || self.fetch_end.is_some()).then(|| {
            format!(
                "bytes={}-{}",
                self.fetch_start,
                self.fetch_end.map(|e| e.to_string()).unwrap_or_default()
            )
        })
    }
}
/// Decrypt the body returned for a DownloadPlan. Matches legacy CBC download behavior:
/// trailing partial blocks (including the MAC) are ignored; start-only ranges emit no bytes.
/// Full authenticated decryption is available through decrypt_verified.
pub fn decrypt_download(
    mut input: impl Read,
    mut output: impl Write,
    keys: &Keys,
    plan: &DownloadPlan,
) -> Result<u64, Error> {
    let mut cipher =
        cbc::Decryptor::<aes::Aes256>::new(keys.cipher_key().into(), (&keys.iv).into());
    let mut need_iv = plan.previous_block_iv;
    let mut block = [0u8; 16];
    let mut used = 0usize;
    let mut pending: Option<[u8; 16]> = None;
    let mut offset = plan.plaintext_offset;
    let mut emitted = 0u64;
    let ranged = plan.range.start_byte.is_some_and(|s| s != 0)
        || plan.range.end_byte.is_some_and(|e| e != 0);
    let mut push = |data: &[u8]| -> Result<(), Error> {
        let selected = if ranged {
            let start = plan
                .range
                .start_byte
                .unwrap_or(0)
                .saturating_sub(offset)
                .min(data.len() as u64) as usize;
            let end = plan
                .range
                .end_byte
                .unwrap_or(0)
                .saturating_sub(offset)
                .min(data.len() as u64) as usize;
            &data[start.min(end)..end]
        } else {
            data
        };
        output.write_all(selected)?;
        emitted = emitted
            .checked_add(selected.len() as u64)
            .ok_or(Error::Range)?;
        offset = offset.checked_add(data.len() as u64).ok_or(Error::Range)?;
        Ok(())
    };
    let mut buffer = Zeroizing::new(vec![0u8; 64 * 1024]);
    loop {
        let n = match input.read(&mut buffer) {
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if n == 0 {
            break;
        }
        for byte in &buffer[..n] {
            block[used] = *byte;
            used += 1;
            if used != 16 {
                continue;
            }
            used = 0;
            if need_iv {
                cipher =
                    cbc::Decryptor::<aes::Aes256>::new(keys.cipher_key().into(), (&block).into());
                need_iv = false;
                continue;
            }
            cipher.decrypt_block_mut((&mut block).into());
            if plan.auto_padding {
                if let Some(previous) = pending.replace(block) {
                    push(&previous)?;
                }
            } else {
                push(&block)?;
            }
        }
    }
    if need_iv {
        return Err(crypto::Error::Padding.into());
    }
    if plan.auto_padding {
        let final_block = pending.ok_or(crypto::Error::Padding)?;
        let padding = usize::from(final_block[15]);
        if padding == 0
            || padding > 16
            || !final_block[16 - padding..]
                .iter()
                .all(|b| usize::from(*b) == padding)
        {
            return Err(crypto::Error::Padding.into());
        }
        push(&final_block[..16 - padding])?;
    }
    Ok(emitted)
}
