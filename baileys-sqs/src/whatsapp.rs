//! The socket operations the connector uses, behind a trait so commands and
//! media handling can be exercised without a WhatsApp server.
use baileys_rs::{
    auth::SqliteAuthStore,
    media::DownloadOptions,
    messages::generation::{Content, Modifiers},
    proto,
    socket::{SendOptions, Socket, presence::Presence},
};
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The account's own identity, as upstream's `sock.user`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct User {
    pub id: String,
    pub lid: Option<String>,
}

pub trait WhatsApp: Send + Sync {
    fn user(&self) -> Option<User>;
    fn send_message<'a>(
        &'a self,
        jid: &'a str,
        content: Content,
        modifiers: Modifiers,
        options: SendOptions,
    ) -> BoxFuture<'a, Result<Option<proto::WebMessageInfo>>>;
    fn presence_subscribe<'a>(&'a self, jid: &'a str) -> BoxFuture<'a, Result<()>>;
    fn send_presence_update<'a>(
        &'a self,
        presence: Presence,
        jid: &'a str,
    ) -> BoxFuture<'a, Result<()>>;
    fn read_messages<'a>(&'a self, keys: Vec<proto::MessageKey>) -> BoxFuture<'a, Result<()>>;
    /// The chat's other identity from `signalRepository.lidMapping`: the
    /// phone number for a LID, the LID for a phone number, as a user JID.
    fn alternate_jid<'a>(&'a self, jid: &'a str) -> BoxFuture<'a, Result<Option<String>>>;
    /// `downloadMediaMessage(msg, 'buffer', {})`: no reupload request.
    fn download_media<'a>(
        &'a self,
        message: &'a proto::WebMessageInfo,
    ) -> BoxFuture<'a, Result<Vec<u8>>>;
}

/// A connected baileys-rs socket and its credential store.
#[derive(Clone)]
pub struct Live {
    pub socket: Socket,
    pub store: SqliteAuthStore,
}

impl WhatsApp for Live {
    fn user(&self) -> Option<User> {
        let me = self.store.load_credentials().ok()?.me.clone()?;
        Some(User {
            id: me.id,
            lid: me.lid,
        })
    }

    fn send_message<'a>(
        &'a self,
        jid: &'a str,
        content: Content,
        modifiers: Modifiers,
        options: SendOptions,
    ) -> BoxFuture<'a, Result<Option<proto::WebMessageInfo>>> {
        Box::pin(async move {
            Ok(self
                .socket
                .send_message(jid, &content, &modifiers, options)
                .await?)
        })
    }

    fn presence_subscribe<'a>(&'a self, jid: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { Ok(self.socket.presence_subscribe(jid).await?) })
    }

    fn send_presence_update<'a>(
        &'a self,
        presence: Presence,
        jid: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            Ok(self
                .socket
                .send_presence_update(presence, Some(jid))
                .await?)
        })
    }

    fn read_messages<'a>(&'a self, keys: Vec<proto::MessageKey>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { Ok(self.socket.read_messages(&keys).await?) })
    }

    fn alternate_jid<'a>(&'a self, jid: &'a str) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            let Some(parsed) = baileys_rs::jid::Jid::parse(jid) else {
                return Ok(None);
            };
            let lid = parsed.is_lid_user();
            if !lid && !parsed.is_pn_user() {
                return Ok(None);
            }
            let mapping = self.socket.lid_mapping();
            let jid = jid.to_owned();
            let alternate = tokio::task::spawn_blocking(move || {
                if lid {
                    mapping.get_pn_for_lid(&jid)
                } else {
                    mapping.get_lid_for_pn(&jid)
                }
            })
            .await??;
            Ok(alternate.map(|jid| baileys_rs::jid::normalized_user(&jid)))
        })
    }

    fn download_media<'a>(
        &'a self,
        message: &'a proto::WebMessageInfo,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let mut message = message.clone();
            let (writer, mut reader) = tokio::io::duplex(64 * 1024);
            let collect = tokio::spawn(async move {
                let mut buffer = vec![];
                tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer)
                    .await
                    .map(|_| buffer)
            });
            self.socket
                .download_media_message_to(
                    &mut message,
                    &DownloadOptions::default(),
                    writer,
                    false,
                    None,
                )
                .await?;
            Ok(collect.await??)
        })
    }
}
