//! Connect a real account: pair with a code on first run, then log events and
//! optionally send one text message.
//!
//! ```sh
//! PHONE=4915112345678 cargo run --example live -- ./auth-live
//! TARGET=4915198765432@s.whatsapp.net TEXT="hello" cargo run --example live -- ./auth-live
//! ```
//!
//! The directory holds the SQLite credentials, so later runs reuse the pairing.
//! `PHONE` (digits only, with country code) is needed only while unpaired: enter
//! the printed code on the phone under Linked devices > Link with phone number.
//! Use a spare account; unofficial clients can get a number banned.
//!
//! The protocol version in `defaults::WA_VERSION` matched upstream's
//! `baileys-version.json` when this was written. A stale version is rejected at
//! login with a 405 close code.
use baileys_rs::{
    auth::SqliteAuthStore,
    messages::generation::{Content, Modifiers},
    socket::{Config, ConnectionState, Event, SendOptions, Socket},
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let folder = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "auth-live".into());
    let store = SqliteAuthStore::open(&folder)?;
    let mut sent = false;
    loop {
        let paired = store.load_credentials()?.me.is_some();
        let (socket, mut events) = Socket::connect(Config::default(), store.clone()).await?;
        if !paired {
            let phone = std::env::var("PHONE")
                .map_err(|_| "set PHONE to the account's number to pair this device")?;
            let code = socket.request_pairing_code(phone, None).await?;
            println!("pairing code: {code}");
        }
        let code = loop {
            let event = match events.recv().await {
                Ok(event) => event,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    println!("skipped {missed} events");
                    continue;
                }
                Err(_) => break 428,
            };
            println!("{event:?}");
            match event {
                Event::Connection(ConnectionState::Closed { code }) => break code,
                Event::Connection(ConnectionState::Open) if !sent => {
                    sent = true;
                    if let Ok(target) = std::env::var("TARGET") {
                        let text = std::env::var("TEXT").unwrap_or_else(|_| "hello".into());
                        let message = socket
                            .send_message(
                                &target,
                                &Content::text(text),
                                &Modifiers::default(),
                                SendOptions::default(),
                            )
                            .await?;
                        println!("sent: {:?}", message.and_then(|message| message.key));
                    }
                }
                _ => {}
            }
        };
        match code {
            // The server asks for a restart once pairing completes.
            515 => println!("restart requested, reconnecting"),
            401 => {
                println!("logged out; delete {folder} to pair again");
                return Ok(());
            }
            code => {
                println!("connection closed ({code}), reconnecting in 3s");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
