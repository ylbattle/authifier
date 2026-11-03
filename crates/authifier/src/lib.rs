#[macro_use]
extern crate serde;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate nanoid;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;

#[cfg(feature = "schemas")]
#[macro_use]
extern crate schemars;
#[cfg(feature = "database-mongodb")]
#[macro_use]
extern crate bson;

mod result;
pub use result::*;

pub mod config;
pub mod database;
pub mod derive;
pub mod events;
pub mod r#impl;
pub mod models;
pub mod util;

pub use config::Config;
pub use database::{Database, Migration};
pub use events::AuthifierEvent;

/// Test-only injection points, compiled only for tests or the
/// `test-support` feature; the standard release build does not enable it.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support {
    use std::cell::Cell;

    thread_local! {
        static UPDATE_SESSION_ERROR: Cell<bool> = const { Cell::new(false) };
        static SET_SUBSCRIPTION_ERROR: Cell<bool> = const { Cell::new(false) };
        static UPDATE_NAME_ERROR: Cell<bool> = const { Cell::new(false) };
    }

    pub fn force_update_session_error() {
        UPDATE_SESSION_ERROR.with(|cell| cell.set(true));
    }

    pub fn take_update_session_error() -> bool {
        UPDATE_SESSION_ERROR.with(|cell| cell.replace(false))
    }

    pub fn force_set_subscription_error() {
        SET_SUBSCRIPTION_ERROR.with(|cell| cell.set(true));
    }

    pub fn take_set_subscription_error() -> bool {
        SET_SUBSCRIPTION_ERROR.with(|cell| cell.replace(false))
    }

    pub fn force_update_name_error() {
        UPDATE_NAME_ERROR.with(|cell| cell.set(true));
    }

    pub fn take_update_name_error() -> bool {
        UPDATE_NAME_ERROR.with(|cell| cell.replace(false))
    }
}

#[cfg(all(feature = "async-std-runtime", feature = "tokio-runtime"))]
compile_error!("features async-std-runtime and tokio-runtime cannot be enabled at the same time");

#[cfg(feature = "async-std-runtime")]
use async_std::channel::Sender;
#[cfg(feature = "tokio-runtime")]
use tokio::sync::mpsc::Sender;

/// Authifier state
#[derive(Default, Clone)]
pub struct Authifier {
    pub config: Config,
    pub database: Database,
    pub event_channel: Option<Sender<AuthifierEvent>>,
}

impl Authifier {
    /// Publish an event without blocking the request path.
    ///
    /// A slow or stalled consumer must never turn a successful login into a
    /// timeout; the event is dropped when the channel is full or closed.
    pub async fn publish_event(&self, event: AuthifierEvent) {
        if let Some(sender) = &self.event_channel {
            if sender.try_send(event).is_err() {
                error!("Failed to publish an Authifier event: channel full or closed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Session;

    fn event() -> AuthifierEvent {
        AuthifierEvent::CreateSession {
            session: Session {
                id: "01EVENT".to_string(),
                user_id: "u1".to_string(),
                token: "t1".to_string(),
                name: "Unknown".to_string(),
                last_seen: "2026-11-03T00:00:00.000Z".to_string(),
                origin: None,
                subscription: None,
            },
        }
    }

    #[cfg(feature = "async-std-runtime")]
    fn channel() -> (
        Sender<AuthifierEvent>,
        async_std::channel::Receiver<AuthifierEvent>,
    ) {
        async_std::channel::bounded(1)
    }

    #[cfg(feature = "tokio-runtime")]
    fn channel() -> (
        Sender<AuthifierEvent>,
        tokio::sync::mpsc::Receiver<AuthifierEvent>,
    ) {
        tokio::sync::mpsc::channel(1)
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn publish_event_drops_when_full_without_blocking() {
        #[allow(unused_mut)]
        let (sender, mut receiver) = channel();
        let authifier = Authifier {
            event_channel: Some(sender),
            ..Default::default()
        };

        // Fill the one-slot channel, then publish again. The second call must
        // return immediately instead of waiting for the consumer.
        authifier.publish_event(event()).await;
        authifier.publish_event(event()).await;

        #[cfg(feature = "async-std-runtime")]
        {
            assert!(receiver.recv().await.is_ok());
            assert!(receiver.try_recv().is_err());
        }

        #[cfg(feature = "tokio-runtime")]
        {
            assert!(receiver.recv().await.is_some());
            assert!(receiver.try_recv().is_err());
        }
    }
}
