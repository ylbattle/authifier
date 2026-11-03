use crate::{
    models::{Account, DeletionInfo, EmailVerification, Invite, MFATicket, Session, WebPushSubscription},
    Error, Result, Success,
};

use futures::lock::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use super::{definition::AbstractDatabase, Migration};

#[derive(Default, Clone)]
pub struct DummyDb {
    pub accounts: Arc<Mutex<HashMap<String, Account>>>,
    pub invites: Arc<Mutex<HashMap<String, Invite>>>,
    pub sessions: Arc<Mutex<HashMap<String, Session>>>,
    pub tickets: Arc<Mutex<HashMap<String, MFATicket>>>,
}

#[async_trait]
impl AbstractDatabase for DummyDb {
    /// Run a database migration
    async fn run_migration(&self, migration: Migration) -> Success {
        println!("skip migration {:?}", migration);
        Ok(())
    }

    /// Find account by id
    async fn find_account(&self, id: &str) -> Result<Account> {
        let accounts = self.accounts.lock().await;
        accounts.get(id).cloned().ok_or(Error::UnknownUser)
    }

    /// Find account by normalised email
    async fn find_account_by_normalised_email(
        &self,
        normalised_email: &str,
    ) -> Result<Option<Account>> {
        let accounts = self.accounts.lock().await;
        Ok(accounts
            .values()
            .find(|account| account.email_normalised == normalised_email)
            .cloned())
    }

    /// Find account with active pending email verification
    async fn find_account_with_email_verification(&self, token_to_match: &str) -> Result<Account> {
        let accounts = self.accounts.lock().await;
        accounts
            .values()
            .find(|account| match &account.verification {
                EmailVerification::Pending { token, .. }
                | EmailVerification::Moving { token, .. } => token == token_to_match,
                _ => false,
            })
            .cloned()
            .ok_or(Error::InvalidToken)
    }

    /// Find account with active password reset
    async fn find_account_with_password_reset(&self, token: &str) -> Result<Account> {
        let accounts = self.accounts.lock().await;
        accounts
            .values()
            .find(|account| {
                if let Some(reset) = &account.password_reset {
                    reset.token == token
                } else {
                    false
                }
            })
            .cloned()
            .ok_or(Error::InvalidToken)
    }

    /// Find account with active deletion token
    async fn find_account_with_deletion_token(&self, token_to_match: &str) -> Result<Account> {
        let accounts = self.accounts.lock().await;
        accounts
            .values()
            .find(|account| {
                if let Some(DeletionInfo::WaitingForVerification { token, .. }) = &account.deletion
                {
                    token == token_to_match
                } else {
                    false
                }
            })
            .cloned()
            .ok_or(Error::InvalidToken)
    }

    /// Find accounts which are due to be deleted
    async fn find_accounts_due_for_deletion(&self) -> Result<Vec<Account>> {
        let accounts = self.accounts.lock().await;
        Ok(accounts
            .values()
            .filter(|account| {
                if let Some(DeletionInfo::Scheduled { .. }) = &account.deletion {
                    unimplemented!()
                } else {
                    false
                }
            })
            .cloned()
            .collect())
    }

    /// Find invite by id
    async fn find_invite(&self, id: &str) -> Result<Invite> {
        let invites = self.invites.lock().await;
        invites.get(id).cloned().ok_or(Error::InvalidInvite)
    }

    /// Find session by id
    async fn find_session(&self, id: &str) -> Result<Session> {
        let sessions = self.sessions.lock().await;
        sessions.get(id).cloned().ok_or(Error::UnknownUser)
    }

    /// Find sessions by user id
    async fn find_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
        let sessions = self.sessions.lock().await;
        Ok(sessions
            .values()
            .filter(|session| session.user_id == user_id)
            .cloned()
            .collect())
    }

    /// Find sessions by user ids
    async fn find_sessions_with_subscription(&self, user_ids: &[String]) -> Result<Vec<Session>> {
        let sessions = self.sessions.lock().await;
        Ok(sessions
            .values()
            .filter(|session| session.subscription.is_some() && user_ids.contains(&session.id))
            .cloned()
            .collect())
    }

    /// Find session by token
    async fn find_session_by_token(&self, token: &str) -> Result<Option<Session>> {
        let sessions = self.sessions.lock().await;
        Ok(sessions
            .values()
            .find(|session| session.token == token)
            .cloned())
    }

    /// Find ticket by token
    async fn find_ticket_by_token(&self, token: &str) -> Result<Option<MFATicket>> {
        let tickets = self.tickets.lock().await;
        Ok(tickets
            .values()
            .find(|ticket| ticket.token == token)
            .cloned())
    }

    // Save account
    async fn save_account(&self, account: &Account) -> Success {
        let mut accounts = self.accounts.lock().await;
        accounts.insert(account.id.to_string(), account.clone());
        Ok(())
    }

    /// Atomically get or create a session by `(user_id, origin)`.
    async fn get_or_create_session(&self, session: &Session) -> Result<(Session, bool)> {
        let origin = session.origin.as_deref().ok_or(Error::IncorrectData {
            with: "session.origin",
        })?;
        let mut sessions = self.sessions.lock().await;

        if let Some(existing) = sessions.values().find(|existing| {
            existing.user_id == session.user_id
                && existing.origin.as_deref() == Some(origin)
        }) {
            return Ok((existing.clone(), false));
        }

        sessions.insert(session.id.clone(), session.clone());
        Ok((session.clone(), true))
    }

    /// Insert a session without upserting.
    async fn insert_session(&self, session: &Session) -> Success {
        let mut sessions = self.sessions.lock().await;
        if sessions.contains_key(&session.id) {
            return Err(Error::DatabaseError {
                operation: "insert_session",
                with: "session",
            });
        }
        sessions.insert(session.id.clone(), session.clone());
        Ok(())
    }

    /// Rename a session.
    async fn update_session_name(&self, id: &str, name: &str) -> Success {
        #[cfg(any(test, feature = "test-support"))]
        if crate::test_support::take_update_name_error() {
            return Err(Error::UnknownUser);
        }

        let mut sessions = self.sessions.lock().await;
        match sessions.get_mut(id) {
            Some(session) => {
                session.name = name.to_string();
                Ok(())
            }
            None => Err(Error::UnknownUser),
        }
    }

    /// Refresh `last_seen`.
    async fn update_session_last_seen(&self, id: &str, last_seen: &str) -> Success {
        #[cfg(any(test, feature = "test-support"))]
        if crate::test_support::take_update_session_error() {
            return Err(Error::DatabaseError {
                operation: "update_session_last_seen",
                with: "session",
            });
        }

        let mut sessions = self.sessions.lock().await;
        match sessions.get_mut(id) {
            Some(session) => {
                session.last_seen = last_seen.to_string();
                Ok(())
            }
            None => Err(Error::InvalidSession),
        }
    }

    /// Set or remove the push subscription.
    async fn set_session_subscription(
        &self,
        id: &str,
        subscription: Option<&WebPushSubscription>,
    ) -> Success {
        #[cfg(any(test, feature = "test-support"))]
        if crate::test_support::take_set_subscription_error() {
            return Err(Error::InvalidSession);
        }

        let mut sessions = self.sessions.lock().await;
        match sessions.get_mut(id) {
            Some(session) => {
                session.subscription = subscription.cloned();
                Ok(())
            }
            None => Err(Error::InvalidSession),
        }
    }

    /// Save invite
    async fn save_invite(&self, invite: &Invite) -> Success {
        let mut invites = self.invites.lock().await;
        invites.insert(invite.id.to_string(), invite.clone());
        Ok(())
    }

    /// Save ticket
    async fn save_ticket(&self, ticket: &MFATicket) -> Success {
        let mut tickets = self.tickets.lock().await;
        tickets.insert(ticket.id.to_string(), ticket.clone());
        Ok(())
    }

    /// Delete session
    async fn delete_session(&self, id: &str) -> Success {
        let mut sessions = self.sessions.lock().await;
        if sessions.remove(id).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidSession)
        }
    }

    /// Delete session
    async fn delete_all_sessions(&self, user_id: &str, ignore: Option<String>) -> Success {
        let mut sessions = self.sessions.lock().await;
        sessions.retain(|_, session| {
            if session.user_id == user_id {
                if let Some(ignore) = &ignore {
                    ignore == &session.id
                } else {
                    false
                }
            } else {
                true
            }
        });

        Ok(())
    }

    /// Delete ticket
    async fn delete_ticket(&self, id: &str) -> Success {
        let mut tickets = self.tickets.lock().await;
        if tickets.remove(id).is_some() {
            Ok(())
        } else {
            Err(Error::InvalidToken)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(id: &str, token: &str, origin: &str) -> Session {
        Session {
            id: id.to_string(),
            user_id: "u1".to_string(),
            token: token.to_string(),
            name: "Unknown".to_string(),
            last_seen: "2026-11-03T00:00:00.000Z".to_string(),
            origin: Some(origin.to_string()),
            subscription: None,
        }
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn get_or_create_session_is_idempotent() {
        let db = DummyDb::default();

        let (created_session, created) = db
            .get_or_create_session(&session("01A", "t1", "skyauth:v1:abc"))
            .await
            .unwrap();
        assert!(created);
        assert_eq!(created_session.id, "01A");

        let (existing, created) = db
            .get_or_create_session(&session("01B", "t2", "skyauth:v1:abc"))
            .await
            .unwrap();
        assert!(!created);
        assert_eq!(existing.id, "01A");
        assert_eq!(existing.token, "t1");

        let (_, created) = db
            .get_or_create_session(&session("01C", "t3", "skyauth:v1:def"))
            .await
            .unwrap();
        assert!(created);

        let mut no_origin = session("01D", "t4", "skyauth:v1:ghi");
        no_origin.origin = None;
        assert!(db.get_or_create_session(&no_origin).await.is_err());
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn update_missing_session_errors() {
        let db = DummyDb::default();
        assert!(matches!(
            db.update_session_last_seen("missing", "2026-11-03T00:00:00.000Z")
                .await,
            Err(Error::InvalidSession)
        ));
        assert!(matches!(
            db.set_session_subscription("missing", None).await,
            Err(Error::InvalidSession)
        ));
        assert!(matches!(
            db.update_session_name("missing", "name").await,
            Err(Error::UnknownUser)
        ));
    }
}
