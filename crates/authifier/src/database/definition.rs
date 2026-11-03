use crate::{
    models::{Account, Invite, MFATicket, Session, WebPushSubscription},
    Result, Success,
};

use super::Migration;

#[async_trait]
pub trait AbstractDatabase: std::marker::Sync {
    /// Run a database migration
    async fn run_migration(&self, migration: Migration) -> Success;

    /// Find account by id
    async fn find_account(&self, id: &str) -> Result<Account>;

    /// Find account by normalised email
    async fn find_account_by_normalised_email(
        &self,
        normalised_email: &str,
    ) -> Result<Option<Account>>;

    /// Find account with active pending email verification
    async fn find_account_with_email_verification(&self, token: &str) -> Result<Account>;

    /// Find account with active password reset
    async fn find_account_with_password_reset(&self, token: &str) -> Result<Account>;

    /// Find account with active deletion token
    async fn find_account_with_deletion_token(&self, token: &str) -> Result<Account>;

    /// Find accounts which are due to be deleted
    async fn find_accounts_due_for_deletion(&self) -> Result<Vec<Account>>;

    /// Find invite by id
    async fn find_invite(&self, id: &str) -> Result<Invite>;

    /// Find session by id
    async fn find_session(&self, id: &str) -> Result<Session>;

    /// Find sessions by user id
    async fn find_sessions(&self, user_id: &str) -> Result<Vec<Session>>;

    /// Find sessions by user ids
    async fn find_sessions_with_subscription(&self, user_ids: &[String]) -> Result<Vec<Session>>;

    /// Find session by token
    async fn find_session_by_token(&self, token: &str) -> Result<Option<Session>>;

    /// Find ticket by token
    async fn find_ticket_by_token(&self, token: &str) -> Result<Option<MFATicket>>;

    // Save account
    async fn save_account(&self, account: &Account) -> Success;

    /// Atomically get or create a session for `(user_id, origin)`.
    ///
    /// Returns `(session, created)`, where `created` is true only when this
    /// call inserted the record. Callers must only publish `CreateSession`
    /// when `created` is true.
    async fn get_or_create_session(&self, session: &Session) -> Result<(Session, bool)>;

    /// Insert a brand new session. Never upserts.
    async fn insert_session(&self, session: &Session) -> Success;

    /// Rename a session. Returns `UnknownUser` if the target does not exist.
    async fn update_session_name(&self, id: &str, name: &str) -> Success;

    /// Refresh `last_seen`. Returns `InvalidSession` if the record is gone.
    async fn update_session_last_seen(&self, id: &str, last_seen: &str) -> Success;

    /// Set (`Some`) or remove (`None`) a web push subscription.
    /// Returns `InvalidSession` if the record is gone.
    async fn set_session_subscription(
        &self,
        id: &str,
        subscription: Option<&WebPushSubscription>,
    ) -> Success;

    /// Save invite
    async fn save_invite(&self, invite: &Invite) -> Success;

    /// Save ticket
    async fn save_ticket(&self, ticket: &MFATicket) -> Success;

    /// Delete session
    async fn delete_session(&self, id: &str) -> Success;

    /// Delete session
    async fn delete_all_sessions(&self, user_id: &str, ignore: Option<String>) -> Success;

    /// Delete ticket
    async fn delete_ticket(&self, id: &str) -> Success;
}
