use bson::{to_document, DateTime, Document};
use chrono::{Duration, Utc};
use futures::{stream::TryStreamExt, StreamExt};
use mongodb::options::{
    Collation, CollationStrength, FindOneAndUpdateOptions, FindOneOptions, IndexOptions,
    ReturnDocument, UpdateOptions,
};
use mongodb::IndexModel;
use std::{ops::Deref, str::FromStr, time::Duration as StdDuration};
use ulid::Ulid;

use crate::{
    models::{Account, Invite, MFATicket, Session, WebPushSubscription},
    Error, Result, Success,
};

use super::{definition::AbstractDatabase, Migration};

#[derive(Clone)]
pub struct MongoDb(pub mongodb::Database);

impl Deref for MongoDb {
    type Target = mongodb::Database;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn is_duplicate_key_kind(kind: &mongodb::error::ErrorKind) -> bool {
    use mongodb::error::{ErrorKind, WriteFailure};

    match kind {
        ErrorKind::Write(WriteFailure::WriteError(error)) => error.code == 11000,
        ErrorKind::Command(error) => error.code == 11000,
        _ => false,
    }
}

fn is_duplicate_key(error: &mongodb::error::Error) -> bool {
    is_duplicate_key_kind(error.kind.as_ref())
}

#[cfg(test)]
mod test_hooks {
    use std::{cell::{Cell, RefCell}, future::Future, pin::Pin};

    pub type Hook = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

    thread_local! {
        static AFTER_FAILURE: RefCell<Option<Hook>> = const { RefCell::new(None) };
        static FORCE_NONE: Cell<usize> = const { Cell::new(0) };
    }

    pub fn set_after_failure(hook: Hook) {
        AFTER_FAILURE.with(|slot| *slot.borrow_mut() = Some(hook));
    }

    pub fn take_after_failure() -> Option<Hook> {
        AFTER_FAILURE.with(|slot| slot.borrow_mut().take())
    }

    pub fn set_force_none(attempts: usize) {
        FORCE_NONE.with(|cell| cell.set(attempts));
    }

    pub fn take_force_none() -> bool {
        FORCE_NONE.with(|cell| {
            let remaining = cell.get();
            if remaining > 0 {
                cell.set(remaining - 1);
                true
            } else {
                false
            }
        })
    }
}

fn index(
    name: &str,
    keys: Document,
    unique: bool,
    partial: Option<Document>,
) -> IndexModel {
    let options = match partial {
        Some(partial) => IndexOptions::builder()
            .name(name.to_string())
            .unique(unique)
            .partial_filter_expression(partial)
            .build(),
        None => IndexOptions::builder()
            .name(name.to_string())
            .unique(unique)
            .build(),
    };

    IndexModel::builder().keys(keys).options(options).build()
}

fn same_keys_ordered(left: &Document, right: &Document) -> bool {
    // Ordered (key, value) pairs: direction/type must match too.
    left.iter().eq(right.iter())
}

fn same_index_spec(left: &IndexModel, right: &IndexModel) -> bool {
    let left_options = left.options.as_ref();
    let right_options = right.options.as_ref();

    same_keys_ordered(&left.keys, &right.keys)
        && left_options.and_then(|options| options.unique).unwrap_or(false)
            == right_options
                .and_then(|options| options.unique)
                .unwrap_or(false)
        && left_options.and_then(|options| options.partial_filter_expression.clone())
            == right_options.and_then(|options| options.partial_filter_expression.clone())
        && left_options.and_then(|options| options.sparse).unwrap_or(false)
            == right_options.and_then(|options| options.sparse).unwrap_or(false)
        && left_options.and_then(|options| options.hidden).unwrap_or(false)
            == right_options.and_then(|options| options.hidden).unwrap_or(false)
        && match (
            left_options.and_then(|options| options.collation.as_ref()),
            right_options.and_then(|options| options.collation.as_ref()),
        ) {
            (None, None) => true,
            (Some(left), Some(right)) => format!("{left:?}") == format!("{right:?}"),
            _ => false,
        }
}

/// Create an index unless an equivalent one already exists under any name.
///
/// Used by the legacy authifier migration so it can run against databases
/// provisioned by the newer bootstrap (which may use different index names).
async fn ensure_index(
    collection: &mongodb::Collection<Document>,
    collection_name: &'static str,
    desired: IndexModel,
) -> Success {
    let name = desired
        .options
        .as_ref()
        .and_then(|options| options.name.clone())
        .unwrap_or_default();

    let existing: Vec<IndexModel> = collection
        .list_indexes()
        .await
        .map_err(|_| Error::DatabaseError {
            operation: "list_indexes",
            with: collection_name,
        })?
        .try_collect()
        .await
        .map_err(|_| Error::DatabaseError {
            operation: "list_indexes",
            with: collection_name,
        })?;

    if existing
        .iter()
        .any(|existing| same_index_spec(existing, &desired))
    {
        return Ok(());
    }

    if existing.iter().any(|existing| {
        existing
            .options
            .as_ref()
            .and_then(|options| options.name.as_deref())
            == Some(name.as_str())
    }) {
        return Err(Error::DatabaseError {
            operation: "create_index",
            with: collection_name,
        });
    }

    collection
        .create_index(desired)
        .await
        .map_err(|_| Error::DatabaseError {
            operation: "create_index",
            with: collection_name,
        })?;

    Ok(())
}

async fn retry_sleep(millis: u64) {
    #[cfg(feature = "tokio-runtime")]
    tokio::time::sleep(StdDuration::from_millis(millis)).await;

    #[cfg(all(feature = "async-std-runtime", not(feature = "tokio-runtime")))]
    async_std::task::sleep(StdDuration::from_millis(millis)).await;

    #[cfg(not(any(feature = "tokio-runtime", feature = "async-std-runtime")))]
    std::thread::sleep(StdDuration::from_millis(millis));
}

#[async_trait]
impl AbstractDatabase for MongoDb {
    /// Run a database migration
    async fn run_migration(&self, migration: Migration) -> Success {
        match migration {
            #[cfg(debug_assertions)]
            Migration::WipeAll => {
                // Drop the entire database
                self.drop().await.unwrap();
            }
            Migration::M2022_06_03EnsureUpToSpec => {
                // Make sure all collections exist
                let list = self.list_collection_names().await.unwrap();
                let collections = ["accounts", "sessions", "invites", "mfa_tickets"];

                for name in collections {
                    if !list.contains(&name.to_string()) {
                        self.create_collection(name).await.unwrap();
                    }
                }

                // Setup indexes for `accounts`.
                //
                // The unique `email` / `email_normalised` indexes are
                // intentionally not created here: the SkyAuth registration
                // path stores an empty `email_normalised` for every account,
                // so a unique index would make the second registration fail.
                // Existing unique indexes are left untouched.
                let accounts = self.collection::<Document>("accounts");

                ensure_index(
                    &accounts,
                    "accounts",
                    index(
                        "email_verification",
                        doc! { "verification.token": 1_i32 },
                        false,
                        None,
                    ),
                )
                .await?;
                ensure_index(
                    &accounts,
                    "accounts",
                    index(
                        "password_reset",
                        doc! { "password_reset.token": 1_i32 },
                        false,
                        None,
                    ),
                )
                .await?;

                // Setup indexes for `sessions`.
                let sessions = self.collection::<Document>("sessions");

                ensure_index(
                    &sessions,
                    "sessions",
                    index("token", doc! { "token": 1_i32 }, true, None),
                )
                .await?;
                ensure_index(
                    &sessions,
                    "sessions",
                    index("user_id", doc! { "user_id": 1_i32 }, false, None),
                )
                .await?;

                // Setup index for `mfa_tickets`.
                let mfa_tickets = self.collection::<Document>("mfa_tickets");

                ensure_index(
                    &mfa_tickets,
                    "mfa_tickets",
                    index("token", doc! { "token": 1_i32 }, true, None),
                )
                .await?;
            }
            Migration::M2022_06_09AddIndexForDeletion => {
                if self
                    .collection::<Document>("accounts")
                    .list_index_names()
                    .await
                    .expect("list of index names")
                    .contains(&"account_deletion".to_owned())
                {
                    return Ok(());
                }

                self.run_command(doc! {
                    "createIndexes": "accounts",
                    "indexes": [
                        {
                            "key": {
                                "deletion.token": 1
                            },
                            "name": "account_deletion"
                        }
                    ]
                })
                .await
                .unwrap();
            }
            Migration::M2025_02_20AddLastSeenToSession => {
                // i had to remove the transaction code which was a lot more sensible
                // but required transactions hence replica sets =(           - insert
                // check commits 2025-05-14 (authifier/authifier) for old code

                loop {
                    #[derive(Deserialize)]
                    struct SessionId {
                        _id: Ulid,
                    }

                    let sessions: Vec<SessionId> = self
                        .collection("sessions")
                        .find(doc! {
                            "$or": [
                                { "last_seen": { "$exists": false } },
                                { "last_seen": "1970-01-01T00:00:00.000Z" }
                            ]
                        })
                        .limit(50_000) // about 400 batches for 2 million
                        .await
                        .expect("Failed to create cursor for sessions!")
                        .map(|doc| doc.expect("id and username"))
                        .collect()
                        .await;

                    if sessions.is_empty() {
                        break;
                    }

                    for session in sessions {
                        let timestamp = iso8601_timestamp::Timestamp::UNIX_EPOCH
                            + iso8601_timestamp::Duration::seconds(
                                session._id.datetime().timestamp(),
                            );

                        self.collection::<Document>("sessions")
                            .update_one(
                                doc! {
                                    "_id": &session._id.to_string(),
                                },
                                doc! {
                                    "$set": {
                                        "last_seen": timestamp.format().to_string()
                                    }
                                },
                            )
                            .await
                            .expect("Failed to update a session.");
                    }
                }
            }
        }

        Ok(())
    }

    /// Find account by id
    async fn find_account(&self, id: &str) -> Result<Account> {
        self.collection("accounts")
            .find_one(doc! {
                "_id": id
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "account",
            })?
            .ok_or(Error::UnknownUser)
    }

    /// Find account by normalised email
    async fn find_account_by_normalised_email(
        &self,
        normalised_email: &str,
    ) -> Result<Option<Account>> {
        self.collection("accounts")
            .find_one(doc! {
                "email_normalised": normalised_email
            })
            .with_options(
                FindOneOptions::builder()
                    .collation(
                        Collation::builder()
                            .locale("en")
                            .strength(CollationStrength::Secondary)
                            .build(),
                    )
                    .build(),
            )
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "account",
            })
    }

    /// Find account with active pending email verification
    async fn find_account_with_email_verification(&self, token: &str) -> Result<Account> {
        self.collection("accounts")
            .find_one(doc! {
                "verification.token": token,
                "verification.expiry": {
                    "$gte": DateTime::now().try_to_rfc3339_string().expect("failed to convert to rfc3339 time string")
                }
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "account",
            })?
            .ok_or(Error::InvalidToken)
    }

    /// Find account with active password reset
    async fn find_account_with_password_reset(&self, token: &str) -> Result<Account> {
        self.collection("accounts")
            .find_one(doc! {
                "password_reset.token": token,
                "password_reset.expiry": {
                    "$gte": DateTime::now().try_to_rfc3339_string().expect("failed to convert to rfc3339 time string")
                }
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "account",
            })?
            .ok_or(Error::InvalidToken)
    }

    /// Find account with active deletion token
    async fn find_account_with_deletion_token(&self, token: &str) -> Result<Account> {
        self.collection("accounts")
            .find_one(doc! {
                "deletion.token": token,
                "deletion.expiry": {
                    "$gte": DateTime::now().try_to_rfc3339_string().expect("failed to convert to rfc3339 time string")
                }
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "account",
            })?
            .ok_or(Error::InvalidToken)
    }

    /// Find accounts which are due to be deleted
    async fn find_accounts_due_for_deletion(&self) -> Result<Vec<Account>> {
        self.collection("accounts")
            .find(doc! {
                "deletion.status": "Scheduled",
                "deletion.after": {
                    "$lte": DateTime::now().try_to_rfc3339_string().expect("failed to convert to rfc3339 time string")
                }
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find",
                with: "accounts",
            })?
            .try_collect()
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "collect",
                with: "accounts",
            })
    }

    /// Find invite by id
    async fn find_invite(&self, id: &str) -> Result<Invite> {
        self.collection("invites")
            .find_one(doc! {
                "_id": id
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "invite",
            })?
            .ok_or(Error::InvalidInvite)
    }

    /// Find session by id
    async fn find_session(&self, id: &str) -> Result<Session> {
        self.collection("sessions")
            .find_one(doc! {
                "_id": id
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "session",
            })?
            .ok_or(Error::UnknownUser)
    }

    /// Find sessions by user id
    async fn find_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
        self.collection::<Session>("sessions")
            .find(doc! {
                "user_id": user_id
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find",
                with: "sessions",
            })?
            .try_collect()
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "collect",
                with: "sessions",
            })
    }

    /// Find sessions by user ids
    async fn find_sessions_with_subscription(&self, user_ids: &[String]) -> Result<Vec<Session>> {
        self.collection::<Session>("sessions")
            .find(doc! {
                "user_id": {
                    "$in": user_ids
                },
                "subscription": {
                    "$exists": true
                }
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find",
                with: "sessions",
            })?
            .try_collect()
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "collect",
                with: "sessions",
            })
    }

    /// Find session by token
    async fn find_session_by_token(&self, token: &str) -> Result<Option<Session>> {
        self.collection("sessions")
            .find_one(doc! {
                "token": token
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "session",
            })?
            .ok_or(Error::UnknownUser)
    }

    /// Find ticket by token
    /// <br>
    /// Ticket is only valid for 1 minute
    async fn find_ticket_by_token(&self, token: &str) -> Result<Option<MFATicket>> {
        let ticket: MFATicket = self
            .collection("mfa_tickets")
            .find_one(doc! {
                "token": token
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "find_one",
                with: "mfa_ticket",
            })?
            .ok_or(Error::InvalidToken)?;

        if let Ok(ulid) = Ulid::from_str(&ticket.id) {
            if (ulid.datetime() + Duration::minutes(1)) > Utc::now() {
                Ok(Some(ticket))
            } else {
                Err(Error::InvalidToken)
            }
        } else {
            Err(Error::InvalidToken)
        }
    }

    /// Insert a brand new account. Never upserts.
    async fn insert_account(&self, account: &Account) -> Success {
        self.collection::<Account>("accounts")
            .insert_one(account)
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "insert_one",
                with: "account",
            })
            .map(|_| ())
    }

    // Save account
    async fn save_account(&self, account: &Account) -> Success {
        self.collection::<Account>("accounts")
            .update_one(
                doc! {
                    "_id": &account.id
                },
                doc! {
                    "$set": to_document(account).map_err(|_| Error::DatabaseError {
                        operation: "to_document",
                        with: "account",
                    })?
                },
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "upsert_one",
                with: "account",
            })
            .map(|_| ())
    }

    /// Atomically get or create a session by `(user_id, origin)`.
    async fn get_or_create_session(&self, session: &Session) -> Result<(Session, bool)> {
        let origin = session.origin.as_deref().ok_or(Error::IncorrectData {
            with: "session.origin",
        })?;

        let filter = doc! {
            "user_id": &session.user_id,
            "origin": { "$eq": origin, "$type": "string" },
        };
        let update = doc! {
            "$setOnInsert": to_document(session).map_err(|_| Error::DatabaseError {
                operation: "to_document",
                with: "session",
            })?,
        };
        let collection = self.collection::<Session>("sessions");

        for attempt in 0..3u32 {
            #[cfg(test)]
            let force_none = test_hooks::take_force_none();
            #[cfg(not(test))]
            let force_none = false;

            let result = if force_none {
                Ok(None)
            } else {
                collection
                    .find_one_and_update(filter.clone(), update.clone())
                    .with_options(
                        FindOneAndUpdateOptions::builder()
                            .upsert(true)
                            .return_document(ReturnDocument::After)
                            .max_time(StdDuration::from_millis(800))
                            .build(),
                    )
                    .await
            };

            match result {
                Ok(Some(existing)) => {
                    let created = existing.id == session.id;
                    return Ok((existing, created));
                }
                Ok(None) => { /* fall through to re-read */ }
                Err(error) if is_duplicate_key(&error) => { /* fall through */ }
                Err(_) => {
                    return Err(Error::DatabaseError {
                        operation: "find_one_and_update",
                        with: "session",
                    })
                }
            }

            // Test-only injection point: insert a competing session after the
            // upsert failed and before the re-read.
            #[cfg(test)]
            if let Some(hook) = test_hooks::take_after_failure() {
                hook().await;
            }

            match collection
                .find_one(filter.clone())
                .with_options(
                    FindOneOptions::builder()
                        .max_time(StdDuration::from_millis(300))
                        .build(),
                )
                .await
            {
                Ok(Some(existing)) => return Ok((existing, false)),
                Ok(None) => { /* retry */ }
                Err(_) => {
                    return Err(Error::DatabaseError {
                        operation: "find_one",
                        with: "session",
                    })
                }
            }

            retry_sleep(10 * (1u64 << attempt)).await;
        }

        Err(Error::ServiceUnavailable)
    }

    /// Insert a session without upserting.
    async fn insert_session(&self, session: &Session) -> Success {
        self.collection::<Document>("sessions")
            .insert_one(to_document(session).map_err(|_| Error::DatabaseError {
                operation: "to_document",
                with: "session",
            })?)
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "insert_one",
                with: "session",
            })
            .map(|_| ())
    }

    /// Rename a session.
    async fn update_session_name(&self, id: &str, name: &str) -> Success {
        #[cfg(any(test, feature = "test-support"))]
        if crate::test_support::take_update_name_error() {
            return Err(Error::UnknownUser);
        }

        let result = self
            .collection::<Document>("sessions")
            .update_one(doc! { "_id": id }, doc! { "$set": { "name": name } })
            .with_options(UpdateOptions::builder().upsert(false).build())
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "update_one",
                with: "session",
            })?;

        if result.matched_count == 1 {
            Ok(())
        } else {
            Err(Error::UnknownUser)
        }
    }

    /// Refresh `last_seen`.
    async fn update_session_last_seen(&self, id: &str, last_seen: &str) -> Success {
        let result = self
            .collection::<Document>("sessions")
            .update_one(
                doc! { "_id": id },
                doc! { "$set": { "last_seen": last_seen } },
            )
            .with_options(UpdateOptions::builder().upsert(false).build())
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "update_one",
                with: "session",
            })?;

        if result.matched_count == 1 {
            Ok(())
        } else {
            Err(Error::InvalidSession)
        }
    }

    /// Set or remove the push subscription.
    async fn set_session_subscription(
        &self,
        id: &str,
        subscription: Option<&WebPushSubscription>,
    ) -> Success {
        let update = match subscription {
            Some(subscription) => doc! {
                "$set": {
                    "subscription": to_document(subscription).map_err(|_| Error::DatabaseError {
                        operation: "to_document",
                        with: "subscription",
                    })?,
                },
            },
            None => doc! { "$unset": { "subscription": "" } },
        };

        let result = self
            .collection::<Document>("sessions")
            .update_one(doc! { "_id": id }, update)
            .with_options(UpdateOptions::builder().upsert(false).build())
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "update_one",
                with: "session",
            })?;

        if result.matched_count == 1 {
            Ok(())
        } else {
            Err(Error::InvalidSession)
        }
    }

    // is_duplicate_key is defined at module scope below the trait impl.

    /// Save invite
    async fn save_invite(&self, invite: &Invite) -> Success {
        self.collection::<Invite>("invites")
            .update_one(
                doc! {
                    "_id": &invite.id
                },
                doc! {
                    "$set": to_document(invite).map_err(|_| Error::DatabaseError {
                        operation: "to_document",
                        with: "invite",
                    })?,
                },
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "upsert_one",
                with: "invite",
            })
            .map(|_| ())
    }

    /// Save ticket
    async fn save_ticket(&self, ticket: &MFATicket) -> Success {
        self.collection::<MFATicket>("mfa_tickets")
            .update_one(
                doc! {
                    "_id": &ticket.id
                },
                doc! {
                    "$set": to_document(ticket).map_err(|_| Error::DatabaseError {
                        operation: "to_document",
                        with: "ticket",
                    })?,
                },
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "upsert_one",
                with: "ticket",
            })
            .map(|_| ())
    }

    /// Delete session
    async fn delete_session(&self, id: &str) -> Success {
        self.collection::<Session>("sessions")
            .delete_one(doc! {
                "_id": id
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "delete_one",
                with: "session",
            })
            .map(|_| ())
    }

    /// Delete session
    async fn delete_all_sessions(&self, user_id: &str, ignore: Option<String>) -> Success {
        let mut query = doc! {
            "user_id": user_id
        };

        if let Some(id) = ignore {
            query.insert(
                "_id",
                doc! {
                    "$ne": id
                },
            );
        }

        self.collection::<Session>("sessions")
            .delete_many(query)
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "delete_one",
                with: "session",
            })
            .map(|_| ())
    }

    /// Delete ticket
    async fn delete_ticket(&self, id: &str) -> Success {
        self.collection::<MFATicket>("mfa_tickets")
            .delete_one(doc! {
                "_id": id
            })
            .await
            .map_err(|_| Error::DatabaseError {
                operation: "delete_one",
                with: "mfa_ticket",
            })
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Session;
    use mongodb::{options::IndexOptions, IndexModel};

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

    #[test]
    fn duplicate_key_classification_covers_write_and_command_errors() {
        use mongodb::error::{CommandError, ErrorKind, WriteError, WriteFailure};

        let write: WriteError = bson::from_document(doc! {
            "code": 11000_i32,
            "codeName": "DuplicateKey",
            "errmsg": "E11000 duplicate key",
        })
        .unwrap();
        assert!(is_duplicate_key_kind(&ErrorKind::Write(
            WriteFailure::WriteError(write)
        )));

        let command: CommandError = bson::from_document(doc! {
            "code": 11000_i32,
            "codeName": "DuplicateKey",
            "errmsg": "E11000 duplicate key",
        })
        .unwrap();
        assert!(is_duplicate_key_kind(&ErrorKind::Command(command)));

        let other: WriteError = bson::from_document(doc! {
            "code": 121_i32,
            "codeName": "DocumentValidationFailure",
            "errmsg": "validation failed",
        })
        .unwrap();
        assert!(!is_duplicate_key_kind(&ErrorKind::Write(
            WriteFailure::WriteError(other)
        )));
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn get_or_create_session_is_atomic_against_mongo() {
        let uri = std::env::var("MONGODB")
            .expect("MONGODB must be set for Mongo integration tests");

        let client = mongodb::Client::with_uri_str(uri.clone()).await.unwrap();
        let db = client.database("authifier_idempotency_test");
        db.drop().await.ok();

        let index = IndexModel::builder()
            .keys(doc! { "user_id": 1, "origin": 1 })
            .options(
                IndexOptions::builder()
                    .name("user_id_origin_partial".to_string())
                    .unique(true)
                    .partial_filter_expression(doc! { "origin": { "$type": "string" } })
                    .build(),
            )
            .build();
        db.collection::<Document>("sessions")
            .create_index(index)
            .await
            .unwrap();

        let origin = "skyauth:v1:race";
        let futures = (0..20u32).map(|i| {
            let uri = uri.clone();
            async move {
                let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
                let db = MongoDb(client.database("authifier_idempotency_test"));
                let id = format!("01RACE{i:0>21}");
                db.get_or_create_session(&session(&id, &format!("token{i}"), origin))
                    .await
            }
        });

        let results = futures::future::join_all(futures).await;
        let mut created = 0;
        let mut existing = 0;

        for result in results {
            let (_, is_new) = result.expect("get_or_create_session failed");
            if is_new {
                created += 1;
            } else {
                existing += 1;
            }
        }

        let count = db
            .collection::<Document>("sessions")
            .count_documents(doc! { "user_id": "u1", "origin": origin })
            .await
            .unwrap();

        assert_eq!(count, 1, "expected exactly one session, got {count}");
        assert_eq!(created, 1, "expected exactly one creator");
        assert_eq!(existing, 19);

        db.drop().await.ok();
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn get_or_create_session_fails_closed_on_unrelated_duplicate_key() {
        let uri = std::env::var("MONGODB")
            .expect("MONGODB must be set for Mongo integration tests");

        let client = mongodb::Client::with_uri_str(uri).await.unwrap();
        let db = client.database("authifier_duplicate_key_test");
        db.drop().await.ok();

        // A unique token index lets a candidate collide with an unrelated
        // session while its own (user_id, origin) filter matches nothing.
        db.collection::<Document>("sessions")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "token": 1 })
                    .options(
                        IndexOptions::builder()
                            .name("token".to_string())
                            .unique(true)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();

        let mut unrelated = session("01UNRELATED", "conflict-token", "skyauth:v1:other");
        unrelated.user_id = "u0".to_string();
        db.collection::<Document>("sessions")
            .insert_one(to_document(&unrelated).unwrap())
            .await
            .unwrap();

        let candidate = session("01CANDIDATE", "conflict-token", "skyauth:v1:candidate");
        let database = MongoDb(db.clone());

        let error = database
            .get_or_create_session(&candidate)
            .await
            .expect_err("duplicate token must not be reported as a session");

        assert_eq!(error, Error::ServiceUnavailable);

        // The failed upsert must not leave a partial document behind.
        let count = db
            .collection::<Document>("sessions")
            .count_documents(doc! { "user_id": "u1", "origin": "skyauth:v1:candidate" })
            .await
            .unwrap();
        assert_eq!(count, 0);

        db.drop().await.ok();
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn get_or_create_session_rereads_after_duplicate_key() {
        let uri = std::env::var("MONGODB")
            .expect("MONGODB must be set for Mongo integration tests");

        let client = mongodb::Client::with_uri_str(uri).await.unwrap();
        let db = client.database("authifier_reread_hit_test");
        db.drop().await.ok();

        db.collection::<Document>("sessions")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "token": 1 })
                    .options(
                        IndexOptions::builder()
                            .name("token".to_string())
                            .unique(true)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();

        // Force the candidate's upsert to fail on the unique token, while its
        // own (user_id, origin) filter matches nothing.
        let mut blocker = session("01BLOCKER", "conflict", "skyauth:v1:blocker");
        blocker.user_id = "u0".to_string();
        db.collection::<Document>("sessions")
            .insert_one(to_document(&blocker).unwrap())
            .await
            .unwrap();

        let candidate = session("01CANDIDATE", "conflict", "skyauth:v1:hit");
        let database = MongoDb(db.clone());

        // Between the failed upsert and the re-read, insert a session that
        // matches the candidate's filter.
        let hook_db = db.clone();
        let winner = session("01WINNER", "winner-token", "skyauth:v1:hit");
        test_hooks::set_after_failure(Box::new(move || {
            let db = hook_db.clone();
            let winner = winner.clone();
            Box::pin(async move {
                db.collection::<Document>("sessions")
                    .insert_one(to_document(&winner).unwrap())
                    .await
                    .unwrap();
            })
        }));

        let (session, created) = database
            .get_or_create_session(&candidate)
            .await
            .expect("the re-read must find the session inserted after E11000");

        assert!(!created, "the re-read hit is not a creator");
        assert_eq!(session.id, "01WINNER");
        assert_eq!(session.token, "winner-token");

        let count = db
            .collection::<Document>("sessions")
            .count_documents(doc! { "user_id": "u1", "origin": "skyauth:v1:hit" })
            .await
            .unwrap();
        assert_eq!(count, 1);

        db.drop().await.ok();
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn get_or_create_session_rereads_after_ok_none() {
        let uri = std::env::var("MONGODB")
            .expect("MONGODB must be set for Mongo integration tests");

        let client = mongodb::Client::with_uri_str(uri).await.unwrap();
        let db = client.database("authifier_ok_none_hit_test");
        db.drop().await.ok();

        let winner = session("01NONEHIT", "winner", "skyauth:v1:none-hit");
        db.collection::<Document>("sessions")
            .insert_one(to_document(&winner).unwrap())
            .await
            .unwrap();

        let candidate = session("01CANDIDATE", "candidate", "skyauth:v1:none-hit");
        let database = MongoDb(db.clone());

        // Force the first upsert to return Ok(None); the re-read must find the
        // pre-existing matching document.
        test_hooks::set_force_none(1);

        let (session, created) = database.get_or_create_session(&candidate).await.unwrap();
        assert!(!created);
        assert_eq!(session.id, "01NONEHIT");

        db.drop().await.ok();
    }

    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn get_or_create_session_handles_ok_none_exhaustion() {
        let uri = std::env::var("MONGODB")
            .expect("MONGODB must be set for Mongo integration tests");

        let client = mongodb::Client::with_uri_str(uri).await.unwrap();
        let db = client.database("authifier_ok_none_exhaust_test");
        db.drop().await.ok();

        let candidate = session("01CANDIDATE", "candidate", "skyauth:v1:none-exhaust");
        let database = MongoDb(db.clone());

        test_hooks::set_force_none(3);

        let error = database
            .get_or_create_session(&candidate)
            .await
            .expect_err("three Ok(None) results must exhaust into 503");
        assert_eq!(error, Error::ServiceUnavailable);

        let count = db
            .collection::<Document>("sessions")
            .count_documents(doc! { "user_id": "u1", "origin": "skyauth:v1:none-exhaust" })
            .await
            .unwrap();
        assert_eq!(count, 0);

        db.drop().await.ok();
    }
}
