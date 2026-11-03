use rocket::{
    http::{ContentType, Header, Status},
    outcome::Outcome,
    request::{self, FromRequest},
    response::{self, Responder},
    Request, Response,
};

use crate::{
    config::{ResolveIp, ShieldValidationInput},
    models::{Account, MFATicket, Session, UnvalidatedTicket, ValidatedTicket},
    Authifier, Error,
};
use iso8601_timestamp::Timestamp;

/// HTTP response builder for Error enum
impl<'r> Responder<'r, 'static> for Error {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let status = match self {
            Error::IncorrectData { .. } => Status::BadRequest,
            Error::DatabaseError { .. } => Status::InternalServerError,
            Error::InternalError => Status::InternalServerError,
            Error::OperationFailed => Status::InternalServerError,
            Error::ServiceUnavailable => Status::ServiceUnavailable,
            Error::RenderFail => Status::InternalServerError,
            Error::MissingHeaders => Status::BadRequest,
            Error::CaptchaFailed => Status::BadRequest,
            Error::BlockedByShield => Status::BadRequest,
            Error::InvalidSession => Status::Unauthorized,
            Error::UnverifiedAccount => Status::Forbidden,
            Error::UnknownUser => Status::NotFound,
            Error::EmailFailed => Status::InternalServerError,
            Error::InvalidCredentials => Status::Unauthorized,
            Error::InvalidToken => Status::Unauthorized,
            Error::MissingInvite => Status::BadRequest,
            Error::InvalidInvite => Status::BadRequest,
            Error::CompromisedPassword => Status::BadRequest,
            Error::ShortPassword => Status::BadRequest,
            Error::Blacklisted => {
                // Fail blacklisted email addresses.
                const RESP: &str = "{\"type\":\"DisallowedContactSupport\", \"email\":\"support@revolt.chat\", \"note\":\"If you see this messages right here, you're probably doing something you shouldn't be.\"}";

                return Response::build()
                    .status(Status::Unauthorized)
                    .sized_body(RESP.len(), std::io::Cursor::new(RESP))
                    .ok();
            }
            Error::LockedOut => Status::Forbidden,
            Error::TotpAlreadyEnabled => Status::BadRequest,
            Error::DisallowedMFAMethod => Status::BadRequest,
        };

        // Serialize the error data structure into JSON.
        let retry_after = matches!(self, Error::ServiceUnavailable);
        let string = json!(self).to_string();

        // Build and send the request.
        let mut builder = Response::build();
        builder
            .sized_body(string.len(), std::io::Cursor::new(string))
            .header(ContentType::new("application", "json"))
            .status(status);

        if retry_after {
            builder.header(Header::new("Retry-After", "1"));
        }

        builder.ok()
    }
}

/// Map a database/guard error to an HTTP status without collapsing
/// infrastructure failures into "invalid credential".
fn status_for_error(error: &Error) -> Status {
    match error {
        Error::ServiceUnavailable => Status::ServiceUnavailable,
        Error::InvalidSession | Error::InvalidToken | Error::InvalidCredentials => {
            Status::Unauthorized
        }
        Error::UnknownUser => Status::NotFound,
        _ => Status::InternalServerError,
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Session {
    type Error = Error;

    #[allow(clippy::collapsible_match)]
    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let header_session_token = request
            .headers()
            .get("x-session-token")
            .next()
            .map(|x| x.to_string());

        match (request.rocket().state::<Authifier>(), header_session_token) {
            (Some(authifier), Some(token)) => {
                match authifier.database.find_session_by_token(&token).await {
                    Ok(Some(mut session)) => {
                        let now = Timestamp::now_utc();
                        let last_seen =
                            Timestamp::parse(&session.last_seen).unwrap_or(Timestamp::UNIX_EPOCH);

                        // Only update if it's more than 1 day old
                        if now.duration_since(last_seen).whole_milliseconds() > 86400 * 1000 {
                            let last_seen = now.format().to_string();
                            match authifier
                                .database
                                .update_session_last_seen(&session.id, &last_seen)
                                .await
                            {
                                Ok(()) => session.last_seen = last_seen,
                                Err(Error::InvalidSession) => {
                                    return Outcome::Error((
                                        Status::Unauthorized,
                                        Error::InvalidSession,
                                    ));
                                }
                                Err(error) => {
                                    return Outcome::Error((status_for_error(&error), error));
                                }
                            }
                        }

                        Outcome::Success(session)
                    }
                    Ok(None) => Outcome::Error((Status::Unauthorized, Error::InvalidSession)),
                    Err(Error::UnknownUser) => {
                        Outcome::Error((Status::Unauthorized, Error::InvalidSession))
                    }
                    Err(error) => Outcome::Error((status_for_error(&error), error)),
                }
            }
            (_, _) => Outcome::Error((Status::Unauthorized, Error::MissingHeaders)),
        }
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Account {
    type Error = Error;

    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        match request.guard::<Session>().await {
            Outcome::Success(session) => {
                let authifier = request.rocket().state::<Authifier>().unwrap();

                if let Ok(account) = authifier.database.find_account(&session.user_id).await {
                    Outcome::Success(account)
                } else {
                    Outcome::Error((Status::InternalServerError, Error::InternalError))
                }
            }
            Outcome::Forward(_) => unreachable!(),
            Outcome::Error(err) => Outcome::Error(err),
        }
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for MFATicket {
    type Error = Error;

    #[allow(clippy::collapsible_match)]
    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let header_mfa_ticket = request
            .headers()
            .get("x-mfa-ticket")
            .next()
            .map(|x| x.to_string());

        match (request.rocket().state::<Authifier>(), header_mfa_ticket) {
            (Some(authifier), Some(token)) => {
                if let Ok(ticket) = authifier.database.find_ticket_by_token(&token).await {
                    if let Some(ticket) = ticket {
                        Outcome::Success(ticket)
                    } else {
                        Outcome::Error((Status::Unauthorized, Error::InvalidToken))
                    }
                } else {
                    Outcome::Error((Status::Unauthorized, Error::InternalError))
                }
            }
            (_, _) => Outcome::Error((Status::Unauthorized, Error::MissingHeaders)),
        }
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ValidatedTicket {
    type Error = Error;

    #[allow(clippy::collapsible_match)]
    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        match request.guard::<MFATicket>().await {
            Outcome::Success(ticket) => {
                if ticket.validated {
                    let authifier = request
                        .rocket()
                        .state::<Authifier>()
                        .expect("This code is unreachable.");

                    if ticket.claim(authifier).await.is_ok() {
                        Outcome::Success(ValidatedTicket(ticket))
                    } else {
                        Outcome::Error((Status::InternalServerError, Error::InternalError))
                    }
                } else {
                    Outcome::Error((Status::Forbidden, Error::InvalidToken))
                }
            }
            Outcome::Forward(f) => Outcome::Forward(f),
            Outcome::Error(err) => Outcome::Error(err),
        }
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for UnvalidatedTicket {
    type Error = Error;

    #[allow(clippy::collapsible_match)]
    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        match request.guard::<MFATicket>().await {
            Outcome::Success(ticket) => {
                if !ticket.validated {
                    Outcome::Success(UnvalidatedTicket(ticket))
                } else {
                    Outcome::Error((Status::Forbidden, Error::InvalidToken))
                }
            }
            Outcome::Forward(f) => Outcome::Forward(f),
            Outcome::Error(err) => Outcome::Error(err),
        }
    }
}

fn resolve_ip(request: &'_ Request<'_>, config: &ResolveIp) -> String {
    match config {
        ResolveIp::Remote => request
            .remote()
            .map(|x| x.ip().to_string())
            .unwrap_or_default(),
        ResolveIp::Cloudflare => request
            .headers()
            .get_one("CF-Connecting-IP")
            .map(|x| x.to_string())
            .unwrap_or_else(|| resolve_ip(request, &ResolveIp::Remote)),
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ShieldValidationInput {
    type Error = Error;

    #[allow(clippy::collapsible_match)]
    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        if let Some(authifier) = request.rocket().state::<Authifier>() {
            Outcome::Success(ShieldValidationInput {
                ip: Some(resolve_ip(request, &authifier.config.resolve_ip)),
                headers: Some(
                    request
                        .headers()
                        .iter()
                        .map(|entry| (entry.name.to_string(), entry.value.to_string()))
                        .collect(),
                ),
                ..Default::default()
            })
        } else {
            Outcome::Error((Status::InternalServerError, Error::InternalError))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::http::Header;
    use rocket::local::{asynchronous::Client as AsyncClient, blocking::Client};
    use rocket::{http::Method, route::BoxFuture, Route};
    fn unavailable<'r>(request: &'r Request<'_>, _data: rocket::Data<'r>) -> BoxFuture<'r> {
        let outcome = match Error::ServiceUnavailable.respond_to(request) {
            Ok(response) => Outcome::Success(response),
            Err(status) => Outcome::Error(status),
        };

        Box::pin(async move { outcome })
    }

    fn guarded<'r>(request: &'r Request<'_>, _data: rocket::Data<'r>) -> BoxFuture<'r> {
        Box::pin(async move {
            match request.guard::<Session>().await {
                Outcome::Success(_) => match "ok".respond_to(request) {
                    Ok(response) => Outcome::Success(response),
                    Err(status) => Outcome::Error(status),
                },
                Outcome::Error((status, _error)) => Outcome::Error(status),
                Outcome::Forward(_) => Outcome::Error(Status::Unauthorized),
            }
        })
    }

    #[test]
    fn guard_error_mapping_distinguishes_infrastructure_failures() {
        assert_eq!(
            status_for_error(&Error::ServiceUnavailable),
            Status::ServiceUnavailable
        );
        assert_eq!(
            status_for_error(&Error::InvalidSession),
            Status::Unauthorized
        );
        assert_eq!(
            status_for_error(&Error::DatabaseError {
                operation: "find",
                with: "session",
            }),
            Status::InternalServerError
        );
    }

    #[test]
    fn service_unavailable_response_has_retry_after() {
        let client = Client::tracked(
            rocket::build().mount("/", vec![Route::new(Method::Get, "/fail", unavailable)]),
        )
        .unwrap();

        let response = client.get("/fail").dispatch();
        assert_eq!(response.status(), Status::ServiceUnavailable);
        assert_eq!(response.headers().get_one("Retry-After"), Some("1"));
    }

    #[test]
    fn session_guard_without_token_returns_401() {
        let client = Client::tracked(
            rocket::build()
                .manage(Authifier::default())
                .mount("/", vec![Route::new(Method::Get, "/guarded", guarded)]),
        )
        .unwrap();

        let response = client.get("/guarded").dispatch();
        assert_eq!(response.status(), Status::Unauthorized);
    }

    #[cfg(feature = "database-mongodb")]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    async fn session_guard_database_error_returns_500() {
        let mut options = mongodb::options::ClientOptions::parse("mongodb://127.0.0.1:1")
            .await
            .unwrap();
        options.server_selection_timeout = Some(std::time::Duration::from_millis(100));
        let mongo = mongodb::Client::with_options(options).unwrap();

        let authifier = Authifier {
            database: crate::database::Database::MongoDb(crate::database::MongoDb(
                mongo.database("guard_error_test"),
            )),
            config: Default::default(),
            event_channel: None,
        };

        let client = AsyncClient::tracked(
            rocket::build()
                .manage(authifier)
                .mount("/", vec![Route::new(Method::Get, "/guarded", guarded)]),
        )
        .await
        .unwrap();

        let response = client
            .get("/guarded")
            .header(Header::new("x-session-token", "token"))
            .dispatch()
            .await;
        assert_eq!(response.status(), Status::InternalServerError);
    }
}
