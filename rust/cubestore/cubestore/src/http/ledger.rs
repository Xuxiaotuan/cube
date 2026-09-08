//! Router-facing bridge. The Driver never receives MetaStore authority credentials.
use super::CubeRejection;
use crate::metastore::pre_aggregation_ledger::LedgerRequest;
use crate::mysql::SqlAuthService;
use crate::sql::SqlService;
use http_auth_basic::Credentials;
use serde::Deserialize;
use std::sync::Arc;
use warp::{Filter, Rejection, Reply};

#[derive(Deserialize)]
struct Lookup {
    key: String,
    generation: Option<String>,
}

fn authentication(
    auth: Arc<dyn SqlAuthService>,
) -> impl Filter<Extract = ((),), Error = Rejection> + Clone {
    warp::header::optional::<String>("authorization").and_then(move |header: Option<String>| {
        let auth = auth.clone();
        async move {
            let unauthorized = || warp::reject::custom(CubeRejection::NotAuthorized);
            let credentials = Credentials::from_header(header.ok_or_else(unauthorized)?)
                .map_err(|_| unauthorized())?;
            let password = auth.authenticate(Some(credentials.user_id.clone())).await
                .map_err(|_| unauthorized())?;
            // Stock passwordless SQL authentication is not sufficient for this endpoint.
            match password {
                Some(password) if !password.is_empty() && password == credentials.password => Ok(()),
                _ => Err(unauthorized()),
            }
        }
    })
}

pub(super) fn routes(
    auth: Arc<dyn SqlAuthService>,
    service: Arc<dyn SqlService>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let authenticated = authentication(auth);
    let writer = service.clone();
    let mutation = warp::path!("router" / "pre-aggregation-ledger")
        .and(warp::post())
        .and(authenticated.clone())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::body::json::<LedgerRequest>())
        .and_then(move |(): (), request: LedgerRequest| {
            let service = writer.clone();
            async move {
                let result = service.mutate_pre_aggregation_ledger(request).await?;
                Ok::<_, Rejection>(warp::reply::json(&result))
            }
        });
    let lookup = warp::path!("router" / "pre-aggregation-ledger")
        .and(warp::get())
        .and(authenticated)
        .and(warp::query::<Lookup>())
        .and_then(move |(): (), query: Lookup| {
            let service = service.clone();
            async move {
                let result = service.read_pre_aggregation_ledger(query.key, query.generation).await?;
                Ok::<_, Rejection>(warp::reply::json(&result))
            }
        });
    mutation.or(lookup)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CubeError;

    struct Auth(Option<String>);
    #[async_trait::async_trait]
    impl SqlAuthService for Auth {
        async fn authenticate(&self, _: Option<String>) -> Result<Option<String>, CubeError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn ledger_auth_rejects_passwordless_and_missing_credentials() {
        let passwordless = authentication(Arc::new(Auth(None)));
        assert!(warp::test::request().header("authorization", "Basic dTpw")
            .filter(&passwordless).await.is_err());
        let protected = authentication(Arc::new(Auth(Some("p".to_string()))));
        assert!(warp::test::request().filter(&protected).await.is_err());
        assert!(warp::test::request().header("authorization", "Basic dTpiYWQ=")
            .filter(&protected).await.is_err());
        assert!(warp::test::request().header("authorization", "Basic dTpw")
            .filter(&protected).await.is_ok());
    }
}
