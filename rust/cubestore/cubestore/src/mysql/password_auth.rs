use crate::CubeError;
use std::env::VarError;

pub(super) fn configured_password() -> Result<Option<String>, CubeError> {
    parse_password(std::env::var("CUBESTORE_SQL_PASSWORD"))
}

fn parse_password(value: Result<String, VarError>) -> Result<Option<String>, CubeError> {
    match value {
        Ok(password) if !password.is_empty() => Ok(Some(password)),
        Err(VarError::NotPresent) => Ok(None),
        // An explicitly configured but unusable secret must not enable passwordless access.
        _ => Err(CubeError::internal(
            "CUBESTORE_SQL_PASSWORD must be a non-empty UTF-8 value when configured".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_password_preserves_legacy_authentication() {
        assert_eq!(parse_password(Err(VarError::NotPresent)).unwrap(), None);
    }

    #[test]
    fn configured_password_is_preserved_exactly() {
        assert_eq!(parse_password(Ok(" test password ".to_string())).unwrap(), Some(" test password ".to_string()));
    }

    #[test]
    fn unusable_secret_does_not_enable_passwordless_access() {
        assert!(parse_password(Ok(String::new())).is_err());
        assert!(parse_password(Err(VarError::NotUnicode(std::ffi::OsString::from("invalid")))).is_err());
    }
}
