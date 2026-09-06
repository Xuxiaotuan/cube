//! The content-addressed object is the durable upload record; no local success ledger.
use crate::remotefs::RemoteFs;
use crate::sql::ha::MutationGuard;
use crate::CubeError;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::io::AsyncReadExt;

#[derive(Debug, Serialize)]
pub struct UploadStatus {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

pub fn validate_name(name: &str, sha256: &str) -> Result<(), CubeError> {
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(CubeError::user(
            "sha256 must be 64 lowercase hexadecimal characters".into(),
        ));
    }
    if name != format!("{sha256}.csv") && name != format!("{sha256}.csv.gz") {
        return Err(CubeError::user(
            "Upload name must be <sha256>.csv or <sha256>.csv.gz".into(),
        ));
    }
    Ok(())
}

pub async fn checksum(path: &Path) -> Result<(String, u64), CubeError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut size = 0;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
        size += n as u64;
    }
    Ok((hex::encode(hasher.finalize()), size))
}

pub async fn status(
    remote: &dyn RemoteFs,
    name: &str,
    sha256: &str,
) -> Result<UploadStatus, CubeError> {
    validate_name(name, sha256)?;
    let key = format!("temp-uploads/{name}");
    let objects = remote.list_with_metadata(key.clone()).await?;
    let object = match objects.iter().find(|object| object.remote_path == key) {
        Some(object) => object,
        None => {
            return Ok(UploadStatus {
                state: "missing",
                sha256: None,
                size: None,
            })
        }
    };
    // This must bypass the cache: a locally complete upload is not remote durability.
    let path = remote
        .download_file_uncached(key, Some(object.file_size))
        .await?;
    // Own the fresh temporary file so cancellation also cleans it up.
    let path = tempfile::TempPath::from_path(path);
    let (actual, size) = checksum(path.as_ref()).await?;
    if actual != sha256 || size != object.file_size {
        return Err(CubeError::internal(
            "Remote upload checksum/size mismatch; object retained for reconciliation".into(),
        ));
    }
    Ok(UploadStatus {
        state: "uploaded",
        sha256: Some(actual),
        size: Some(size),
    })
}

pub async fn publish(
    remote: &dyn RemoteFs,
    guard: &MutationGuard,
    name: &str,
    sha256: &str,
    path: &Path,
) -> Result<UploadStatus, CubeError> {
    validate_name(name, sha256)?;
    let (actual, _) = checksum(path).await?;
    if actual != sha256 {
        return Err(CubeError::user("Upload body does not match sha256".into()));
    }
    guard.check()?;
    let existing = status(remote, name, sha256).await?;
    if existing.state == "uploaded" {
        return Ok(existing);
    }
    // A changed lease must not publish bytes accepted under the old lease.
    guard.check()?;
    remote
        .upload_file(
            path.to_string_lossy().into_owned(),
            format!("temp-uploads/{name}"),
        )
        .await?;
    guard.check()?;
    // Preserve the remote object even on an ambiguous result; a retry can reconcile it.
    status(remote, name, sha256).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_names_reject_traversal_wrong_digest_and_unsupported_suffixes() {
        let hash = "a".repeat(64);
        assert!(validate_name(&format!("{hash}.csv"), &hash).is_ok());
        assert!(validate_name(&format!("{hash}.csv.gz"), &hash).is_ok());
        for name in [
            format!("../{hash}.csv"),
            "payload.csv".into(),
            format!("{hash}.gz"),
            format!("{}.csv", "b".repeat(64)),
        ] {
            assert!(validate_name(&name, &hash).is_err());
        }
        assert!(validate_name("x.csv", "123").is_err());
        assert!(validate_name(&format!("{}.csv", "A".repeat(64)), &"A".repeat(64)).is_err());
    }

    #[tokio::test]
    async fn checksum_uses_exact_bytes_including_gzip_header() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), b"abc").await.unwrap();
        let (hash, size) = checksum(file.path()).await.unwrap();
        assert_eq!(
            hash,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(size, 3);
        tokio::fs::write(file.path(), b"\x1f\x8babc").await.unwrap();
        assert_ne!(checksum(file.path()).await.unwrap().0, hash);
    }

    #[tokio::test]
    async fn durable_status_rechecks_origin_even_with_correct_local_cache() {
        let origin = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let remote =
            crate::remotefs::LocalDirRemoteFs::new(Some(origin.path().into()), cache.path().into());
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let name = format!("{hash}.csv");
        let key = format!("temp-uploads/{name}");
        tokio::fs::create_dir_all(origin.path().join("temp-uploads"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(cache.path().join("temp-uploads"))
            .await
            .unwrap();
        tokio::fs::write(cache.path().join(&key), b"abc")
            .await
            .unwrap();
        tokio::fs::write(origin.path().join(&key), b"bad")
            .await
            .unwrap();
        assert!(status(remote.as_ref(), &name, hash).await.is_err());
        assert_eq!(
            tokio::fs::read(origin.path().join(&key)).await.unwrap(),
            b"bad"
        );
        tokio::fs::write(origin.path().join(&key), b"abc")
            .await
            .unwrap();
        let result = status(remote.as_ref(), &name, hash).await.unwrap();
        assert_eq!(result.state, "uploaded");
        assert_eq!(result.size, Some(3));
        tokio::fs::remove_file(origin.path().join(&key))
            .await
            .unwrap();
        assert_eq!(
            status(remote.as_ref(), &name, hash).await.unwrap().state,
            "missing"
        );
    }

    #[tokio::test]
    async fn retry_reconciles_durable_object_without_overwriting_conflicts() {
        use crate::sql::ha::MutationGate;
        use std::sync::Arc;
        let origin = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let remote =
            crate::remotefs::LocalDirRemoteFs::new(Some(origin.path().into()), cache.path().into());
        let gate = MutationGate::with_checker(true, Arc::new(|| Ok("epoch-1".into())));
        let guard = gate.begin().unwrap();
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let name = format!("{hash}.csv");
        let key = format!("temp-uploads/{name}");
        tokio::fs::create_dir_all(origin.path().join("temp-uploads"))
            .await
            .unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), b"abc").await.unwrap();
        assert_eq!(
            publish(remote.as_ref(), &guard, &name, hash, file.path())
                .await
                .unwrap()
                .state,
            "uploaded"
        );
        // Reconnect with a fresh cache: the receipt must survive router-local state loss.
        let fresh_cache = tempfile::tempdir().unwrap();
        let restarted = crate::remotefs::LocalDirRemoteFs::new(
            Some(origin.path().into()),
            fresh_cache.path().into(),
        );
        tokio::fs::write(file.path(), b"abc").await.unwrap();
        assert_eq!(
            publish(restarted.as_ref(), &guard, &name, hash, file.path())
                .await
                .unwrap()
                .state,
            "uploaded"
        );
        tokio::fs::write(origin.path().join(&key), b"bad")
            .await
            .unwrap();
        assert!(
            publish(restarted.as_ref(), &guard, &name, hash, file.path())
                .await
                .is_err()
        );
        assert_eq!(
            tokio::fs::read(origin.path().join(&key)).await.unwrap(),
            b"bad"
        );
    }

    #[tokio::test]
    async fn stale_upload_guard_cannot_publish_after_body_arrives() {
        use crate::sql::ha::MutationGate;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let origin = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let remote =
            crate::remotefs::LocalDirRemoteFs::new(Some(origin.path().into()), cache.path().into());
        let epoch = Arc::new(AtomicUsize::new(1));
        let value = epoch.clone();
        let gate = MutationGate::with_checker(
            true,
            Arc::new(move || Ok(value.load(Ordering::SeqCst).to_string())),
        );
        let guard = gate.begin().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), b"abc").await.unwrap();
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let name = format!("{hash}.csv");
        epoch.store(2, Ordering::SeqCst);
        assert!(publish(remote.as_ref(), &guard, &name, hash, file.path())
            .await
            .is_err());
        assert!(!origin.path().join(format!("temp-uploads/{name}")).exists());
    }
}
