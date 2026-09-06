use super::{LocalDirRemoteFs, RemoteFs};

#[tokio::test]
async fn uncached_read_uses_origin_not_stale_local_bytes() {
    let origin = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let fs = LocalDirRemoteFs::new(
        Some(origin.path().to_path_buf()),
        local.path().to_path_buf(),
    );
    tokio::fs::write(origin.path().join("object"), b"original")
        .await
        .unwrap();
    let cached = fs.download_file("object".into(), None).await.unwrap();
    tokio::fs::write(origin.path().join("object"), b"updated")
        .await
        .unwrap();
    let fresh = fs
        .download_file_uncached("object".into(), Some(7))
        .await
        .unwrap();
    assert_ne!(cached, fresh);
    assert_eq!(tokio::fs::read(&cached).await.unwrap(), b"original");
    assert_eq!(tokio::fs::read(&fresh).await.unwrap(), b"updated");
    tokio::fs::remove_file(fresh).await.unwrap();
    tokio::fs::remove_file(origin.path().join("object"))
        .await
        .unwrap();
    assert!(fs
        .download_file_uncached("object".into(), None)
        .await
        .is_err());
}

#[tokio::test]
async fn uncached_read_rejects_wrong_size_and_missing_origin() {
    let origin = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let fs = LocalDirRemoteFs::new(
        Some(origin.path().to_path_buf()),
        local.path().to_path_buf(),
    );
    tokio::fs::write(origin.path().join("object"), b"bytes")
        .await
        .unwrap();
    assert!(fs
        .download_file_uncached("object".into(), Some(10))
        .await
        .is_err());
    let noop = LocalDirRemoteFs::new_noop(local.path().to_path_buf());
    assert!(noop
        .download_file_uncached("object".into(), None)
        .await
        .is_err());
}
