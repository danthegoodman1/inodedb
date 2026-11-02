//! `inodedb --init-key` gives a store with no keys its first admin key.

use inodedb_proto as pb;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use tokio::process::{Child, Command};

fn serve(config: &Path, key_file: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_inodedb"))
        .arg("--config")
        .arg(config)
        .arg("--init-key")
        .arg(key_file)
        .kill_on_drop(true)
        .spawn()
        .expect("the server starts")
}

/// The root's version, read with `key`, once the server answers.
async fn stat_root(port: u16, key: &str) -> Result<u64, tonic::Status> {
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..500 {
        if let Ok(mut rpc) = pb::inode_db_client::InodeDbClient::connect(endpoint.clone()).await {
            let mut request = tonic::Request::new(pb::StatRequest {
                targets: vec![pb::Target {
                    kind: Some(pb::target::Kind::Path(Default::default())),
                }],
                at: None,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {key}").parse().expect("a header"),
            );
            return rpc
                .stat(request)
                .await
                .map(|response| response.into_inner().version);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the server never answered");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_first_key_comes_from_the_key_file() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port();
    let config = dir.path().join("inodedb.toml");
    std::fs::write(
        &config,
        format!(
            "listen = \"127.0.0.1:{port}\"\n[storage]\nbackend = \"sqlite\"\npath = {:?}\n",
            dir.path().join("db.sqlite")
        ),
    )
    .expect("writes the config");
    let key_file = dir.path().join("admin.key");
    let mut server = serve(&config, &key_file);
    let key = loop {
        match std::fs::read_to_string(&key_file) {
            Ok(key) if key.ends_with('\n') => break key.trim().to_string(),
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    let mode = std::fs::metadata(&key_file)
        .expect("the key file")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "only its owner reads the key");
    stat_root(port, &key).await.expect("the key works");
    server.kill().await.expect("stops");
    // The file's key works again, and the store keeps its keys: a new
    // file's key goes unminted.
    let mut server = serve(&config, &key_file);
    stat_root(port, &key).await.expect("the key still works");
    server.kill().await.expect("stops");
    let other_file = dir.path().join("other.key");
    let mut server = serve(&config, &other_file);
    stat_root(port, &key)
        .await
        .expect("the first key still works");
    let other = std::fs::read_to_string(&other_file).expect("a new key file");
    let refused = stat_root(port, other.trim())
        .await
        .expect_err("the new key is refused");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
    server.kill().await.expect("stops");
    // A revoked first key stays revoked though the file still holds it.
    let mut server = serve(&config, &key_file);
    stat_root(port, &key).await.expect("the key works");
    revoke(port, &key).await;
    server.kill().await.expect("stops");
    let _server = serve(&config, &key_file);
    let refused = stat_root(port, &key)
        .await
        .expect_err("the revoked key is refused");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
}

/// The binary serves a lionturtle store from its config, and finds what it
/// committed after a kill.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_keeps_a_lionturtle_store() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port();
    let config = dir.path().join("inodedb.toml");
    std::fs::write(
        &config,
        format!(
            "listen = \"127.0.0.1:{port}\"\n[storage]\nbackend = \"lionturtle\"\npath = {:?}\n",
            dir.path().join("db")
        ),
    )
    .expect("writes the config");
    let key_file = dir.path().join("admin.key");
    let mut server = serve(&config, &key_file);
    let key = loop {
        match std::fs::read_to_string(&key_file) {
            Ok(key) if key.ends_with('\n') => break key.trim().to_string(),
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    let before = stat_root(port, &key).await.expect("the key works");
    server.kill().await.expect("stops");
    server.wait().await.expect("exits");
    let mut server = serve(&config, &key_file);
    let after = stat_root(port, &key).await.expect("the store kept the key");
    assert!(after >= before, "version {after} after {before}");
    server.kill().await.expect("stops");
}

async fn revoke(port: u16, key: &str) {
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .expect("connects");
    let mut request = tonic::Request::new(pb::RevokeKeyRequest {
        id: key.to_string(),
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {key}").parse().expect("a header"),
    );
    rpc.revoke_key(request).await.expect("revokes itself");
}
