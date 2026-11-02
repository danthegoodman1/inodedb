//! The server's TOML configuration.

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    /// Where clients reach this server, which standbys name when they send
    /// a request on to the writer; `http://` and the listening address
    /// when unset.
    pub advertise: Option<String>,
    pub storage: StorageConfig,
    #[serde(default)]
    pub writer: WriterConfig,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub cache: CacheConfig,
}

/// What the writer's cache, and a standby's, may hold.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CacheConfig {
    /// Directories with entries cached.
    pub dirs: usize,
    /// Inode records, with each file's links.
    pub inodes: usize,
    /// Bytes of values.
    pub value_bytes: usize,
    /// Whether the writer reads the tree into its cache when it starts, up
    /// to the budget.
    pub preload: bool,
}

impl Default for CacheConfig {
    fn default() -> CacheConfig {
        let budget = inodedb_core::cache::Budget::default();
        CacheConfig {
            dirs: budget.dirs,
            inodes: budget.inodes,
            value_bytes: budget.value_bytes,
            preload: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// Every log entry stays at least this long.
    pub min_retention_ms: u64,
    /// An entry a live lease's position needs stays up to this long.
    pub max_retention_ms: u64,
}

impl Default for LogConfig {
    fn default() -> LogConfig {
        let retention = crate::feed::Retention::default();
        LogConfig {
            min_retention_ms: retention.min.as_millis() as u64,
            max_retention_ms: retention.max.as_millis() as u64,
        }
    }
}

impl LogConfig {
    pub fn retention(&self) -> crate::feed::Retention {
        crate::feed::Retention {
            min: std::time::Duration::from_millis(self.min_retention_ms),
            max: std::time::Duration::from_millis(self.max_retention_ms),
            ..crate::feed::Retention::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "backend", rename_all = "lowercase")]
pub enum StorageConfig {
    Memory,
    Sqlite {
        path: PathBuf,
    },
    Rocksdb {
        path: PathBuf,
    },
    /// A store under `prefix` in the cluster `cluster_file` names, or the
    /// default cluster file's.
    Fdb {
        cluster_file: Option<PathBuf>,
        prefix: String,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WriterConfig {
    pub group_max_bytes: usize,
    pub request_ttl_ms: u64,
    /// How long the writer answers reads after its last commit; a standby
    /// takes over once the head goes unchanged this long plus the margin.
    pub lease_ms: u64,
    /// How often an idle writer commits to renew its lease.
    pub renew_every_ms: u64,
    /// What a standby waits beyond the lease, covering clock drift.
    pub takeover_margin_ms: u64,
    /// How long a reader's lease lasts past its last heartbeat.
    pub lease_ttl_ms: u64,
    /// How long a consumer's claim lasts past its last acknowledgment.
    pub claim_ttl_ms: u64,
}

impl Default for WriterConfig {
    fn default() -> WriterConfig {
        let core = inodedb_core::writer::Config::default();
        WriterConfig {
            group_max_bytes: core.group_max_bytes,
            request_ttl_ms: core.request_ttl,
            lease_ms: core.writer_lease,
            renew_every_ms: core.renew_every,
            takeover_margin_ms: core.takeover_margin,
            lease_ttl_ms: core.lease_ttl,
            claim_ttl_ms: core.claim_ttl,
        }
    }
}

impl Config {
    /// The writer's configuration, which a standby's follower shares.
    pub fn core(&self) -> inodedb_core::writer::Config {
        let writer = &self.writer;
        inodedb_core::writer::Config {
            group_max_bytes: writer.group_max_bytes,
            request_ttl: writer.request_ttl_ms,
            writer_lease: writer.lease_ms,
            renew_every: writer.renew_every_ms,
            takeover_margin: writer.takeover_margin_ms,
            lease_ttl: writer.lease_ttl_ms,
            claim_ttl: writer.claim_ttl_ms,
            cache: inodedb_core::cache::Budget {
                dirs: self.cache.dirs,
                inodes: self.cache.inodes,
                value_bytes: self.cache.value_bytes,
                ..inodedb_core::cache::Budget::default()
            },
            preload: self.cache.preload,
            ..inodedb_core::writer::Config::default()
        }
    }
}

/// The shortest request-record lifetime: well past the client's retry
/// deadline, so a retry always finds its first answer.
pub const MIN_REQUEST_TTL_MS: u64 = 5 * 60 * 1000;

impl Config {
    pub fn parse(text: &str) -> Result<Config, String> {
        let config: Config = toml::from_str(text).map_err(|error| error.to_string())?;
        config.check()?;
        Ok(config)
    }

    fn check(&self) -> Result<(), String> {
        if self.writer.request_ttl_ms < MIN_REQUEST_TTL_MS {
            return Err(format!(
                "writer.request_ttl_ms must be at least {MIN_REQUEST_TTL_MS}, so retries find their records"
            ));
        }
        if self.writer.group_max_bytes > inodedb_core::writer::MAX_GROUP_BYTES {
            return Err(format!(
                "writer.group_max_bytes must be at most {}",
                inodedb_core::writer::MAX_GROUP_BYTES
            ));
        }
        if self.writer.renew_every_ms == 0 || self.writer.renew_every_ms * 2 > self.writer.lease_ms
        {
            return Err(
                "writer.renew_every_ms must be positive and at most half of writer.lease_ms".into(),
            );
        }
        if self.writer.lease_ttl_ms < 1_000 || self.writer.claim_ttl_ms < 1_000 {
            return Err("writer.lease_ttl_ms and claim_ttl_ms must be at least a second".into());
        }
        if self.writer.takeover_margin_ms * 10 < self.writer.lease_ms {
            return Err(
                "writer.takeover_margin_ms must be at least a tenth of writer.lease_ms, to cover clocks that drift apart".into(),
            );
        }
        Ok(())
    }

    /// Where clients reach this server.
    pub fn advertised(&self) -> String {
        self.advertise
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.listen))
    }

    pub fn load(path: &std::path::Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        Config::parse(&text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_minimal_config() {
        let config = Config::parse(
            r#"
            listen = "127.0.0.1:7070"
            [storage]
            backend = "sqlite"
            path = "data/db.sqlite"
            "#,
        )
        .expect("parses");
        assert!(matches!(config.storage, StorageConfig::Sqlite { .. }));
        assert_eq!(config.writer.group_max_bytes, 9_000_000);
    }

    /// The defaults DESIGN.md's table gives, with the measurements behind
    /// them.
    #[test]
    fn defaults_match_the_design() {
        let config = Config::parse("listen = \"127.0.0.1:7070\"\n[storage]\nbackend = \"memory\"")
            .expect("parses");
        let core = config.core();
        assert_eq!(
            (core.writer_lease, core.renew_every, core.takeover_margin),
            (5_000, 1_000, 1_000)
        );
        assert_eq!((core.lease_ttl, core.claim_ttl), (30_000, 60_000));
        assert_eq!(core.request_ttl, 10 * 60 * 1000);
        assert_eq!(core.group_max_bytes, 9_000_000);
        let retention = config.log.retention();
        assert_eq!(
            (retention.min.as_secs(), retention.max.as_secs()),
            (5 * 60, 60 * 60)
        );
        assert_eq!(
            (
                core.cache.dirs,
                core.cache.inodes,
                core.cache.value_bytes,
                core.cache.undo_bytes
            ),
            (1 << 18, 1 << 20, 256 << 20, 64 << 20)
        );
        assert!(core.preload);
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(
            Config::parse(
                r#"
                listen = "127.0.0.1:7070"
                    typo = 1
                [storage]
                backend = "memory"
                "#
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unsafe_values() {
        let parse = |writer: &str| {
            Config::parse(&format!(
                "listen = \"127.0.0.1:7070\"\n[storage]\nbackend = \"memory\"\n[writer]\n{writer}"
            ))
        };
        assert!(parse("").is_ok());
        assert!(parse("request_ttl_ms = 1000").is_err());
        assert!(parse("group_max_bytes = 20000000").is_err());
        assert!(parse("renew_every_ms = 3000").is_err());
        assert!(parse("takeover_margin_ms = 0").is_err());
        assert!(parse("lease_ttl_ms = 0").is_err());
        assert!(parse("lease_ms = 10000\ntakeover_margin_ms = 1000").is_ok());
    }
}
