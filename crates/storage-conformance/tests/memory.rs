use inodedb_storage_conformance::backends::Mem;
use inodedb_storage_conformance::conformance;

conformance!(Mem);

mod faults {
    use inodedb_core::keys::{self, KvWrite, WriteSet};
    use inodedb_core::writer::CommitOutcome;
    use inodedb_storage::memory::Fault;
    use inodedb_storage::{Memory, StorageError, commit, resolve};

    fn head(value: &[u8]) -> WriteSet {
        let mut writes = WriteSet::default();
        writes
            .writes
            .push(KvWrite::Set(keys::head(), value.to_vec()));
        writes
    }

    #[tokio::test]
    async fn an_unknown_commit_that_applied_resolves_as_committed() {
        let store = Memory::new();
        store.inject(Fault::UnknownApplied);
        assert_eq!(
            commit(&store, None, &head(b"h1")).await,
            CommitOutcome::Unknown
        );
        assert_eq!(resolve(&store).await, Ok(Some(b"h1".to_vec())));
    }

    #[tokio::test]
    async fn an_unknown_commit_that_never_applied_resolves_as_failed() {
        let store = Memory::new();
        assert_eq!(
            commit(&store, None, &head(b"h1")).await,
            CommitOutcome::Committed
        );
        store.inject(Fault::UnknownDropped);
        assert_eq!(
            commit(&store, Some(b"h1"), &head(b"h2")).await,
            CommitOutcome::Unknown
        );
        assert_eq!(resolve(&store).await, Ok(Some(b"h1".to_vec())));
    }

    #[tokio::test]
    async fn an_unavailable_commit_fails() {
        let store = Memory::new();
        store.inject(Fault::Unavailable);
        assert_eq!(
            commit(&store, None, &head(b"h1")).await,
            CommitOutcome::Failed
        );
        assert_eq!(resolve(&store).await, Ok::<_, StorageError>(None));
    }
}
