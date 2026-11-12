//! Scripted scenarios for each op's rules, conditions and cascades.

use inodedb_core::Bytes;
use inodedb_core::ops::{Batch, Op, OpSpec, Outcome, Pred, PredSource, PutMode, RenameFlag, Skip};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Error, Kind, Life, MAX_VALUE};
use inodedb_sim::harness::*;

fn applied(result: &inodedb_core::ops::CommitResult) -> Vec<bool> {
    result.outcomes.iter().map(Outcome::applied).collect()
}

fn error(outcome: &Outcome) -> Option<Error> {
    match outcome {
        Outcome::Skipped(Skip::Error(error)) => Some(*error),
        _ => None,
    }
}

fn errors(result: &inodedb_core::ops::CommitResult) -> Vec<Option<Error>> {
    result.outcomes.iter().map(error).collect()
}

fn nlink(harness: &mut Harness, path: &str) -> u32 {
    match harness.stat(path) {
        QueryResult::Stat { inode, .. } => inode.nlink,
        other => panic!("{path}: {other:?}"),
    }
}

#[test]
fn mkdir_reports_each_error() {
    let mut h = Harness::new();
    h.ops([mkdir("a"), put("f", "x")]);
    let result = h.ops([
        mkdir("a"),
        mkdir("x/y"),
        mkdir("f/y"),
        mkdir("f"),
        mkdir(""),
    ]);
    assert_eq!(
        errors(&result),
        [
            Some(Error::Exists),
            Some(Error::NotFound),
            Some(Error::NotDir),
            Some(Error::Exists),
            Some(Error::Exists)
        ]
    );
    assert!(!result.committed, "nothing changed");
    let result = h.ops([mkdir_p("x/y/z"), mkdir_p("a")]);
    assert_eq!(applied(&result), [true, true]);
    assert!(matches!(h.stat("x/y/z"), QueryResult::Stat { inode, .. } if inode.kind == Kind::Dir));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn put_creates_replaces_and_frees_the_old_value() {
    let mut h = Harness::new();
    let first = h.ops([put("f", "one")]);
    let ino = match first.outcomes[0] {
        Outcome::Applied { ino: Some(ino) } => ino,
        other => panic!("{other:?}"),
    };
    let result = h.ops([
        put_mode("f", "x", PutMode::Create),
        put_mode("g", "x", PutMode::Replace),
        put("", "x"),
        OpSpec::new(Op::Put {
            path: p("big"),
            value: Bytes::from(vec![0; MAX_VALUE + 1]),
            mode: PutMode::Upsert,
            mtime: None,
        }),
        put_mode("f", "two", PutMode::Replace),
    ]);
    assert_eq!(
        errors(&result),
        [
            Some(Error::Exists),
            Some(Error::NotFound),
            Some(Error::IsDir),
            Some(Error::TooLarge),
            None
        ]
    );
    assert_eq!(result.outcomes[4], Outcome::Applied { ino: Some(ino) });
    let (_, read) = h.query(Query::Read {
        target: Target::Path(p("f")),
        version: None,
    });
    assert!(matches!(read, QueryResult::Read { value, .. } if value == "two"));
    // The first value stays readable by version until its queue entry goes.
    let (_, old) = h.query(Query::Read {
        target: Target::Ino(ino),
        version: Some(first.version),
    });
    assert!(matches!(old, QueryResult::Read { value, .. } if value == "one"));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn hardlinks_share_a_file_and_count_links() {
    let mut h = Harness::new();
    h.ops([mkdir("d"), put("d/f", "v"), link("d/f", "g")]);
    assert_eq!(nlink(&mut h, "g"), 2);
    let result = h.ops([
        link("d", "x"),
        link("d/f", "g"),
        link("missing", "x"),
        link("d/f", ""),
    ]);
    assert_eq!(
        errors(&result),
        [
            Some(Error::IsDir),
            Some(Error::Exists),
            Some(Error::NotFound),
            Some(Error::Exists)
        ]
    );
    h.ops([unlink("d/f")]);
    assert_eq!(nlink(&mut h, "g"), 1);
    h.ops([put("g", "w")]);
    let result = h.ops([unlink("d"), unlink("missing"), unlink("")]);
    assert_eq!(
        errors(&result),
        [
            Some(Error::IsDir),
            Some(Error::NotFound),
            Some(Error::Invalid)
        ]
    );
    let gone = h.ops([OpSpec::new(Op::Unlink {
        path: p("missing"),
        if_exists: true,
    })]);
    assert_eq!(gone.outcomes, [Outcome::Applied { ino: None }]);
    assert!(!gone.committed);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn rmdir_needs_an_empty_directory() {
    let mut h = Harness::new();
    h.ops([mkdir("d"), put("d/f", "v"), put("f", "v")]);
    let result = h.ops([rmdir("d"), rmdir("f"), rmdir("missing"), rmdir("")]);
    assert_eq!(
        errors(&result),
        [
            Some(Error::NotEmpty),
            Some(Error::NotDir),
            Some(Error::NotFound),
            Some(Error::Invalid)
        ]
    );
    let result = h.ops([unlink("d/f"), rmdir("d")]);
    assert_eq!(applied(&result), [true, true]);
    assert!(matches!(h.stat("d"), QueryResult::Failed(Error::NotFound)));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn rename_follows_rename_2() {
    let mut h = Harness::new();
    h.ops([
        mkdir("a"),
        mkdir("a/b"),
        mkdir("empty"),
        mkdir("full"),
        put("full/x", "1"),
        put("f", "f"),
        put("g", "g"),
    ]);
    let result = h.ops([
        mv("a", "a/b/c"),
        mv("a", "f"),
        mv("f", "a"),
        mv("a", "full"),
        mv("missing", "z"),
        mv("", "z"),
        rename("f", "g", RenameFlag::NoReplace),
        rename("f", "z", RenameFlag::Exchange),
    ]);
    assert_eq!(
        errors(&result),
        [
            Some(Error::Invalid),
            Some(Error::NotDir),
            Some(Error::IsDir),
            Some(Error::NotEmpty),
            Some(Error::NotFound),
            Some(Error::Invalid),
            Some(Error::Exists),
            Some(Error::NotFound),
        ]
    );
    // A file replaces a file, and a directory an empty directory.
    let result = h.ops([mv("f", "g"), mv("a", "empty")]);
    assert_eq!(applied(&result), [true, true]);
    let (_, read) = h.query(Query::Read {
        target: Target::Path(p("g")),
        version: None,
    });
    assert!(matches!(read, QueryResult::Read { value, .. } if value == "f"));
    assert!(matches!(h.stat("empty/b"), QueryResult::Stat { .. }));
    assert!(matches!(h.stat("a"), QueryResult::Failed(Error::NotFound)));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn renames_onto_the_same_file_change_nothing() {
    let mut h = Harness::new();
    h.ops([put("f", "v"), link("f", "g")]);
    let result = h.ops([
        mv("f", "f"),
        mv("f", "g"),
        rename("f", "g", RenameFlag::Exchange),
    ]);
    assert_eq!(applied(&result), [true, true, true]);
    assert!(!result.committed);
    assert_eq!(nlink(&mut h, "f"), 2);
}

#[test]
fn exchange_checks_cycles_both_ways() {
    let mut h = Harness::new();
    h.ops([mkdir("a"), mkdir("a/b"), mkdir("a/b/c"), put("f", "1")]);
    let result = h.ops([
        rename("a", "a/b/c", RenameFlag::Exchange),
        rename("a/b/c", "a", RenameFlag::Exchange),
    ]);
    assert_eq!(
        errors(&result),
        [Some(Error::Invalid), Some(Error::Invalid)]
    );
    let result = h.ops([rename("a/b", "f", RenameFlag::Exchange)]);
    assert!(result.committed);
    assert!(matches!(h.stat("f/c"), QueryResult::Stat { .. }));
    assert!(matches!(h.stat("a/b"), QueryResult::Stat { inode, .. } if inode.kind == Kind::File));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn renaming_a_directory_moves_everything_under_it() {
    let mut h = Harness::new();
    h.ops([
        mkdir_p("t/tmp_insert_1"),
        put("t/tmp_insert_1/data.bin", "d"),
        put("t/tmp_insert_1/checksums.txt", "c"),
    ]);
    let result = h.ops([rename(
        "t/tmp_insert_1",
        "t/all_1_1_0",
        RenameFlag::NoReplace,
    )]);
    assert!(result.committed);
    let (_, listing) = h.query(Query::List {
        target: Target::Path(p("t/all_1_1_0")),
        after: None,
        prefix: None,
        limit: 10,
        budget: usize::MAX,
        attrs: true,
        values: true,
    });
    let QueryResult::List { entries, more, .. } = listing else {
        panic!("{listing:?}");
    };
    assert!(!more);
    let names: Vec<&[u8]> = entries.iter().map(|listed| &*listed.name).collect();
    assert_eq!(names, [&b"checksums.txt"[..], b"data.bin"]);
    assert_eq!(entries[1].value.as_deref(), Some(&b"d"[..]));
}

#[test]
fn skips_cascade_through_state_and_applied() {
    let mut h = Harness::new();
    h.ops([put("x", "file")]);
    let result = h.ops([
        mkdir("x"),
        put("x/f", "v"),
        mkdir("d").when(Pred::Absent(p("d"))),
        put("d/f", "v").when(Pred::Applied(2)),
        put("e/f", "v").when(Pred::Applied(0)),
    ]);
    assert_eq!(
        result.outcomes,
        [
            Outcome::Skipped(Skip::Error(Error::Exists)),
            Outcome::Skipped(Skip::Error(Error::NotDir)),
            result.outcomes[2],
            result.outcomes[3],
            Outcome::Skipped(Skip::Pred(PredSource::Cond(0))),
        ]
    );
    assert_eq!(applied(&result)[2..4], [true, true]);
}

#[test]
fn a_failed_when_applies_nothing() {
    let mut h = Harness::new();
    let owner = h.ops([put(".owner", "writer-1")]);
    let fenced = h.commit(Batch {
        ops: vec![mkdir("part"), put("part/a", "1")],
        when: vec![version_is(".owner", owner.version + 100)],
        ..Batch::default()
    });
    assert!(!fenced.committed);
    assert_eq!(
        fenced.outcomes,
        [
            Outcome::Skipped(Skip::Pred(PredSource::When(0))),
            Outcome::Skipped(Skip::Pred(PredSource::When(0)))
        ]
    );
    let accepted = h.commit(Batch {
        ops: vec![mkdir("part"), put("part/a", "1")],
        when: vec![version_is(".owner", owner.version)],
        ..Batch::default()
    });
    assert!(accepted.committed);
}

#[test]
fn all_or_nothing_applies_nothing_when_any_op_skips() {
    let mut h = Harness::new();
    h.ops([mkdir("d")]);
    let result = h.commit(Batch {
        ops: vec![mkdir("a"), mkdir("d"), mkdir("b")],
        all_or_nothing: true,
        ..Batch::default()
    });
    assert!(!result.committed);
    assert_eq!(
        result.outcomes,
        [
            Outcome::Skipped(Skip::Aborted),
            Outcome::Skipped(Skip::Error(Error::Exists)),
            Outcome::Skipped(Skip::Aborted)
        ]
    );
    assert!(matches!(h.stat("a"), QueryResult::Failed(Error::NotFound)));
}

#[test]
fn a_batch_that_changes_nothing_takes_no_version() {
    let mut h = Harness::new();
    let first = h.ops([mkdir("d")]);
    let nothing = h.ops([mkdir("d")]);
    assert!(!nothing.committed);
    assert_eq!(nothing.version, first.version);
    let next = h.ops([mkdir("e")]);
    assert_eq!(next.version, first.version + 1);
}

#[test]
fn a_retried_request_gets_its_first_answer() {
    let mut h = Harness::new();
    let batch = Batch {
        ops: vec![mv("a", "b")],
        request_id: 77,
        ..Batch::default()
    };
    h.ops([mkdir("a")]);
    let first = h.commit(batch.clone());
    assert!(first.committed);
    let retried = h.commit(batch);
    assert_eq!(retried, first);
}

#[test]
fn directory_mtime_follows_its_entries() {
    let mut h = Harness::new();
    h.ops([mkdir("d")]);
    h.tick(500);
    let result = h.ops([put("d/f", "v")]);
    match h.stat("d") {
        QueryResult::Stat { inode, .. } => {
            assert_eq!(inode.mtime, h.now);
            assert_eq!(inode.version, result.version);
        }
        other => panic!("{other:?}"),
    }
    h.tick(500);
    h.ops([put("d/f", "w")]);
    match h.stat("d") {
        QueryResult::Stat { inode, .. } => assert_eq!(
            inode.version, result.version,
            "writing a file leaves its directory"
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn remove_all_detaches_a_subtree_then_frees_it() {
    let mut h = Harness::new();
    h.ops([
        mkdir_p("t/p/q"),
        put("t/p/q/f", "v"),
        put("t/p/g", "v"),
        link("t/p/g", "keep"),
    ]);
    let result = h.ops([remove_all("t/p")]);
    assert!(result.committed);
    assert!(matches!(
        h.stat("t/p"),
        QueryResult::Failed(Error::NotFound)
    ));
    // The idle writer frees the subtree in later commits.
    assert!(h.writer.version() > result.version);
    assert_eq!(h.writer.detached(), 0);
    assert_eq!(nlink(&mut h, "keep"), 1, "a link outside the subtree stays");
    let removed = match result.outcomes[0] {
        Outcome::Applied { ino: Some(ino) } => ino,
        other => panic!("{other:?}"),
    };
    let (_, stat) = h.query(Query::Stat(Target::Ino(removed)));
    assert!(
        matches!(stat, QueryResult::Stat { inode, .. } if matches!(inode.life, Life::Freed(_)))
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_large_subtree_frees_in_bounded_commits() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a"), mkdir("t/b")]);
    for chunk in 0..5 {
        h.ops((0..500).map(|index| put(&format!("t/a/f{chunk}-{index}"), "v")));
    }
    h.ops((0..300).map(|index| mkdir(&format!("t/b/d{index}"))));
    // A restarted writer frees the subtree from a cold cache.
    let mut h = Harness::with(h.storage.clone(), Default::default());
    let removed = h.ops([remove_all("t")]).version;
    let commits = h.writer.version() - removed;
    assert!(
        (3..=4).contains(&commits),
        "2,803 inodes in commits of at most 1,000: {commits}"
    );
    assert!(
        h.reads <= 25,
        "a round of reads per directory, not per entry: {}",
        h.reads
    );
    assert_eq!(h.writer.detached(), 0);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A batch may write little and still be too large: each file it frees
/// deep in the tree writes a queue key under every directory above it,
/// and a transaction is charged several times a small key's bytes.
#[test]
fn a_batch_is_too_large_by_what_its_small_keys_are_charged() {
    const FILES: usize = 6_000;
    let mut h = Harness::new();
    let deep = "a/b/c/d/e/f";
    h.ops([mkdir_p(deep), mkdir("top")]);
    for chunk in (0..FILES).collect::<Vec<_>>().chunks(2_000) {
        h.ops(chunk.iter().map(|n| put(&format!("{deep}/f{n}"), "v")));
        h.ops(chunk.iter().map(|n| put(&format!("top/f{n}"), "v")));
    }
    let unlinks = |dir: &str, files: std::ops::Range<usize>| Batch {
        ops: files.map(|n| unlink(&format!("{dir}/f{n}"))).collect(),
        ..Batch::default()
    };
    // As many small files as a batch holds ops fit.
    h.ops([mkdir("wide")]);
    let wide = h.ops((0..10_000).map(|n| put(&format!("wide/f{n}"), "a small value")));
    assert!(wide.outcomes.iter().all(Outcome::applied));
    // One directory down, the batch commits.
    let before = h.writer.version();
    assert!(h.commit(unlinks("top", 0..FILES)).committed);
    // Six down, it writes under 3 MiB and is charged over 9 MB.
    assert_eq!(
        h.submit(unlinks(deep, 0..FILES)),
        inodedb_core::writer::Answer::Rejected(inodedb_core::ops::BatchError::TooLarge)
    );
    assert_eq!(h.writer.version(), before + 1, "it changed nothing");
    // In halves it commits.
    assert!(h.commit(unlinks(deep, 0..FILES / 2)).committed);
    assert!(h.commit(unlinks(deep, FILES / 2..FILES)).committed);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn malformed_batches_are_rejected() {
    let mut h = Harness::new();
    let answer = h.submit(Batch {
        ops: vec![mkdir("a").when(Pred::Applied(0))],
        ..Batch::default()
    });
    assert_eq!(
        answer,
        inodedb_core::writer::Answer::Rejected(inodedb_core::ops::BatchError::Malformed)
    );
}

#[test]
fn a_restarted_writer_reads_what_it_needs() {
    let mut h = Harness::new();
    h.ops([mkdir_p("a/b"), put("a/b/f", "v"), link("a/b/f", "a/g")]);
    let storage = h.storage.clone();
    let mut restarted = Harness::with(storage, Default::default());
    let result = restarted.ops([mv("a/b", "c"), unlink("a/g")]);
    assert_eq!(applied(&result), [true, true]);
    assert_eq!(nlink(&mut restarted, "c/f"), 1);
    assert!(restarted.fsck().is_empty(), "{:?}", restarted.fsck());
}

/// How many inodes each commit after `after` put in the free queue.
fn queued_by_commit(h: &Harness, after: u64) -> Vec<usize> {
    let mut queued = std::collections::BTreeMap::new();
    for (key, _) in h.storage.dump() {
        if let Some(inodedb_core::keys::Parsed::Free(version, _, None)) =
            inodedb_core::keys::parse(&key)
            && version > after
        {
            *queued.entry(version).or_insert(0) += 1;
        }
    }
    queued.into_values().collect()
}

#[test]
fn detached_subtrees_free_together_in_one_commit() {
    const PARTS: usize = 20;
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a"), mkdir_p("u/b")]);
    let part = |n: usize| [format!("t/a/p{n}"), format!("u/b/q{n}")];
    for dir in (0..PARTS).flat_map(part) {
        h.ops([
            mkdir(&dir),
            put(&format!("{dir}/x"), "v"),
            put(&format!("{dir}/y"), "v"),
            put(&format!("{dir}/z"), "v"),
        ]);
    }
    let ino = |h: &mut Harness, path: &str| match h.stat(path) {
        QueryResult::Stat { ino, .. } => ino,
        other => panic!("{path}: {other:?}"),
    };
    let (t, a) = (ino(&mut h, "t"), ino(&mut h, "t/a"));
    let (u, b) = (ino(&mut h, "u"), ino(&mut h, "u/b"));
    // One batch detaches forty parts under two parents.
    let dirs: Vec<String> = (0..PARTS).flat_map(part).collect();
    let removed = h.ops(dirs.iter().map(|dir| remove_all(dir))).version;
    assert_eq!(
        h.writer.version() - removed,
        1,
        "forty subtrees of four inodes each fit one commit's budget"
    );
    assert_eq!(h.writer.detached(), 0);
    assert_eq!(queued_by_commit(&h, removed), [8 * PARTS]);
    // Each freed record names the directories that lay above its own
    // subtree: a part's files and its top alike.
    let page = h.list_freed(inodedb_core::auth::Caller::Trusted(h.root), None, 1_000);
    let under = |above: [u64; 2], apart: [u64; 2]| {
        page.entries
            .iter()
            .filter(|entry| {
                above.iter().all(|dir| entry.ancestors.contains(dir))
                    && apart.iter().all(|dir| !entry.ancestors.contains(dir))
            })
            .count()
    };
    assert_eq!(page.entries.len(), 8 * PARTS);
    assert_eq!(under([t, a], [u, b]), 4 * PARTS);
    assert_eq!(under([u, b], [t, a]), 4 * PARTS);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_freeing_commit_spends_one_budget_across_subtrees() {
    let mut h = Harness::new();
    let dirs = ["a", "b", "c"];
    for dir in dirs {
        h.ops([mkdir(dir)]);
        h.ops((0..400).map(|index| put(&format!("{dir}/f{index}"), "v")));
    }
    let removed = h.ops(dirs.map(remove_all)).version;
    // 1,203 inodes: the first commit frees two subtrees whole and the
    // third's files up to its budget, and the second frees the rest.
    assert_eq!(queued_by_commit(&h, removed), [1_000, 203]);
    assert_eq!(h.writer.detached(), 0);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// Deep in the tree a freed inode writes a queue key under each of many
/// directories, so a freeing commit stops at a number of keys before it
/// reaches its number of inodes.
#[test]
fn a_freeing_commit_deep_in_the_tree_stops_at_its_keys() {
    const DEPTH: usize = 300;
    const FILES: usize = 600;
    let mut h = Harness::new();
    let deep = vec!["d"; DEPTH].join("/");
    h.ops([mkdir_p(&deep)]);
    h.ops((0..FILES).map(|index| put(&format!("{deep}/f{index}"), "v")));
    let removed = h.ops([remove_all("d")]).version;
    assert_eq!(h.writer.detached(), 0);
    // Every key the queue gained, by the commit that wrote it.
    let mut keys = std::collections::BTreeMap::new();
    for (key, _) in h.storage.dump() {
        let version = match inodedb_core::keys::parse(&key) {
            Some(inodedb_core::keys::Parsed::Free(version, _, _)) => version,
            Some(inodedb_core::keys::Parsed::FreeUnder(_, version, _, _)) => version,
            _ => continue,
        };
        assert!(version > removed);
        *keys.entry(version).or_insert(0) += 1;
    }
    let most = *keys.values().max().expect("the freeing queued its inodes");
    // A file writes its key and one under each of the 300 directories
    // above it, the last a commit frees among them.
    assert!(
        most <= inodedb_core::exec::REAP_KEYS + DEPTH + 1,
        "{most} queue keys in one commit"
    );
    assert!(keys.len() >= 3, "{} commits", keys.len());
    assert_eq!(
        queued_by_commit(&h, removed).iter().sum::<usize>(),
        FILES + DEPTH
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn cold_subtrees_ask_for_their_records_in_the_same_rounds() {
    const PARTS: usize = 40;
    let mut h = Harness::new();
    let dirs: Vec<String> = (0..PARTS).map(|n| format!("p{n}")).collect();
    for dir in &dirs {
        h.ops([
            mkdir(dir),
            put(&format!("{dir}/x"), "v"),
            put(&format!("{dir}/y"), "v"),
        ]);
    }
    // The detach commits, and the writer stops before its freeing does.
    let token = h.token();
    h.writer.submit(
        token,
        inodedb_core::auth::Caller::Trusted(h.root),
        Batch {
            ops: dirs.iter().map(|dir| remove_all(dir)).collect(),
            ..Batch::default()
        },
    );
    h.run_until(token);
    // A restarted writer frees the forty subtrees from a cold cache.
    let h = Harness::with(h.storage.clone(), Default::default());
    assert_eq!(h.writer.detached(), 0);
    assert!(
        h.reads <= 4,
        "a round of reads per level, not per subtree: {}",
        h.reads
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// Each subtree holds a directory of its own. A subtree asks for what it
/// lacks at each level in the round the others ask in, whichever of them
/// still lack records.
#[test]
fn cold_subtrees_with_directories_inside_share_their_rounds() {
    const PARTS: usize = 40;
    let mut h = Harness::new();
    let dirs: Vec<String> = (0..PARTS).map(|n| format!("p{n}")).collect();
    for dir in &dirs {
        h.ops([
            mkdir(dir),
            put(&format!("{dir}/x"), "v"),
            mkdir(&format!("{dir}/in")),
            put(&format!("{dir}/in/a"), "v"),
            put(&format!("{dir}/in/b"), "v"),
        ]);
    }
    // The detach commits, and the writer stops before its freeing does.
    let token = h.token();
    h.writer.submit(
        token,
        inodedb_core::auth::Caller::Trusted(h.root),
        Batch {
            ops: dirs.iter().map(|dir| remove_all(dir)).collect(),
            ..Batch::default()
        },
    );
    h.run_until(token);
    // A restarted writer frees the forty subtrees from a cold cache.
    let h = Harness::with(h.storage.clone(), Default::default());
    assert_eq!(h.writer.detached(), 0);
    assert!(
        h.reads <= 6,
        "a round of reads per level, not per subtree: {}",
        h.reads
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_wide_directory_frees_over_several_commits() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    for chunk in 0..3 {
        h.ops((0..500).map(|index| put(&format!("t/f{chunk}-{index}"), "v")));
    }
    let removed = h.ops([remove_all("t")]).version;
    assert!(
        h.writer.version() - removed >= 2,
        "1,501 inodes take more than one commit of at most 1,000"
    );
    assert_eq!(h.writer.detached(), 0);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}
