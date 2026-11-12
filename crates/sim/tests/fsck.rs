//! Each invariant `fsck` checks, broken on its own in a sound store.

use inodedb_core::auth::{Caller, Grant};
use inodedb_core::keys::{self, Pairs, Parsed};
use inodedb_core::log::LogEntry;
use inodedb_core::types::{Entry, Ino, Inode, Kind, ROOT, Version};
use inodedb_core::writer::Head;
use inodedb_core::{fsck, query::QueryResult};
use inodedb_sim::harness::*;

/// A sound store: directories a/b, a file a/f linked again as a/b/g, a
/// key rooted at a, and a request record.
struct Store {
    pairs: Pairs,
    log: Vec<(Version, Vec<u8>)>,
    a: Ino,
    b: Ino,
    f: Ino,
}

fn store() -> Store {
    let mut h = Harness::new();
    h.ops([mkdir_p("a/b"), put("a/f", "value")]);
    h.ops([link("a/f", "a/b/g")]);
    h.mint(Caller::Trusted(ROOT), "a", Grant::Write, u64::MAX);
    h.commit(inodedb_core::ops::Batch {
        ops: vec![set_mtime("a", 5)],
        request_id: 1 << 64 | 1,
        ..Default::default()
    });
    let ino = |h: &mut Harness, path| match h.stat(path) {
        QueryResult::Stat { ino, .. } => ino,
        other => panic!("{other:?}"),
    };
    let store = Store {
        a: ino(&mut h, "a"),
        b: ino(&mut h, "a/b"),
        f: ino(&mut h, "a/f"),
        pairs: h.storage.dump(),
        log: h.storage.log_now(0, usize::MAX),
    };
    assert_eq!(problems(&store), Vec::<String>::new());
    store
}

fn problems(store: &Store) -> Vec<String> {
    fsck::check_with_log(
        store
            .pairs
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
        store
            .log
            .iter()
            .map(|(version, bytes)| (*version, bytes.as_slice())),
    )
}

/// Breaks the store with `change` and checks fsck names the break.
fn finds(change: impl FnOnce(&mut Store), expected: &str) {
    let mut store = store();
    change(&mut store);
    let found = problems(&store);
    assert!(
        found.iter().any(|problem| problem.contains(expected)),
        "expected {expected:?} in {found:?}"
    );
}

fn set(store: &mut Store, key: Vec<u8>, value: Vec<u8>) {
    match store.pairs.binary_search_by(|(k, _)| k.cmp(&key)) {
        Ok(at) => store.pairs[at].1 = value,
        Err(at) => store.pairs.insert(at, (key, value)),
    }
}

fn clear(store: &mut Store, key: &[u8]) {
    let before = store.pairs.len();
    store.pairs.retain(|(k, _)| k != key);
    assert_eq!(store.pairs.len() + 1, before, "the key was there");
}

fn inode(store: &Store, ino: Ino) -> Inode {
    let key = keys::inode(ino);
    let (_, value) = store.pairs.iter().find(|(k, _)| *k == key).expect("stored");
    Inode::from_bytes(value).expect("decodes")
}

fn entry(store: &Store, dir: Ino, name: &str) -> Entry {
    let key = keys::entry(dir, name.as_bytes());
    let (_, value) = store.pairs.iter().find(|(k, _)| *k == key).expect("stored");
    Entry::from_bytes(value).expect("decodes")
}

fn head(store: &Store) -> Head {
    let (_, value) = store
        .pairs
        .iter()
        .find(|(k, _)| *k == keys::head())
        .expect("stored");
    Head::decode(value).expect("decodes")
}

fn matching(store: &Store, wanted: impl Fn(Parsed) -> bool) -> Vec<u8> {
    store
        .pairs
        .iter()
        .find(|(key, _)| keys::parse(key).is_some_and(&wanted))
        .expect("a matching key")
        .0
        .clone()
}

#[test]
fn a_sound_store_passes() {
    store();
}

#[test]
fn an_entry_naming_a_missing_inode() {
    finds(|s| clear(s, &keys::inode(s.b)), "names missing inode");
}

#[test]
fn an_entry_of_the_wrong_kind() {
    finds(
        |s| {
            let mut e = entry(s, s.a, "b");
            e.kind = Kind::File;
            set(s, keys::entry(s.a, b"b"), e.to_bytes());
        },
        "has the wrong kind",
    );
}

#[test]
fn an_entry_newer_than_its_directory() {
    finds(
        |s| {
            let mut e = entry(s, s.a, "f");
            e.version = inode(s, s.a).version + 1;
            set(s, keys::entry(s.a, b"f"), e.to_bytes());
        },
        "newer than its directory",
    );
}

#[test]
fn an_entry_from_the_future() {
    finds(
        |s| {
            let mut e = entry(s, s.a, "f");
            e.version = head(s).version + 1;
            set(s, keys::entry(s.a, b"f"), e.to_bytes());
        },
        "is from the future",
    );
}

#[test]
fn an_inode_from_the_future() {
    finds(
        |s| {
            let mut i = inode(s, s.f);
            i.version = head(s).version + 1;
            set(s, keys::inode(s.f), i.to_bytes());
        },
        "is from the future",
    );
}

#[test]
fn an_inode_past_the_id_counter() {
    finds(
        |s| {
            let ino = head(s).next_ino;
            let i = inode(s, s.f);
            set(s, keys::inode(ino), i.to_bytes());
        },
        "past the id counter",
    );
}

#[test]
fn a_missing_back_pointer() {
    finds(
        |s| clear(s, &keys::backptr(s.f, s.b, b"g")),
        "counts 2 links, has 2 entries and 1 back-pointers",
    );
}

#[test]
fn a_back_pointer_without_its_entry() {
    finds(
        |s| set(s, keys::backptr(s.f, s.b, b"h"), Vec::new()),
        "has no entry",
    );
}

#[test]
fn a_link_count_off_by_one() {
    finds(
        |s| {
            let mut i = inode(s, s.f);
            i.nlink = 1;
            set(s, keys::inode(s.f), i.to_bytes());
        },
        "counts 1 links",
    );
}

#[test]
fn a_directory_naming_the_wrong_parent() {
    finds(
        |s| {
            let mut i = inode(s, s.b);
            i.parent = ROOT;
            set(s, keys::inode(s.b), i.to_bytes());
        },
        "but names parent",
    );
}

#[test]
fn a_directory_named_twice() {
    finds(
        |s| {
            let e = entry(s, s.a, "b");
            set(s, keys::entry(ROOT, b"b2"), e.to_bytes());
        },
        "has 2 entries naming it",
    );
}

#[test]
fn parents_that_loop() {
    finds(
        |s| {
            let mut i = inode(s, s.a);
            i.parent = s.b;
            set(s, keys::inode(s.a), i.to_bytes());
        },
        "parents loop",
    );
}

#[test]
fn an_entry_naming_the_root() {
    finds(
        |s| {
            let e = Entry {
                child: ROOT,
                kind: Kind::Dir,
                version: 1,
            };
            set(s, keys::entry(s.b, b"up"), e.to_bytes());
        },
        "the root has an entry naming it",
    );
}

#[test]
fn an_unreachable_inode() {
    finds(
        |s| {
            clear(s, &keys::entry(s.a, b"f"));
            clear(s, &keys::backptr(s.f, s.a, b"f"));
            clear(s, &keys::entry(s.b, b"g"));
            clear(s, &keys::backptr(s.f, s.b, b"g"));
            let mut i = inode(s, s.f);
            i.nlink = 0;
            set(s, keys::inode(s.f), i.to_bytes());
        },
        &format!("inode {} is unreachable", store().f),
    );
}

#[test]
fn a_file_without_its_value() {
    finds(
        |s| {
            let i = inode(s, s.f);
            clear(s, &keys::value(s.f, i.value));
        },
        "lacks its value",
    );
}

#[test]
fn a_size_that_disagrees_with_the_value() {
    finds(
        |s| {
            let i = inode(s, s.f);
            set(s, keys::value(s.f, i.value), b"longer value".to_vec());
        },
        "has size 5 but value 12",
    );
}

#[test]
fn a_stray_value() {
    finds(
        |s| set(s, keys::value(s.f, 1), b"old".to_vec()),
        "is neither current nor queued",
    );
}

#[test]
fn a_queue_entry_for_a_live_inode() {
    finds(
        |s| {
            let chain = inodedb_core::delta::encode_ancestors(&[ROOT]);
            set(s, keys::free(1, s.f, None), chain);
        },
        "wasn't freed",
    );
}

#[test]
fn a_queue_entry_without_its_chain() {
    finds(
        |s| set(s, keys::free(1, s.f, None), Vec::new()),
        "names no directories above it",
    );
}

#[test]
fn inodes_without_a_head() {
    finds(|s| clear(s, &keys::head()), "inodes without a head record");
}

#[test]
fn a_request_record_without_its_expiry() {
    finds(
        |s| {
            let key = matching(s, |parsed| matches!(parsed, Parsed::System(b't', _)));
            clear(s, &key);
        },
        "has 0 expiry keys",
    );
}

#[test]
fn an_expiry_that_miscounts_its_record() {
    finds(
        |s| {
            let key = matching(s, |parsed| matches!(parsed, Parsed::System(b't', _)));
            set(s, key, 7u16.to_be_bytes().to_vec());
        },
        "its expiry key says otherwise",
    );
}

#[test]
fn an_expiry_without_its_record() {
    finds(
        |s| {
            let key = matching(s, |parsed| matches!(parsed, Parsed::System(b'r', _)));
            clear(s, &key);
        },
        "without a record",
    );
}

#[test]
fn a_key_rooted_outside_the_tree() {
    finds(
        |s| {
            let mut i = inode(s, s.a);
            i.kind = Kind::File;
            set(s, keys::inode(s.a), i.to_bytes());
        },
        "isn't a live directory in the tree",
    );
}

#[test]
fn a_short_handle_secret() {
    finds(
        |s| set(s, keys::handle_secret(), vec![0; 16]),
        "the handle secret holds Some(16) bytes",
    );
}

#[test]
fn an_unknown_key() {
    finds(|s| set(s, vec![0xff], Vec::new()), "unknown key");
}

#[test]
fn a_log_with_a_gap() {
    finds(
        |s| {
            let middle = s.log.len() / 2;
            s.log.remove(middle);
        },
        "the log skips",
    );
}

#[test]
fn a_log_entry_under_the_wrong_version() {
    finds(
        |s| {
            let (version, bytes) = s.log.pop().expect("entries");
            let mut entry = LogEntry::decode(&bytes).expect("decodes");
            entry.version += 1;
            s.log.push((version, entry.encode()));
        },
        "carries version",
    );
}

#[test]
fn a_log_that_ends_before_the_head() {
    finds(
        |s| {
            s.log.pop();
        },
        "the log ends at",
    );
}

/// A sound store whose free queue holds one entry: the file a/b/f, freed
/// at `at`, which lies under a and b.
fn queued() -> (Store, Version) {
    let mut h = Harness::new();
    h.ops([mkdir_p("a/b"), mkdir("c"), put("a/b/f", "value")]);
    let ino = |h: &mut Harness, path| match h.stat(path) {
        QueryResult::Stat { ino, .. } => ino,
        other => panic!("{other:?}"),
    };
    let (a, b, f) = (ino(&mut h, "a"), ino(&mut h, "a/b"), ino(&mut h, "a/b/f"));
    let at = h.ops([unlink("a/b/f")]).version;
    let store = Store {
        a,
        b,
        f,
        pairs: h.storage.dump(),
        log: h.storage.log_now(0, usize::MAX),
    };
    assert_eq!(problems(&store), Vec::<String>::new());
    (store, at)
}

/// Breaks the store that holds a queue entry and checks fsck names the
/// break.
fn finds_queued(change: impl FnOnce(&mut Store, Version), expected: &str) {
    let (mut store, at) = queued();
    change(&mut store, at);
    let found = problems(&store);
    assert!(
        found.iter().any(|problem| problem.contains(expected)),
        "expected {expected:?} in {found:?}"
    );
}

#[test]
fn a_queue_entry_missing_under_a_directory_above_it() {
    finds_queued(
        |s, at| clear(s, &keys::free_under(s.a, at, s.f, None)),
        "is missing under directory",
    );
}

#[test]
fn a_directory_holding_an_entry_the_queue_lacks() {
    finds_queued(
        |s, at| {
            // Another inode's, which the queue never held.
            set(s, keys::free_under(s.a, at, s.f + 100, None), Vec::new());
        },
        "that the queue lacks",
    );
}

#[test]
fn a_directory_holding_an_entry_that_names_another() {
    finds_queued(
        |s, at| set(s, keys::free_under(s.b + 1, at, s.f, None), Vec::new()),
        "which doesn't name it",
    );
}

#[test]
fn the_root_holding_an_entry_under_it() {
    finds_queued(
        |s, at| set(s, keys::free_under(ROOT, at, s.f, None), Vec::new()),
        "which doesn't name it",
    );
}

#[test]
fn a_key_under_a_directory_that_holds_a_value() {
    finds_queued(
        |s, at| {
            let chain = inodedb_core::delta::encode_ancestors(&[ROOT]);
            set(s, keys::free_under(s.b, at, s.f, None), chain);
        },
        "bytes under directory",
    );
}

#[test]
fn a_queue_kept_under_directories_from_past_the_head() {
    finds_queued(
        |s, _| {
            set(
                s,
                keys::free_index(),
                inodedb_core::writer::encode_indexed_from(u64::MAX),
            )
        },
        "past the head",
    );
}

#[test]
fn directories_holding_entries_with_no_record_of_when_they_began() {
    finds_queued(
        |s, _| clear(s, &keys::free_index()),
        "no record says from which version",
    );
}

#[test]
fn a_record_of_the_queues_parts_that_doesnt_decode() {
    finds_queued(
        |s, _| set(s, keys::free_index(), vec![0xff]),
        "index record doesn't decode",
    );
}

/// An entry freed before the queue kept its parts may lie under no
/// directory.
#[test]
fn an_older_entry_under_no_directory_is_sound() {
    let (mut store, at) = queued();
    let (a, b, f) = (store.a, store.b, store.f);
    clear(&mut store, &keys::free_under(a, at, f, None));
    clear(&mut store, &keys::free_under(b, at, f, None));
    set(
        &mut store,
        keys::free_index(),
        inodedb_core::writer::encode_indexed_from(at),
    );
    assert_eq!(problems(&store), Vec::<String>::new());
}
