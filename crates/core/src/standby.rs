//! A standby: a copy of the store that the log keeps current, which
//! answers reads at or past a version a client names, sends everything
//! else to the writer, and takes over once the writer stops renewing.

use crate::auth::{self, Caller, Denied, Grant, KeyRecord, Keys, Scope};
use crate::cache::Cache;
use crate::codec::DecodeError;
use crate::feed::{Change, Subscriber};
use crate::keys::{self, KvRead, KvResult};
use crate::log::LogEntry;
use crate::query::{self, Query};
use crate::types::{Millis, ROOT, Version};
use crate::view::{Fact, Read};
use crate::writer::{Action, Answer, Config, Head, Takeover, Token};
use std::collections::BTreeMap;

/// Watches the head record, which every commit rewrites, and says when it
/// has gone unchanged for `wait` on the standby's own clock. An empty
/// store has no writer to wait for.
#[derive(Debug)]
pub struct Successor {
    wait: Millis,
    /// The head as last read, and when a read first returned it.
    seen: Option<(Option<Vec<u8>>, Millis)>,
}

impl Successor {
    pub fn new(wait: Millis) -> Successor {
        Successor { wait, seen: None }
    }

    /// Notes the head a read found, as of when the read returned; once the
    /// writer is due to be taken over, what to take over from.
    pub fn observe(&mut self, head: Option<Vec<u8>>, now: Millis) -> Option<Takeover> {
        let since = match &self.seen {
            Some((seen, since)) if *seen == head => *since,
            _ => {
                self.seen = Some((head.clone(), now));
                now
            }
        };
        (head.is_none() || now >= since + self.wait).then_some(Takeover::From(head))
    }

    /// Starts watching afresh, as after a takeover that lost.
    pub fn reset(&mut self) {
        self.seen = None;
    }
}

/// A read a client asked of the standby.
#[derive(Debug)]
struct Waiting {
    token: Token,
    caller: Caller,
    query: Query,
    at_least: Version,
    /// The newest version a read for it reflects; it runs again once the
    /// follower reaches that, when the read has filled the cache.
    after: Version,
    /// Whether a read for it is in flight.
    reading: bool,
    /// The cache's mark when it first missed: eviction spares what its
    /// reads filled since.
    since: Option<u64>,
}

/// The read every follower starts with: the head, every key, revoked
/// keys' ids, and the handle secret.
const STARTUP: u64 = 0;

/// The whole tree as of a version, kept current by the log, answering
/// `AtLeast` reads from its cache and from storage.
pub struct Follower {
    config: Config,
    now: Millis,
    subscriber: Subscriber,
    started: bool,
    /// The head record as last read: the writer's address, and how far
    /// storage has reached.
    head: Option<Head>,
    secret: Option<[u8; 32]>,
    successor: Successor,
    queries: BTreeMap<u64, Waiting>,
    /// The query each read in progress serves.
    reads: BTreeMap<u64, (u64, Vec<Read>)>,
    actions: Vec<Action>,
    next_id: u64,
}

impl Follower {
    /// A follower that reads the head and the keys before it answers.
    pub fn new(config: Config, now: Millis) -> Follower {
        let budget = config.cache;
        let successor = Successor::new(config.takeover_wait());
        Follower {
            config,
            now,
            subscriber: Subscriber::with_budget(ROOT, 0, budget),
            started: false,
            head: None,
            secret: None,
            successor,
            queries: BTreeMap::new(),
            reads: BTreeMap::new(),
            actions: vec![Action::Read {
                id: STARTUP,
                reads: vec![
                    KvRead::Get(keys::head()),
                    KvRead::Scan {
                        range: keys::key_records(),
                        limit: usize::MAX,
                    },
                    KvRead::Scan {
                        range: keys::retired_keys(),
                        limit: usize::MAX,
                    },
                    KvRead::Get(keys::handle_secret()),
                ],
            }],
            next_id: STARTUP + 1,
        }
    }

    pub fn set_now(&mut self, now: Millis) {
        self.now = self.now.max(now);
    }

    /// Whether the startup read has come back, so that the follower knows
    /// where the log continues.
    pub fn is_started(&self) -> bool {
        self.started
    }

    /// The version the follower's cache reflects; the log continues after
    /// it.
    pub fn version(&self) -> Version {
        self.subscriber.version()
    }

    /// The secret handles' MACs are made under, once storage holds one.
    pub fn handle_secret(&self) -> Option<[u8; 32]> {
        self.secret
    }

    /// How many keys the store holds.
    pub fn keys(&self) -> usize {
        self.subscriber.cache().keys().len()
    }

    /// Where the writer the head names takes requests.
    pub fn address(&self) -> String {
        self.head
            .as_ref()
            .map_or_else(String::new, |head| head.address.clone())
    }

    /// Notes the head record a read found just now; once it has gone
    /// unchanged long enough, what to take over from.
    pub fn observe(&mut self, head: Option<Vec<u8>>) -> Result<Option<Takeover>, DecodeError> {
        if let Some(bytes) = &head {
            self.head = Some(Head::decode(bytes)?);
        }
        Ok(self.successor.observe(head, self.now))
    }

    /// Applies the log's next entry, which follows the follower's version.
    pub fn apply(&mut self, entry: &LogEntry) {
        let spare = self
            .queries
            .values()
            .filter_map(|waiting| waiting.since)
            .min()
            .unwrap_or(u64::MAX);
        self.subscriber.spare(spare);
        self.subscriber.apply(&Change::whole(entry));
        self.run_waiting();
    }

    /// The caller's scope under the keys as of the follower's version.
    pub fn authorize(&self, caller: &Caller, need: Grant) -> Result<Scope, Denied> {
        auth::authorize(self.subscriber.cache(), caller, self.now, need)
    }

    /// Answers a read at `at_least` or later once the follower reaches it.
    /// The newest state, asked for with zero, is the writer's to give.
    pub fn query(&mut self, token: Token, caller: Caller, query: Query, at_least: Version) {
        if at_least == 0 {
            let address = self.address();
            self.actions.push(Action::Answer {
                token,
                answer: Answer::NotWriter { address },
            });
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.queries.insert(
            id,
            Waiting {
                token,
                caller,
                query,
                at_least,
                after: 0,
                reading: false,
                since: None,
            },
        );
        self.run(id);
    }

    /// Runs the queries the follower's version has reached that no read
    /// holds up.
    fn run_waiting(&mut self) {
        let version = self.version();
        let ready: Vec<u64> = self
            .queries
            .iter()
            .filter(|(_, waiting)| {
                !waiting.reading && waiting.at_least.max(waiting.after) <= version
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ready {
            self.run(id);
        }
    }

    fn run(&mut self, id: u64) {
        let Some(waiting) = self.queries.get(&id) else {
            return;
        };
        let version = self.version();
        if !self.started || waiting.at_least.max(waiting.after) > version {
            return;
        }
        let answer = match self.authorize(&waiting.caller, Grant::Read) {
            // A key the follower doesn't know may be newer than the log it
            // has read, so the writer, which knows every key, decides.
            Err(Denied::Unknown) => Answer::NotWriter {
                address: self.address(),
            },
            Err(denied) => Answer::Denied(denied),
            Ok(scope) => match query::run(self.subscriber.cache(), scope.root, &waiting.query) {
                Ok(result) => Answer::Query { version, result },
                Err(reads) => {
                    let read = self.next_id;
                    self.next_id += 1;
                    let mut kv = Vec::with_capacity(reads.len() + 1);
                    kv.push(KvRead::Get(keys::head()));
                    kv.extend(reads.iter().map(Read::kv));
                    self.reads.insert(read, (id, reads));
                    let mark = self.subscriber.mark();
                    if let Some(waiting) = self.queries.get_mut(&id) {
                        waiting.reading = true;
                        waiting.since.get_or_insert(mark);
                    }
                    self.actions.push(Action::Read {
                        id: read,
                        reads: kv,
                    });
                    return;
                }
            },
        };
        let token = waiting.token;
        self.queries.remove(&id);
        self.actions.push(Action::Answer { token, answer });
    }

    /// Results of a snapshot read, in the order the action listed them.
    pub fn read_done(&mut self, id: u64, results: Vec<KvResult>) -> Result<(), DecodeError> {
        let mut results = results.into_iter();
        let head = match results.next() {
            Some(KvResult::Get(head)) => head.as_deref().map(Head::decode).transpose()?,
            _ => return Err(DecodeError),
        };
        let at = head.as_ref().map_or(0, |head| head.version);
        if id == STARTUP {
            let (
                Some(KvResult::Scan(records)),
                Some(KvResult::Scan(retired)),
                Some(KvResult::Get(secret)),
            ) = (results.next(), results.next(), results.next())
            else {
                return Err(DecodeError);
            };
            let prefix = keys::key_records().start.len();
            let id_of = |key: &[u8]| -> Result<u64, DecodeError> {
                let id = key.get(prefix..).ok_or(DecodeError)?;
                Ok(u64::from_be_bytes(id.try_into().map_err(|_| DecodeError)?))
            };
            let mut loaded = Keys::default();
            for (key, value) in records {
                loaded.put(id_of(&key)?, Some(KeyRecord::decode(&value)?));
            }
            for (key, _) in retired {
                loaded.retire(id_of(&key)?);
            }
            self.secret = secret
                .map(|secret| secret.as_slice().try_into().map_err(|_| DecodeError))
                .transpose()?;
            self.subscriber = Subscriber::with_budget(ROOT, at, self.config.cache);
            self.subscriber.load_keys(loaded);
            self.head = head.or(self.head.take());
            self.started = true;
            self.run_waiting();
            return Ok(());
        }
        let Some((query, reads)) = self.reads.remove(&id) else {
            return Ok(());
        };
        // A value under a version the read hadn't reached may yet be
        // written: the query waits for the log to reach that version.
        let mut after = at;
        for (read, result) in reads.iter().zip(results) {
            let fact = read.fact(result)?;
            if let Fact::Value(_, version, None) = &fact
                && *version > at
            {
                after = after.max(*version);
            }
            self.subscriber.fill(fact, at);
        }
        if let Some(waiting) = self.queries.get_mut(&query) {
            waiting.after = waiting.after.max(after);
            waiting.reading = false;
        }
        self.run(query);
        Ok(())
    }

    /// The actions to carry out, in order.
    pub fn drain(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }

    /// What the writer that takes over starts from.
    pub fn hand_over(self) -> Handover {
        let version = self.version();
        let waiting = self
            .queries
            .into_values()
            .map(|waiting| Pending {
                token: waiting.token,
                caller: waiting.caller,
                query: waiting.query,
                at_least: waiting.at_least,
            })
            .collect();
        Handover {
            cache: self.subscriber.into_cache(),
            version,
            waiting,
        }
    }
}

/// A follower's cache and the version it reflects, with the reads still
/// waiting, for the writer that takes over.
pub struct Handover {
    pub cache: Cache,
    pub version: Version,
    pub waiting: Vec<Pending>,
}

/// A read a follower had yet to answer.
pub struct Pending {
    pub token: Token,
    pub caller: Caller,
    pub query: Query,
    pub at_least: Version,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_standby_waits_for_the_head_to_go_unchanged() {
        let mut successor = Successor::new(6_000);
        assert_eq!(
            successor.observe(None, 0),
            Some(Takeover::From(None)),
            "an empty store has no writer"
        );
        let head = Some(vec![1]);
        assert_eq!(successor.observe(head.clone(), 100), None);
        assert_eq!(successor.observe(head.clone(), 6_099), None);
        // A renewal starts the wait over.
        assert_eq!(successor.observe(Some(vec![2]), 6_200), None);
        assert_eq!(successor.observe(Some(vec![2]), 12_199), None);
        assert_eq!(
            successor.observe(Some(vec![2]), 12_200),
            Some(Takeover::From(Some(vec![2])))
        );
        successor.reset();
        assert_eq!(successor.observe(Some(vec![2]), 12_300), None);
    }
}
