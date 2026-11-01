//! `idb mount`: a read-only FUSE view of the tree under the key's root, for
//! debugging. Each lookup and stat asks the server, and the kernel caches
//! attributes and names for a second. An open directory lists once and an
//! open file reads once, so each reads as of one version.

use fuser::{
    Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, INodeNo, LockOwner,
    MountOption, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, Request,
};
use inodedb::{Client, Error, FsError, Handle, Inode, Kind, List, Retry, Target};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};
use tokio::runtime::Handle as Runtime;

/// How long the kernel may keep an answer.
const TTL: Duration = Duration::from_secs(1);

/// How long a call waits for the server before the view answers EIO: each
/// call blocks the process that made it, and the shell that stats the mount.
const RETRY: Retry = Retry {
    attempt_timeout: Duration::from_secs(5),
    deadline: Duration::from_secs(5),
    first_delay: Duration::from_millis(20),
    max_delay: Duration::from_secs(1),
};

/// What the view knows of an inode it has shown.
#[derive(Clone)]
struct Known {
    handle: Handle,
    /// The directory it was reached through, for a directory's `..`.
    parent: u64,
}

/// What an open directory or file holds.
enum Opened {
    Dir(Vec<(INodeNo, FileType, Vec<u8>)>),
    File(inodedb::Bytes),
}

struct View {
    client: Client,
    runtime: Runtime,
    /// The key's root, which the kernel knows as inode 1.
    root: u64,
    known: Mutex<BTreeMap<u64, Known>>,
    opened: Mutex<BTreeMap<u64, Opened>>,
    next_handle: AtomicU64,
}

fn errno(error: &Error) -> Errno {
    match error {
        Error::Fs(FsError::NotFound) => Errno::ENOENT,
        Error::Fs(FsError::NotDir) => Errno::ENOTDIR,
        Error::Fs(FsError::IsDir) => Errno::EISDIR,
        Error::Fs(FsError::Invalid) => Errno::EINVAL,
        Error::Status(status)
            if matches!(
                status.code(),
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
            ) =>
        {
            Errno::EACCES
        }
        _ => Errno::EIO,
    }
}

fn file_type(kind: Kind) -> FileType {
    match kind {
        Kind::Dir => FileType::Directory,
        Kind::File => FileType::RegularFile,
    }
}

impl View {
    /// The kernel's number for an inode: the key's root is 1.
    fn number(&self, ino: u64) -> INodeNo {
        INodeNo(if ino == self.root { 1 } else { ino })
    }

    fn ino(&self, number: INodeNo) -> u64 {
        match number.0 {
            1 => self.root,
            ino => ino,
        }
    }

    fn known(&self, number: INodeNo) -> Option<Known> {
        self.known
            .lock()
            .expect("no panics holding the inodes")
            .get(&self.ino(number))
            .cloned()
    }

    fn learn(&self, inode: &Inode, parent: u64) {
        self.known
            .lock()
            .expect("no panics holding the inodes")
            .insert(
                inode.handle.ino,
                Known {
                    handle: inode.handle.clone(),
                    parent,
                },
            );
    }

    fn open(&self, opened: Opened) -> FileHandle {
        let fh = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.opened
            .lock()
            .expect("no panics holding the open files")
            .insert(fh, opened);
        FileHandle(fh)
    }

    fn close(&self, fh: FileHandle) {
        self.opened
            .lock()
            .expect("no panics holding the open files")
            .remove(&fh.0);
    }

    fn attr(&self, inode: &Inode) -> FileAttr {
        let mtime = UNIX_EPOCH + Duration::from_millis(inode.mtime);
        let perm = match inode.kind {
            Kind::Dir => 0o555,
            Kind::File => 0o444,
        };
        FileAttr {
            ino: self.number(inode.handle.ino),
            size: inode.size,
            blocks: inode.size.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind: file_type(inode.kind),
            perm,
            nlink: inode.nlink.max(1),
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

impl Filesystem for View {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(dir) = self.known(parent) else {
            return reply.error(Errno::ENOENT);
        };
        // The directory by its handle, wherever it now is; the name sorts
        // first among those it prefixes.
        let name = name.as_bytes();
        let options = List {
            prefix: Some(inodedb::Bytes::copy_from_slice(name)),
            limit: 1,
            attrs: true,
            ..List::default()
        };
        let listing = match self
            .runtime
            .block_on(self.client.list_dir(&dir.handle, &options))
        {
            Ok(listing) => listing,
            Err(error) => return reply.error(errno(&error)),
        };
        match listing
            .entries
            .first()
            .filter(|entry| entry.name == name)
            .and_then(|entry| entry.inode.as_ref())
        {
            Some(inode) => {
                self.learn(inode, dir.handle.ino);
                reply.entry(&TTL, &self.attr(inode), fuser::Generation(0));
            }
            None => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Some(known) = self.known(ino) else {
            return reply.error(Errno::ENOENT);
        };
        match self
            .runtime
            .block_on(self.client.stat(Target::from(&known.handle)))
        {
            Ok(inode) => reply.attr(&TTL, &self.attr(&inode)),
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let Some(known) = self.known(ino) else {
            return reply.error(Errno::ENOENT);
        };
        match self
            .runtime
            .block_on(self.client.read(Target::from(&known.handle)))
        {
            Ok(value) => reply.opened(self.open(Opened::File(value.value)), FopenFlags::empty()),
            Err(error) => reply.error(errno(&error)),
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let opened = self
            .opened
            .lock()
            .expect("no panics holding the open files");
        let Some(Opened::File(value)) = opened.get(&fh.0) else {
            return reply.error(Errno::EBADF);
        };
        let start = (offset as usize).min(value.len());
        let end = start.saturating_add(size as usize).min(value.len());
        reply.data(&value[start..end]);
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.close(fh);
        reply.ok();
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let Some(dir) = self.known(ino) else {
            return reply.error(Errno::ENOENT);
        };
        let options = List {
            attrs: true,
            ..List::default()
        };
        let listing = match self
            .runtime
            .block_on(self.client.list_dir(&dir.handle, &options))
        {
            Ok(listing) => listing,
            Err(error) => return reply.error(errno(&error)),
        };
        let parent = if dir.handle.ino == self.root {
            ino
        } else {
            self.number(dir.parent)
        };
        // Offsets count from one: "." and "..", then the entries.
        let mut entries = vec![
            (ino, FileType::Directory, b".".to_vec()),
            (parent, FileType::Directory, b"..".to_vec()),
        ];
        for entry in &listing.entries {
            if let Some(inode) = &entry.inode {
                self.learn(inode, dir.handle.ino);
            }
            entries.push((
                self.number(entry.handle.ino),
                file_type(entry.kind),
                entry.name.to_vec(),
            ));
        }
        reply.opened(self.open(Opened::Dir(entries)), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let opened = self
            .opened
            .lock()
            .expect("no panics holding the open files");
        let Some(Opened::Dir(entries)) = opened.get(&fh.0) else {
            return reply.error(Errno::EBADF);
        };
        for (index, (number, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*number, index as u64 + 1, *kind, OsStr::from_bytes(name)) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.close(fh);
        reply.ok();
    }
}

/// Mounts the tree under the key's root at `dir` until it is unmounted.
pub async fn mount(client: Client, dir: std::path::PathBuf) -> Result<(), String> {
    let client = client.with_retry(RETRY);
    let root = client
        .stat("")
        .await
        .map_err(|error| format!("the key's root: {error}"))?;
    let view = View {
        client,
        runtime: Runtime::current(),
        root: root.handle.ino,
        known: Mutex::new(BTreeMap::from([(
            root.handle.ino,
            Known {
                handle: root.handle.clone(),
                parent: root.handle.ino,
            },
        )])),
        opened: Mutex::new(BTreeMap::new()),
        next_handle: AtomicU64::new(1),
    };
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::RO,
        MountOption::FSName("inodedb".into()),
        MountOption::Subtype("inodedb".into()),
    ];
    // The FUSE loop blocks; each callback waits on the runtime for its call.
    tokio::task::spawn_blocking(move || fuser::mount(view, &dir, &config))
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| format!("mount: {error}"))
}
