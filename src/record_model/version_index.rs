use std::collections::HashSet;
use std::fmt::Display;
use std::ops::Deref;
use std::process::exit;
use std::ptr::{null, null_mut};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Release};
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::locking::locking_strategy::{LHL_read, LockingStrategy};
use CCBPlusTree::record_model::record_point::RecordPoint;
use CCBPlusTree::test::{dec_key, inc_key};
use CCBPlusTree::tree::bplus_tree::BPlusTree;
use crossbeam_skiplist::SkipMap;
use itertools::Itertools;
use parking_lot::RwLock;
use crate::n_test::NUM_RECORDS;
use crate::record_model::v_record_point::{RecordInfo, VersionIndexType};
use crate::record_model::Version;
use crate::utils::safe_cell::SafeCell;

pub const BTREE_V_INDEX_FAN_OUT: usize = NUM_RECORDS / 3;
pub const BTREE_V_INDEX_NUMBER_RECORDS: usize = BTREE_V_INDEX_FAN_OUT * 2;
pub const BTREE_V_INDEX_CONCURRENCY_PROTOCOL: LockingStrategy = LHL_read(4);
pub type InsertVersion = Version;
type DexaBTree<Payload> = BPlusTree<
    BTREE_V_INDEX_FAN_OUT,
    BTREE_V_INDEX_NUMBER_RECORDS,
    InsertVersion,
    RecordInfo<Payload>
>;

#[inline(always)]
fn new_btree<Payload: Default + Clone + Sync + Display + 'static>() -> DexaBTree<Payload> {
    DexaBTree::new_with(
        BTREE_V_INDEX_CONCURRENCY_PROTOCOL,
        Version::MIN,
        Version::MAX,
        inc_key,
        dec_key)
}

type SkipListImpl<Payload> = SkipMap<
    InsertVersion,
    SafeCell<RecordInfo<Payload>>
>;

struct AtomicPtrWrapper<E>(AtomicPtr<E>);

impl<E> Deref for AtomicPtrWrapper<E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0.load(Acquire) }
    }
}

impl<E> Drop for AtomicPtrWrapper<E> {
    fn drop(&mut self) {
        unsafe { let _dang = Box::from_raw(self.0.load(Acquire)); }
    }
}

impl<K, V> Into<AtomicPtrWrapper<SkipMap<K, V>>> for SkipMap<K, V> {
    fn into(self) -> AtomicPtrWrapper<SkipMap<K, V>> {
        AtomicPtrWrapper(AtomicPtr::new(Box::into_raw(Box::new(self))))
    }
}

impl<K, V> Into<AtomicPtrWrapper<RwLock<SkipMap<K, V>>>> for SkipMap<K, V> {
    fn into(self) -> AtomicPtrWrapper<RwLock<SkipMap<K, V>>> {
        AtomicPtrWrapper(AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(self)))))
    }
}

impl<Payload: Default + Clone + Send + Sync + Display + 'static> Into<VersionIndex<Payload>>
for DexaBTree<Payload> {
    fn into(self) -> VersionIndex<Payload> {
        VersionIndex::DexaBTree(AtomicPtrWrapper(AtomicPtr::new(Box::into_raw(Box::new(self)))))
    }
}

pub enum VersionIndex<Payload: Clone + Display + Default + Send + Sync + 'static> {
    VANILLA(AtomicVersionList<Payload>),
    SkipList(AtomicPtrWrapper<SkipListImpl<Payload>>),
    SkipListSynced(AtomicPtrWrapper<RwLock<SkipListImpl<Payload>>>),
    DexaBTree(AtomicPtrWrapper<DexaBTree<Payload>>),
}

impl<Payload: Clone + Default + Display + Send + Sync + 'static> Clone for VersionIndex<Payload> {
    fn clone(&self) -> Self {
        match self {
            VersionIndex::VANILLA(version_list) =>
                VersionIndex::VANILLA(version_list.clone()),
            VersionIndex::SkipList(skip_list) => {
                let sk = SkipMap::new();
                
                for x in skip_list.iter() {
                    sk.insert(*x.key(), SafeCell::new(x.value().get_mut().clone()));
                }

                VersionIndex::SkipList(sk.into())
            },
            VersionIndex::SkipListSynced(rw_skip_list) => {
                let skip_list = rw_skip_list.read();
                let sk = SkipMap::new();
                for x in skip_list.iter() {
                    sk.insert(*x.key(), SafeCell::new(x.value().as_ref().clone()));
                }

                VersionIndex::SkipList(sk.into())
            },
            VersionIndex::DexaBTree(tree) => {
                let n_tree
                    = new_btree();

                if let CRUDOperationResult::MatchedRecords(records)
                    = tree.dispatch(CRUDOperation::Range((0..InsertVersion::MAX).into())).1 {

                    for record in records {
                        let _ins
                            = n_tree.dispatch(CRUDOperation::Insert(record.key, record.payload));
                    }
                }
                n_tree.into()
            }
        }
    }
}

impl<Payload: Clone + Default + Display + Send + Sync + 'static> VersionIndex<Payload> {
    pub fn len(&self) -> usize {
        match self {
            VersionIndex::VANILLA(version_list) =>
                version_list.len(),
            VersionIndex::SkipList(skip_list) =>
                skip_list.len(),
            VersionIndex::SkipListSynced(rw_skip_list) =>
                rw_skip_list.read().len(),
            VersionIndex::DexaBTree(tree) =>
                2usize.pow(tree.height() as _) - 1
        }
    }
    #[inline]
    pub fn new(kind: VersionIndexType, payload: Payload, version: Version) -> Self {
        match kind {
            VersionIndexType::VANILLA =>
                Self::VANILLA(AtomicVersionList::new(payload, version, None)),
            VersionIndexType::SkipList =>
                Self::SkipList(SkipMap::from_iter([(version, SafeCell::new(RecordInfo::new(payload)))]).into()),
            VersionIndexType::SkipListSynced =>
                Self::SkipListSynced(
                    SkipMap::from_iter([(version, SafeCell::new(RecordInfo::new(payload)))]).into()),
            VersionIndexType::BTree => new_btree().into(),
        }
    }

    #[inline]
    pub fn find(&self, version: Version) -> Option<RecordPoint<Version, Payload>> {
        match self {
            VersionIndex::VANILLA(version_list) => version_list
                .find(version)
                .map(|v_entry|
                    RecordPoint::new(v_entry.insert_version, v_entry.payload.clone())),
            VersionIndex::SkipList(skip_list) => {
               skip_list
                   .range(..=version)
                   .rev()
                   .next()
                   .map(|entry|
                       if version >= *entry.key() &&
                           entry.value().del_version.get().map(|del| del > version)
                               .unwrap_or(true)
                       {
                           Some(RecordPoint::new(*entry.key(), entry.value().payload.clone()))
                       }
                       else {
                           None
                       }
                   ).unwrap_or_default()
            },
            VersionIndex::SkipListSynced(skip_list) =>
                skip_list
                    .read()
                    .range(..=version)
                    .rev()
                    .next()
                    .map(|entry|
                        if version >= *entry.key() &&
                            entry.value().del_version.get().map(|del| del > version)
                                .unwrap_or(true)
                        {
                            Some(RecordPoint::new(*entry.key(), entry.value().payload.clone()))
                        }
                        else {
                            None
                        }
                    ).unwrap_or_default(),
            VersionIndex::DexaBTree(tree) =>
                match tree.dispatch(CRUDOperation::Point(version)).1 {
                    CRUDOperationResult::MatchedRecord(record) =>
                        record.map(|record|
                            RecordPoint::new(record.key, record.payload.payload.clone())),
                    _ => unreachable!()
                },
        }
    }

    #[inline]
    pub fn append(&self, insert_version: Version, payload: Payload) -> Payload {
        match self {
            VersionIndex::VANILLA(version_list) => version_list
                .append(insert_version, payload),
            VersionIndex::SkipList(skip_list) => {
                let old = skip_list
                    .back()
                    .unwrap();

                if old.value().del_version.get().is_none() {
                    old.value().get_mut().del_version = insert_version.into();
                }
                let _
                    = skip_list.insert(insert_version, SafeCell::new(RecordInfo::new(payload)));

                old.value().payload.clone()
            }
            VersionIndex::SkipListSynced(rw_skip_list) => {
                let skip_list = rw_skip_list.write();
                let old = skip_list
                    .back()
                    .unwrap();

                if old.value().del_version.get().is_none() {
                    old.value().get_mut().del_version = insert_version.into();
                }
                let _
                    = skip_list.insert(insert_version, SafeCell::new(RecordInfo::new(payload)));

                old.value().payload.clone()
            }
            VersionIndex::DexaBTree(tree) => {
                let (_nv, peek)
                    = tree.dispatch(CRUDOperation::PeekMax);

                let mut old =
                    if let CRUDOperationResult::MatchedRecord(Some(entry_record)) = peek {
                        entry_record
                    } else {
                        unreachable!()
                    };

                if old.payload.del_version.get().is_none() {
                    old.payload.del_version = insert_version.into();

                    if let CRUDOperationResult::Updated(..) =
                        tree.dispatch(CRUDOperation::Update(old.key, old.payload.clone())).1
                    { } else { unreachable!() };
                }

                match tree
                    .dispatch(CRUDOperation::Insert(insert_version, RecordInfo::new(payload)))
                    .1
                {
                    CRUDOperationResult::Inserted(..) => old.payload.payload,
                    _ => unreachable!()
                }
            }
        }
    }

    #[inline]
    pub fn delete(&self, del_version: Version) -> Option<Payload> {
        match self {
            VersionIndex::VANILLA(version_list) => version_list
                .delete(del_version),
            VersionIndex::SkipList(skip_list) => {
                let current
                    = skip_list.back().unwrap();

                if *current.key() < del_version && !current
                    .value()
                    .del_version
                    .get()
                    .map(|_| true)
                    .unwrap_or(false)
                {
                    current.value().get_mut().del_version
                        = del_version.into();

                    Some(current.value().payload.clone())
                } else {
                    None
                }
            }
            VersionIndex::SkipListSynced(rw_skip_list) => {
                let skip_list = rw_skip_list.write();
                let current
                    = skip_list.back().unwrap();

                if *current.key() < del_version && !current
                    .value()
                    .del_version
                    .get()
                    .map(|_| true)
                    .unwrap_or(false)
                {
                    current.value().get_mut().del_version
                        = del_version.into();

                    Some(current.value().payload.clone())
                } else {
                    None
                }
            }
            VersionIndex::DexaBTree(tree) => {
                let (_nv, peek)
                    = tree.dispatch(CRUDOperation::PeekMax);

                if let CRUDOperationResult::MatchedRecord(Some(mut entry_record)) = peek {
                    if entry_record.key < del_version && entry_record
                        .payload
                        .del_version
                        .get()
                        .is_none()
                    {
                        entry_record.payload.del_version = del_version.into();
                        match tree.dispatch(CRUDOperation::Update(entry_record.key, entry_record.payload)).1 {
                            CRUDOperationResult::Updated(.., v) =>
                                return Some(v.payload),
                            _ => unreachable!(),
                        }
                    }
                }

                None
            }
        }
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        match self {
            VersionIndex::VANILLA(version_list) =>
                version_list.is_live(),
            VersionIndex::SkipList(skip_list) =>
                skip_list.back().unwrap().value().del_version.get().is_none(),
            VersionIndex::SkipListSynced(skip_list) =>
                skip_list.read().back().unwrap().value().del_version.get().is_none(),
            VersionIndex::DexaBTree(tree) => match tree.dispatch(CRUDOperation::PeekMax).1 {
                CRUDOperationResult::MatchedRecord(Some(record)) =>
                    record.payload.del_version.get().is_none(),
                _ => false
            }
        }
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        match self {
            VersionIndex::VANILLA(version_list) =>
                version_list.newest_payload(),
            VersionIndex::SkipList(skip_list) =>
                skip_list.back().unwrap().value().payload.clone(),
            VersionIndex::SkipListSynced(skip_list) =>
                skip_list.read().back().unwrap().value().payload.clone(),
            VersionIndex::DexaBTree(tree) => match tree.dispatch(CRUDOperation::PeekMax).1 {
                CRUDOperationResult::MatchedRecord(Some(record)) =>
                    record.payload.payload.clone(),
                _ => unreachable!()
            }
        }
    }
}


#[derive(Default, Clone)]
pub struct VersionedEntry<Payload: Clone + Default + Display + Send + Sync + 'static> {
    next: Option<*mut VersionedEntry<Payload>>,
    pub payload: Payload,
    pub insert_version: Version,
    pub del_version: Version,
}

#[derive(Default)]
pub struct AtomicVersionList<Payload: Clone + Default + Display + Sync + Send + 'static> {
    head: AtomicPtr<VersionedEntry<Payload>>,
    // len: AtomicUsize
}

pub struct VersionListIterator<Payload: Clone + Default + Display + Sync + Send + 'static> {
    current: Option<VersionedEntry<Payload>>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Iterator for VersionListIterator<Payload> {
    type Item = VersionedEntry<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.take() {
            Some(version_entry) => {
                self.current = match version_entry.next {
                    Some(next) => unsafe { Some((*next).clone()) },
                    _ => None,
                };

                Some(version_entry)
            }
            _ => None,
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Clone for AtomicVersionList<Payload> {
    fn clone(&self) -> Self {
        let data = self.iter().collect_vec();
        let list = AtomicVersionList {
            head: AtomicPtr::new(null_mut())
        };

        data.into_iter().rev().for_each(|entry| unsafe {
            list.insert_entry(entry.insert_version, entry.del_version, entry.payload);
        });

        list
    }
}

impl<Payload: Clone + Default + Sync + Send + Display + 'static> Drop for AtomicVersionList<Payload> {
    fn drop(&mut self) {
        unsafe {
            let mut curr = self.head.load(Acquire);

            // fence(Acquire);
            while !curr.is_null() {
                let mut curr_ref = Box::from_raw(curr);

                curr = curr_ref.next.take().unwrap_or(null_mut());

                drop(curr_ref);
            }
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicVersionList<Payload> {
    pub fn iter(&self) -> VersionListIterator<Payload> {
        let ptr = self.head.load(Acquire);

        VersionListIterator {
            current: match ptr.is_null() {
                true => None,
                _ => unsafe {
                    // fence(Acquire);

                    Some((*ptr).clone())
                },
            },
        }
    }

    #[inline(always)]
    fn head_ref(&self) -> &VersionedEntry<Payload> {
        let p = unsafe { &*self.head.load(Acquire) };

        // fence(Acquire);
        p
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
        // self.len.load(Acquire)
    }

    // #[inline(always)]
    // pub fn from(insert_version: Version, payload: Payload) -> Self {
    //     Self::new(payload, insert_version)
    // }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: Version, del_version: Option<Version>) -> Self {
        Self {
            head: AtomicPtr::new(Box::into_raw(Box::new(VersionedEntry {
                next: None,
                payload,
                insert_version,
                del_version: del_version.unwrap_or(Version::MAX),
            }))),
            // len: AtomicUsize::new(1)
        }
    }

    #[inline]
    pub fn find(&self, version: Version) -> Option<&VersionedEntry<Payload>> {
        let mut curr = self.head_ref();

        loop {
            if curr.insert_version <= version && curr.del_version > version {
                return Some(curr);
            }

            curr = match curr.next {
                None => break,
                Some(next) => unsafe { &*next },
            };
        }

        None
    }

    #[inline]
    unsafe fn insert_entry(&self, insert_version: Version, del_version: Version, payload: Payload) {
        let head_p
            = self.head.load(Acquire);

        let next
            = if head_p.is_null() { None } else { Some(head_p) };

        let new_head = Box::into_raw(Box::new(VersionedEntry {
            next,
            payload,
            insert_version,
            del_version,
        }));

        self.head.store(new_head, Release);
    }

    #[inline]
    pub fn append(&self, insert_version: Version, payload: Payload) -> Payload {
        let head_p
            = self.head.load(Acquire);

        let head
            = unsafe { &mut *head_p };

        let old_ele = head.payload.clone();

        let new_head = Box::into_raw(Box::new(VersionedEntry {
            next: Some(head_p),
            payload,
            insert_version,
            del_version: Version::MAX,
        }));

        if head.del_version == Version::MAX {
            head.del_version = insert_version;
        }

        self.head.store(new_head, Release);

        old_ele
    }

    #[inline]
    pub fn delete(&self, del_version: Version) -> Option<Payload> {
        let head_p
            = self.head.load(Acquire);

        let head
            = unsafe { &mut *head_p };

        if head.insert_version < del_version && head.del_version == Version::MAX {
            head.del_version = del_version;
            // fence(Release);

            Some(head.payload.clone())
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.head_ref().del_version == Version::MAX
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.head_ref().payload.clone()
    }
}