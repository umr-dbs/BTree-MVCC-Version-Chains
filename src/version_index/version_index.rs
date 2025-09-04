use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::Acquire;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::locking::locking_strategy::{LHL_read, LockingStrategy};
use CCBPlusTree::record_model::record_point::RecordPoint;
use CCBPlusTree::test::{dec_key, inc_key};
use CCBPlusTree::tree::bplus_tree::BPlusTree;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use crate::n_test::NUM_RECORDS;
use crate::record_model::v_record_point::{RecordInfo, VersionIndexType};
use crate::version_index::vanilla::AtomicVersionList;
use crate::record_model::Version;
use crate::utils::safe_cell::SafeCell;
use crate::version_index::v_weaver::AtomicVWeaverList;

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

impl<Key: Hash + Ord + Copy + Default, 
    Payload: Default + Clone + Send + Sync + Display + 'static> Into<VersionIndex<Key, Payload>>
for DexaBTree<Payload> {
    fn into(self) -> VersionIndex<Key, Payload> {
        VersionIndex::DexaBTree(AtomicPtrWrapper(AtomicPtr::new(Box::into_raw(Box::new(self)))))
    }
}

pub enum VersionIndex<
    Key: Hash + Ord + Copy + Default, 
    Payload: Clone + Display + Default + Send + Sync + 'static> 
{
    VWEAVER(AtomicVWeaverList<Key, Payload>),
    VANILLA(AtomicVersionList<Payload>),
    SkipList(AtomicPtrWrapper<SkipListImpl<Payload>>),
    SkipListSynced(AtomicPtrWrapper<RwLock<SkipListImpl<Payload>>>),
    DexaBTree(AtomicPtrWrapper<DexaBTree<Payload>>),
}

impl<Key: Hash + Ord + Copy + Default, 
    Payload: Clone + Default + Display + Send + Sync + 'static> Clone for VersionIndex<Key, Payload> {
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
            VersionIndex::VWEAVER(weaver) =>
                VersionIndex::VWEAVER(weaver.clone()) // this is smart clone not the vanilla!
        }
    }
}

impl<Key: Hash + Ord + Copy + Default, 
    Payload: Clone + Default + Display + Send + Sync + 'static> VersionIndex<Key, Payload> {
    pub fn len(&self) -> usize {
        match self {
            VersionIndex::VANILLA(version_list) =>
                version_list.len(),
            VersionIndex::SkipList(skip_list) =>
                skip_list.len(),
            VersionIndex::SkipListSynced(rw_skip_list) =>
                rw_skip_list.read().len(),
            VersionIndex::DexaBTree(tree) =>
                2usize.pow(tree.height() as _) - 1,
            VersionIndex::VWEAVER(weaver) =>
                weaver.len()
        }
    }
    #[inline]
    pub fn new(key: Key, kind: VersionIndexType, payload: Payload, version: Version) -> Self {
        match kind {
            VersionIndexType::VANILLA =>
                Self::VANILLA(AtomicVersionList::new(payload, version, None)),
            VersionIndexType::SkipList =>
                Self::SkipList(SkipMap::from_iter([(version, SafeCell::new(RecordInfo::new(payload)))]).into()),
            VersionIndexType::SkipListSynced =>
                Self::SkipListSynced(
                    SkipMap::from_iter([(version, SafeCell::new(RecordInfo::new(payload)))]).into()),
            VersionIndexType::BTree => new_btree().into(),
            VersionIndexType::VWEAVER =>
                Self::VWEAVER(AtomicVWeaverList::new(key, payload, version))
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
            VersionIndex::VWEAVER(weaver) => weaver
                .find(version)
                .map(|weaver_node|
                    RecordPoint::new(
                        weaver_node.insert_version,
                        weaver_node.payload.as_ref().clone().unwrap()))
        }
    }

    #[inline]
    pub fn push(&self, insert_version: Version, payload: Payload) {
        match self {
            VersionIndex::VANILLA(version_list) => version_list
                .push(insert_version, payload),
            VersionIndex::SkipList(skip_list) => {
                let old = skip_list
                    .back()
                    .unwrap();

                if old.value().del_version.get().is_none() {
                    old.value().get_mut().del_version = insert_version.into();
                }
                let _
                    = skip_list.insert(insert_version, SafeCell::new(RecordInfo::new(payload)));
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
                    CRUDOperationResult::Inserted(..) => { },
                    _ => unreachable!()
                }
            }
            VersionIndex::VWEAVER(weaver) =>
                weaver.push(Some(payload), insert_version)
        };
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
            VersionIndex::VWEAVER(weaver) =>
                weaver.append(Some(payload), insert_version).unwrap()
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
            VersionIndex::VWEAVER(weaver) =>
                weaver.delete(del_version)
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
            },
            VersionIndex::VWEAVER(weaver) =>
                weaver.is_live()
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
            },
            VersionIndex::VWEAVER(weaver) =>
                weaver.newest_payload()
        }
    }
}