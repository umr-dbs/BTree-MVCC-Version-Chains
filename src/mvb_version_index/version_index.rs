use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use arc_swap::ArcSwap;
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::locking::locking_strategy::{LHL_read, LockingStrategy};
use CCBPlusTree::record_model::record_point::RecordPoint;
use CCBPlusTree::test::{dec_key, inc_key};
use CCBPlusTree::tree::bplus_tree::BPlusTree;
use crossbeam_skiplist::SkipMap;
use crate::n_test::NUM_RECORDS;
use crate::mvb_record_model::v_record_point::{RecordInfo, VersionIndexType};
use crate::mvb_version_index::vanilla::AtomicVersionList;
use crate::mvb_record_model::Version;
use crate::mvb_record_model::version_info::VersionInfo;
use crate::mvb_utils::safe_cell::SafeCell;
use crate::mvb_version_index::frugal::AtomicFrugalList;
use crate::mvb_version_index::v_weaver::{AtomicVWeaverList, WeaverNodeLink};

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

impl<Key: Hash + Ord + Copy + Default, 
    Payload: Default + Clone + Send + Sync + Display + 'static> Into<VersionIndex<Key, Payload>>
for DexaBTree<Payload> {
    fn into(self) -> VersionIndex<Key, Payload> {
        VersionIndex::DexaBTree(ArcSwap::new(Arc::new(self)))
    }
}

pub enum VersionIndex<
    Key: Hash + Ord + Copy + Default, 
    Payload: Clone + Display + Default + Send + Sync + 'static> 
{
    FrugalSkipList(AtomicFrugalList<Payload>),
    VWEAVER(AtomicVWeaverList<Key, Payload>),
    VANILLA(AtomicVersionList<Payload>),
    SkipList(Arc<SkipListImpl<Payload>>),
    DexaBTree(ArcSwap<DexaBTree<Payload>>),
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Send + Sync + 'static> Clone for VersionIndex<Key, Payload> {
    fn clone(&self) -> Self {
        match self {
            VersionIndex::VANILLA(version_list) =>
                VersionIndex::VANILLA(version_list.clone()),
            VersionIndex::SkipList(skip_list) =>
                VersionIndex::SkipList(skip_list.clone()),
            VersionIndex::FrugalSkipList(fg) =>
                VersionIndex::FrugalSkipList(fg.clone()),
            VersionIndex::DexaBTree(tree) =>
                VersionIndex::DexaBTree(ArcSwap::new(tree.load_full())),
            VersionIndex::VWEAVER(weaver) =>
                VersionIndex::VWEAVER(weaver.clone())
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
            VersionIndex::FrugalSkipList(fg) =>
                fg.len(),
            VersionIndex::DexaBTree(tree) =>
                2usize.pow(tree.load().height() as _) - 1,
            VersionIndex::VWEAVER(weaver) =>
                weaver.len()
        }
    }
    #[inline]
    pub fn new(key: Key, kind: VersionIndexType, payload: Payload, version: Version) -> Self {
        match kind {
            VersionIndexType::FrugalSkipList =>
                Self::FrugalSkipList(AtomicFrugalList::new(payload, version)),
            VersionIndexType::VANILLA =>
                Self::VANILLA(AtomicVersionList::new(payload, version, None)),
            VersionIndexType::SkipList =>
                Self::SkipList(Arc::new(
                    SkipMap::from_iter([(version, SafeCell::new(RecordInfo::new(payload)))]))),
            VersionIndexType::BTree => new_btree().into(),
            VersionIndexType::VWEAVER =>
                Self::VWEAVER(AtomicVWeaverList::new(key, payload, version))
        }
    }

    #[inline(always)]
    pub(crate) fn find_weaver_node(&self, version: Version) -> WeaverNodeLink<Key, Payload> {
        if let VersionIndex::VWEAVER(weaver) = self  {
            weaver.find(version)
        }
        else {
            None
        }
    }

    #[inline(always)]
    pub(crate) fn find_weaver_node_or_tombstone(&self, version: Version) -> WeaverNodeLink<Key, Payload> {
        if let VersionIndex::VWEAVER(weaver) = self  {
            weaver.find_any(version)
        }
        else {
            None
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
            VersionIndex::DexaBTree(tree) =>
                match tree.load().dispatch(CRUDOperation::Point(version)).1 {
                    CRUDOperationResult::MatchedRecord(record) =>
                        record.map(|record|
                            RecordPoint::new(record.key, record.payload.payload.clone())),
                    _ => unreachable!()
                },
            VersionIndex::FrugalSkipList(fg) => fg
                .find(version)
                .map(|frugal_node|
                    RecordPoint::new(
                        frugal_node.insert_version,
                        frugal_node.payload.as_ref().clone().unwrap())),
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
            VersionIndex::FrugalSkipList(fg) => fg
                .push(Some(payload), insert_version),
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
            VersionIndex::DexaBTree(tree) => {
                let tree
                    = tree.load();

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
    pub fn append(&self, insert_version: Version, payload: Payload) {
        match self {
            VersionIndex::FrugalSkipList(fg) => fg
                .append(Some(payload), insert_version),
            VersionIndex::VANILLA(version_list) => version_list
                .append(insert_version, payload),
            VersionIndex::SkipList(skip_list) => {
                let old = skip_list
                    .back()
                    .unwrap();

                if old.value().del_version.get().is_none() {
                    old.value().get_mut().del_version = insert_version.into();
                    let _ = skip_list
                        .insert(insert_version, SafeCell::new(RecordInfo::new(payload)));
                }
            }
            VersionIndex::DexaBTree(tree) => {
                let tree
                    = tree.load();

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
                weaver.append(Some(payload), insert_version)
        }
    }

    #[inline]
    pub fn delete(&self, del_version: Version) {
        match self {
            VersionIndex::FrugalSkipList(fg) => fg
                .delete(del_version),
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
                }
            }
            VersionIndex::DexaBTree(tree) => {
                let tree
                    = tree.load();

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
                            CRUDOperationResult::Updated(..) => return,
                            _ => unreachable!(),
                        }
                    }
                }
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
            VersionIndex::FrugalSkipList(fg) =>
                fg.is_live(),
            VersionIndex::DexaBTree(tree) =>
                match tree.load().dispatch(CRUDOperation::PeekMax).1
            {
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
            VersionIndex::FrugalSkipList(fg) =>
                fg.newest_payload(),
            VersionIndex::VANILLA(version_list) =>
                version_list.newest_payload(),
            VersionIndex::SkipList(skip_list) =>
                skip_list.back().unwrap().value().payload.clone(),
            VersionIndex::DexaBTree(tree) =>
                match tree.load().dispatch(CRUDOperation::PeekMax).1
                {
                    CRUDOperationResult::MatchedRecord(Some(record)) =>
                        record.payload.payload.clone(),
                    _ => unreachable!()
            },
            VersionIndex::VWEAVER(weaver) =>
                weaver.newest_payload()
        }
    }
}