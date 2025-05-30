use crate::n_test::{Payload, NUM_RECORDS};
use crate::page_model::ObjectCount;
use crossbeam_skiplist::{SkipList, SkipMap};
use std::cell::Cell;
use std::collections::LinkedList;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, IndexMut};
use std::ptr::{null_mut, slice_from_raw_parts_mut, NonNull};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicPtr, AtomicUsize};
use std::sync::Arc;
use std::{mem, ptr, slice};
use CCBPlusTree::crud_model::crud_api::CRUDDispatcher;
use CCBPlusTree::crud_model::crud_operation::CRUDOperation;
use CCBPlusTree::crud_model::crud_operation_result::CRUDOperationResult;
use CCBPlusTree::locking::locking_strategy::{LHL_read, LockingStrategy};
use CCBPlusTree::record_model::record_point::RecordPoint;
use CCBPlusTree::test::{dec_key, inc_key};
use CCBPlusTree::tree;
use CCBPlusTree::tree::bplus_tree::BPlusTree;
// use crate::record_model::record_point::RecordPoint;
use crate::record_model::v_record_point::VersionIndexType;
use crate::record_model::version_info::{DeletedVersionInfo, VersionInfo};
use crate::record_model::Version;
use crate::utils::safe_cell::SafeCell;

pub struct ShadowVec<E: Default + Clone> {
    pub(crate) ptr: *mut E,
    pub(crate) len: SafeCell<usize>,
    pub(crate) update_len: Option<*mut ObjectCount>,
}

impl<E: Default + Clone> ShadowVec<E> {
    pub fn get_unchecked_mut(&self, index: usize) -> &mut E {
        unsafe { &mut *self.ptr.add(index) }
    }

    pub fn get_unchecked(&self, index: usize) -> &E {
        unsafe { &*self.ptr.add(index) }
    }

    pub fn clear(&self) {
        unsafe {
            (&mut *slice_from_raw_parts_mut(self.ptr, *self.len))
                .iter_mut()
                .for_each(|c| ptr::drop_in_place(c));

            *self.len.get_mut() = 0;
        }
    }

    pub fn extend<I>(&self, items: I)
    where
        I: IntoIterator<Item = E>,
    {
        let mut len = *self.len;

        items.into_iter().for_each(|item| unsafe {
            self.ptr.add(len).write(item);

            len += 1;
        });

        *self.len.get_mut() = len
    }

    pub fn pop(&self) -> E {
        let len = *self.len;

        *self.len.get_mut() = len - 1;

        unsafe { self.ptr.add(len - 1).read() }
    }

    pub fn remove(&self, index: usize) -> E {
        unsafe {
            let len = *self.len;

            if index == len - 1 {
                return self.pop();
            }

            let e = self.ptr.add(index).read();

            self.ptr
                .add(index)
                .copy_from(self.ptr.add(index + 1), len - index - 1);

            *self.len.get_mut() = len - 1;

            e
        }
    }

    pub fn push(&self, e: E) {
        unsafe {
            let len = *self.len;

            self.ptr.add(len).write(e);

            *self.len.get_mut() = len + 1
        }
    }

    pub fn insert(&self, index: usize, e: E) {
        unsafe {
            let len = *self.len;

            let p = self.ptr.add(index);

            if index < len {
                ptr::copy(p, p.add(1), len - index);
            }

            p.write(e);

            *self.len.get_mut() = len + 1
        }
    }

    pub fn extend_from_slice(&self, other: &[E]) {
        unsafe {
            let len = *self.len;

            let p = self.ptr.add(len);
            other
                .iter()
                .enumerate()
                .for_each(|(i, e)| p.add(i).write(e.clone()));

            // ptr::copy(other.as_ptr(), self.ptr.add(len), other.len());

            *self.len.get_mut() = len + other.len()
        }
    }
}

impl<E: Default + Clone> Drop for ShadowVec<E> {
    fn drop(&mut self) {
        unsafe {
            if let Some(obj_len_ptr) = self.update_len {
                ptr::write_unaligned(obj_len_ptr, *self.len as _)
                // *self.obj_cnt = self.unreal_vec.len() as _
            }
        }
    }
}

// #[derive(Clone, Default)]
// pub struct VersionList<Payload: Clone + Default>(LinkedList<VTuple<Payload>>);
//
// unsafe impl<Payload: Clone + Default> Sync for VersionList<Payload> {}
//
//
// impl<Payload: Clone + Default> VersionList<Payload> {
//     #[inline(always)]
//     pub fn from(version: Version, payload: Payload) -> Self {
//         Self(LinkedList::from([VTuple {
//             version,
//             del_version: Version::MAX,
//             payload,
//         }]))
//     }
//
//     #[inline(always)]
//     pub fn new(version: Version, payload: Payload) -> Self {
//         Self(LinkedList::from([VTuple {
//             version,
//             del_version: Version::MAX,
//             payload,
//         }]))
//     }
//
//     #[inline(always)]
//     pub fn is_live(&self) -> bool {
//         self.0
//             .front()
//             .map(|l| l.del_version == Version::MAX)
//             .unwrap_or(false)
//     }
//
//     #[inline(always)]
//     pub fn newest_payload(&self) -> Payload {
//         self.0
//             .front()
//             .map(|l| l.payload.clone())
//             .unwrap_or(Payload::default())
//     }
//
//     #[inline(always)]
//     pub fn newest_version(&self) -> Version {
//         self.0
//             .front()
//             .map(|l| l.version)
//             .unwrap_or(Version::MIN)
//     }
//
//     pub fn oldest_payload(&self) -> VTuple<Payload> {
//         let mut curr
//             = &self.0;
//
//         curr.back().unwrap().clone()
//     }
//
//     pub fn find(&self, version: Version) -> Option<&VTuple<Payload>> {
//         self.0.iter()
//            .find(|v| v.version < version)
//            .filter(|v| v.del_version > version)
//     }
//
//     #[inline(always)]
//     pub fn delete(&mut self, del_version: Version) -> Option<Payload> {
//         self.delete_internal(self.newest_version(), del_version)
//     }
//
//     #[inline(always)]
//     fn delete_internal(&mut self, version: Version, del_version: Version) -> Option<Payload> {
//         let item
//             = self.0.front_mut().unwrap();
//
//         if item.version > version && item.del_version > del_version {
//             item.del_version = del_version;
//             Some(item.payload.clone())
//         }
//         else {
//             None
//         }
//     }
//
//     pub fn find_mut(&mut self, version: Version) -> Option<&mut VTuple<Payload>> {
//         self.0
//             .iter_mut()
//             .find(|v| v.version < version && v.del_version > version)
//     }
//
//     pub fn append(&mut self, version: Version, payload: Payload) -> Payload {
//         let old = self.0.front().unwrap().payload.clone();
//         self.0.push_front(VTuple {
//             version,
//             del_version: Version::MAX,
//             payload,
//         });
//
//         old
//     }
//
//     pub fn len(&self) -> usize {
//         self.0.len()
//     }
// }
//
// #[derive(Clone, Default)]
// pub struct VEntryPayload_<Payload: Clone + Default>  {
//     pub entry: VTuple<Payload>,
//     next: Option<Arc<SafeCell<VEntryPayload_<Payload>>>>
// }
//
// impl<Payload: Clone + Default> Deref for VEntryPayload_<Payload> {
//     type Target = VTuple<Payload>;
//     fn deref(&self) -> &Self::Target {
//         &self.entry
//     }
// }
//
// #[derive(Clone, Default)]
// pub struct VTuple<Payload: Clone + Default> {
//     version: Version,
//     pub del_version: Version,
//     pub payload: Payload
// }
//
// impl<Payload: Clone + Default>  VTuple<Payload> {
//     pub fn payload(&self) -> &Payload {
//         &self.payload
//     }
// }
// impl<Payload: Clone + Default> Deref for VTuple<Payload> {
//     type Target = Payload;
//     fn deref(&self) -> &Self::Target {
//         &self.payload
//     }
// }

// const DEL_VERSION_FLAG: Version = 1 << 63;

#[derive(Default, Clone)]
pub struct VersionedEntry<Payload: Clone + Default + Display + Send + Sync + 'static> {
    next: Option<*mut VersionedEntry<Payload>>,
    pub payload: Payload,
    pub insert_version: Version,
    pub del_version: Version,
}

pub const BTREE_V_INDEX_FAN_OUT: usize = NUM_RECORDS / 3;
pub const BTREE_V_INDEX_NUMBER_RECORDS: usize = BTREE_V_INDEX_FAN_OUT * 2;
pub const BTREE_V_INDEX_CONCURRENCY_PROTOCOL: LockingStrategy = LHL_read(4);
pub type InsertVersion = Version;


#[derive(Clone, Default)]
struct RecordInfo<Payload: Clone + Display + Default + Sync + 'static> {
    del_version: DeletedVersionInfo,
    payload: Payload
}

impl<Payload: Clone + Display + Default + Sync + 'static> Display for RecordInfo<Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Deleted version: {}, Payload: {}",
               self.del_version
                   .get()
                   .map(|del| del.to_string())
                   .unwrap_or("*".to_string()),
               self.payload)
    }
}

impl<Payload: Clone + Display + Default + Sync + 'static> RecordInfo<Payload> {
    const fn new(payload: Payload) -> RecordInfo<Payload> {
        Self {
            del_version: DeletedVersionInfo::new_null(),
            payload
        }
    }
}

pub enum VersionIndex<Payload: Clone + Display + Default + Send + Sync + 'static> {
    VANILLA(VersionList<Payload>),
    SkipList(SkipMap<InsertVersion, SafeCell<RecordInfo<Payload>>>),
    DexaBTree(BPlusTree<BTREE_V_INDEX_FAN_OUT, BTREE_V_INDEX_NUMBER_RECORDS, InsertVersion, RecordInfo<Payload>>),
}

impl<Payload: Clone + Default + Display + Send + Sync + 'static> Clone for VersionIndex<Payload> {
    fn clone(&self) -> Self {
        match self {
            VersionIndex::VANILLA(version_list) => VersionIndex::VANILLA(version_list.clone()),
            VersionIndex::SkipList(skip_list) => {
                let mut sk = SkipMap::new();
                for x in skip_list.iter() {
                    sk.insert(*x.key(), SafeCell::new(x.value().get_mut().clone()));
                }

                VersionIndex::SkipList(sk)
            },
            VersionIndex::DexaBTree(tree) => {
                let n_tree
                    = BPlusTree::<BTREE_V_INDEX_FAN_OUT, BTREE_V_INDEX_NUMBER_RECORDS, _, _>::new_with(
                        BTREE_V_INDEX_CONCURRENCY_PROTOCOL,
                        InsertVersion::MIN, InsertVersion::MAX,
                        dec_key, inc_key);

                if let CRUDOperationResult::MatchedRecords(records)
                    = tree.dispatch(CRUDOperation::Range((0..InsertVersion::MAX).into())).1 {

                    for record in records {
                        let _ins
                            = n_tree.dispatch(CRUDOperation::Insert(record.key, record.payload));
                    }
                }
                VersionIndex::DexaBTree(n_tree)
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
            VersionIndex::DexaBTree(tree) =>
                2usize.pow(tree.height() as _) - 1
        }
    }
    #[inline]
    pub fn new(kind: VersionIndexType, payload: Payload, version: Version) -> Self {
        match kind {
            VersionIndexType::VANILLA =>
                Self::VANILLA(VersionList::new(payload, version)),
            VersionIndexType::SkipList =>
                Self::SkipList(SkipMap::from_iter([(version, SafeCell::new(RecordInfo::new(payload)))])),
            VersionIndexType::BTree => Self::DexaBTree(BPlusTree::new_with(
                BTREE_V_INDEX_CONCURRENCY_PROTOCOL,
                Version::MIN,
                Version::MAX,
                inc_key,
                dec_key,
            )),
        }
    }

    #[inline]
    pub fn find(&self, version: Version) -> Option<RecordPoint<Version, Payload>> {
        match self {
            VersionIndex::VANILLA(version_list) => version_list
                .find(version)
                .map(|v_entry|
                    RecordPoint::new(v_entry.insert_version, v_entry.payload.clone())),
            VersionIndex::SkipList(skip_list) => skip_list
                .get(&version)
                .map(|entry|
                    RecordPoint::new(*entry.key(), entry.value().payload.clone())),
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
            VersionIndex::DexaBTree(tree) => match tree.dispatch(CRUDOperation::PeekMax).1 {
                CRUDOperationResult::MatchedRecord(Some(record)) =>
                    record.payload.payload.clone(),
                _ => unreachable!()
            }
        }
    }
}

#[derive(Default)]
pub struct VersionList<Payload: Clone + Default + Display + Sync + Send + 'static> {
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

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Clone for VersionList<Payload> {
    fn clone(&self) -> Self {
        let mut iter = self.iter();

        let list = match iter.next() {
            Some(ele) => VersionList::new(ele.payload, ele.insert_version),
            None => VersionList {
                head: AtomicPtr::new(null_mut()),
                // len: AtomicUsize::new(0),
            },
        };

        while let Some(entry) = iter.next() {
            list.append(entry.insert_version, entry.payload);
        }

        list
    }
}

impl<Payload: Clone + Default + Sync + Send + Display + 'static> Drop for VersionList<Payload> {
    fn drop(&mut self) {
        unsafe {
            let mut curr = self.head.load(Acquire);

            // fence(Acquire);
            while !curr.is_null() {
                let mut curr_ref = Box::from_raw(curr);

                curr = curr_ref.next.take().unwrap_or_else(|| null_mut());

                drop(curr_ref);
            }
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> VersionList<Payload> {
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
    fn head_mut(&self) -> &mut VersionedEntry<Payload> {
        let p = unsafe { &mut *self.head.load(Acquire) };

        // fence(Acquire);
        p
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
        // self.len.load(Acquire)
    }

    #[inline(always)]
    pub fn from(insert_version: Version, payload: Payload) -> Self {
        Self::new(payload, insert_version)
    }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: Version) -> Self {
        Self {
            head: AtomicPtr::new(Box::into_raw(Box::new(VersionedEntry {
                next: None,
                payload,
                insert_version,
                del_version: Version::MAX,
            }))),
            // len: AtomicUsize::new(1)
        }
    }

    #[inline]
    pub fn find(&self, version: Version) -> Option<&VersionedEntry<Payload>> {
        let mut curr = self.head_ref();

        if curr.insert_version <= version && curr.del_version > version {
            return Some(curr);
        }

        while let Some(next_p) = curr.next {
            let next = unsafe { &*next_p };
            if next.insert_version <= version && next.del_version > version {
                return Some(next);
            } else {
                curr = next;
            }
        }

        None
    }

    #[inline]
    pub fn append(&self, insert_version: Version, payload: Payload) -> Payload {
        let head = self.head_mut();

        let old_ele = head.payload.clone();

        let new_head = Box::into_raw(Box::new(VersionedEntry {
            next: Some(head),
            payload,
            insert_version,
            del_version: Version::MAX,
        }));

        if head.del_version == Version::MAX {
            head.del_version = insert_version;
        }

        // fence(Release);
        self.head.store(new_head, Release);
        // self.len.fetch_add(1, Release);

        old_ele
    }

    #[inline]
    pub fn delete(&self, del_version: Version) -> Option<Payload> {
        let head = self.head_mut();

        if head.insert_version < del_version && head.del_version > del_version {
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
