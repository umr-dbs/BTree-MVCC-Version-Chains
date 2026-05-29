use std::cell::Cell;
use std::fmt::Display;
use std::hash::Hash;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use arc_swap::ArcSwap;
use CCBPlusTree::record_model::record_point::RecordPoint;
use itertools::Itertools;
use crate::mvb_block::block::BlockGuard;
use crate::mvb_crud_model::crud_api::NodeVisits;
use crate::mvb_record_model::Version;
use crate::mvb_record_model::version_info::VersionInfo;
use crate::mvb_tree::bplus_tree::MVBPlusTree;
use crate::mvb_utils::interval::Interval;
use crate::mvb_utils::safe_cell::SafeCell;
use crate::mvb_utils::smart_cell::sched_yield;
use crate::mvb_version_index::version_index::InsertVersion;

type TowerLevel = usize;
type FrugalPayload<Payload> = SafeCell<Option<Payload>>; // allows tombstones
const FLAT_LEVEL: TowerLevel = 0; // all linear links
const SENTINEL_LEVEL: TowerLevel = TowerLevel::MAX; // head starter node

#[derive(Default)]
pub struct AtomicFrugalList<
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    head: AtomicPtr<FrugalNodeSt<Payload>>, // We use handshake for arc (not refcount) loaders/posters
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Drop for AtomicFrugalList<Payload> {
    fn drop(&mut self) {
        let mut p = self.head.load(Relaxed);
        while !p.is_null() {
            let boxed = unsafe { Box::from_raw(p) };
            p = boxed.next.unwrap_or(null_mut());
            drop(boxed);
        }
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Clone for AtomicFrugalList<Payload>
{
    fn clone(&self) -> Self {
        let head = self.head.load(Acquire);
        if head.is_null() {
            return Self::default();
        }

        // Walk raw chain head → tail to capture every node (including tombstones).
        let mut items: Vec<(Option<Payload>, Version)> = Vec::new();
        let mut p = head;
        while !p.is_null() {
            let node = unsafe { &*p };
            items.push((node.payload.as_ref().clone(), node.insert_version));
            p = node.next.unwrap_or(null_mut());
        }

        // The tail is the original sentinel — it's always live (constructed via `new`).
        let (tail_payload, tail_version) = items.pop().unwrap();
        let list = Self::new(
            tail_payload.expect("sentinel must be live"),
            tail_version,
        );

        // Replay oldest → newest. `push`/`append` prepend at head, so newest ends up there.
        for (payload, version) in items.into_iter().rev() {
            list.append(payload, version);
        }

        list
    }
}

#[derive(Default, Clone)]
pub struct FrugalNodeSt<
    Payload: Clone + Default + Display + Send + Sync + 'static>
{
    pub(crate) next: Option<*mut FrugalNodeSt<Payload>>, // linear to prev versions
    pub(crate) v_ridgy: Option<*mut FrugalNodeSt<Payload>>, // skip to prev versions

    pub(crate) payload: FrugalPayload<Payload>,
    pub(crate) insert_version: Version,
    pub(crate) level: Cell<TowerLevel>
}

unsafe impl<Payload: Clone + Default + Display + Send + Sync + 'static> Send for FrugalNodeSt<Payload> {}
unsafe impl<Payload: Clone + Default + Display + Send + Sync + 'static> Sync for FrugalNodeSt<Payload> {

}
impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicFrugalList<Payload>
{
    #[inline(always)]
    fn load_read(&self) -> &FrugalNodeSt<Payload> {
        unsafe {
            &*self.head.load(Acquire)
        }
    }

    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.load_read().payload.as_ref().clone().unwrap()
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.load_read().is_live()
    }

    #[inline(always)]
    pub fn iter(&self) -> FrugalVersionIterator<Payload> {
        let head = self.head.load(Acquire);
        let current = if head.is_null() {
            None
        } else {
            Some((head, unsafe { (*head).insert_version }))
        };

        FrugalVersionIterator { current }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: InsertVersion) -> Self {
        Self {
            head: AtomicPtr::new(Box::into_raw(Box::new(FrugalNodeSt::new(
                Some(payload),
                insert_version,
                SENTINEL_LEVEL // acts as sentinel, i.e., any coin toss matches a v_ridgy eventually
            ))))
        }
    }

    #[inline(always)]
    pub fn delete(&self, insert_version: InsertVersion) {
        if self.is_live() {
            self.append(None, insert_version)
        }
    }

    #[inline(always)]
    pub fn push(&self, payload: Option<Payload>, insert_version: InsertVersion) {
        self.append(payload, insert_version);
    }

    #[inline]
    pub fn append(&self, payload: Option<Payload>, insert_version: InsertVersion) {
        const COIN_TOSS_PROBABILITY: f64 = 0.5;

        if rand::random_bool(COIN_TOSS_PROBABILITY) {
            self.append_tower(payload, insert_version)
        }
        else {
            self.append_next(payload, insert_version)
        }
    }

    #[inline(always)]
    fn append_next(&self, payload: Option<Payload>, insert_version: InsertVersion) {
        let head
            = self.head.load(Acquire);

        let new_head = Box::into_raw(Box::new(FrugalNodeSt::new_with(
            payload,
            insert_version,
            FLAT_LEVEL,
            Some(head), // next
            Some(head) // v_ridgy
        )));

        self.head.store(new_head, Release);
    }

    #[inline(always)]
    fn append_tower(&self, payload: Option<Payload>, insert_version: InsertVersion) {
        let mut curr
            = self.head.load(Acquire);

        let mut curr_deref = unsafe { &*curr };
        let mut new_tower_node = FrugalNodeSt::new_with(
            payload,
            insert_version,
            curr_deref.level.get() + 1,
            Some(curr), // next
            None // v_ridgy
        );

        let new_tower_level
            = new_tower_node.level.get();

        while curr_deref.level.get() < new_tower_level {
            curr = match curr_deref.v_ridgy {
                Some(next) => next,
                None => unreachable!("weaving sentinel never seen!")
            };
            curr_deref = unsafe { &*curr };
        }

        new_tower_node.v_ridgy = Some(curr);
        self.head.store(Box::into_raw(Box::new(new_tower_node)), Release);
    }

    #[inline(always)]
    pub fn find_from<'a>(curr: Option<*mut FrugalNodeSt<Payload>>,
                         look_up_version: Version,
                         allow_tombstone: bool) -> Option<&'a FrugalNodeSt<Payload>>
    {
        let mut curr_deref = unsafe { &*curr? };
        while curr_deref.level.get() < SENTINEL_LEVEL && curr_deref.insert_version > look_up_version {
            curr_deref = match curr_deref.v_ridgy {
                Some(v_ridgy) if unsafe { (*v_ridgy).insert_version } > look_up_version =>
                    unsafe { &*v_ridgy },
                _ => unsafe { &*curr_deref.next.unwrap() },
            };
        }

        (curr_deref.insert_version <= look_up_version
            && (allow_tombstone || curr_deref.is_live()))
            .then(move || curr_deref)
    }

    #[inline]
    pub fn find<'a>(&self, look_up_version: Version) -> Option<&'a FrugalNodeSt<Payload>> {
        Self::find_from(Some(self.head.load(Acquire)), look_up_version, false)
    }

    #[inline]
    pub fn find_any<'a>(&self, look_up_version: Version) -> Option<&'a FrugalNodeSt<Payload>> {
        Self::find_from(Some(self.head.load(Acquire)), look_up_version, true)
    }
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> FrugalNodeSt<Payload>
{
    #[inline(always)]
    fn new(payload: Option<Payload>, insert_version: InsertVersion, level: TowerLevel) -> Self {
        Self::new_with(payload, insert_version, level, None, None)
    }

    #[inline(always)]
    pub fn new_with(payload: Option<Payload>,
                    insert_version: InsertVersion,
                    level: TowerLevel,
                    next: Option<*mut FrugalNodeSt<Payload>>,
                    v_ridgy: Option<*mut FrugalNodeSt<Payload>>) -> Self
    {
        Self {
            next,
            v_ridgy,
            payload: SafeCell::new(payload),
            insert_version,
            level: Cell::new(level),
        }
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.payload.is_some()
    }

    #[inline(always)]
    pub fn is_deleted(&self) -> bool {
        !self.is_live()
    }

    // #[inline(always)]
    // pub fn find(curr: FrugalNode<Payload>, version: Version, allow_tombstone: bool)
    //             -> FrugalNodeLink<Payload>
    // {
    //     AtomicFrugalList::find_from(curr, version, allow_tombstone)
    // }
}

pub struct FrugalVersionIterator<
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    current: Option<(*mut FrugalNodeSt<Payload>, Version)>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for FrugalVersionIterator<Payload> {
    type Item = (*mut FrugalNodeSt<Payload>, Version);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (curr, version) = self.current?;
            let curr_ref = unsafe { &*curr };
            self.current = curr_ref
                .next
                .map(|n| (n, unsafe { (*n).insert_version }));
            if curr_ref.is_deleted() {
                continue;
            }
            break Some((curr, version));
        }
    }
}