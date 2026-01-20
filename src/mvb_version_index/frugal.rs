use std::cell::Cell;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use arc_swap::ArcSwap;
use CCBPlusTree::record_model::record_point::RecordPoint;
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
pub(crate) type FrugalNode<Payload> = Arc<FrugalNodeSt<Payload>>;
// nullable ptr right away
pub(crate) type FrugalNodeLink<Payload> = Option<FrugalNode<Payload>>;
// wrap memory order for arc loaders and posters into a single indirection instead of 2
type FrugalHeadNodeLink<Payload> = ArcSwap<FrugalNodeSt<Payload>>;

const FLAT_LEVEL: TowerLevel = 0; // all linear links
const SENTINEL_LEVEL: TowerLevel = TowerLevel::MAX; // head starter node

#[derive(Default)]
pub struct AtomicFrugalList<
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    head: FrugalHeadNodeLink<Payload>, // We use handshake for arc (not refcount) loaders/posters
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> Clone for AtomicFrugalList<Payload>
{
    fn clone(&self) -> Self {
        AtomicFrugalList {
            head: ArcSwap::new(self.head.load().clone()),
        }
    }
}

#[derive(Default, Clone)]
pub struct FrugalNodeSt<
    Payload: Clone + Default + Display + Send + Sync + 'static>
{
    pub(crate) next: FrugalNodeLink<Payload>, // linear to prev versions
    pub(crate) v_ridgy: FrugalNodeLink<Payload>, // skip to prev versions

    pub(crate) payload: FrugalPayload<Payload>,
    pub(crate) insert_version: Version,
    pub(crate) level: Cell<TowerLevel>
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static> AtomicFrugalList<Payload>
{
    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.head.load().payload.as_ref().clone().unwrap()
    }

    #[inline(always)]
    pub fn is_live(&self) -> bool {
        self.head.load().is_live()
    }

    #[inline(always)]
    pub fn iter(&self) -> FrugalVersionIterator<Payload> {
        FrugalVersionIterator {
            current: Some(self.head.load_full())
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn new(payload: Payload, insert_version: InsertVersion) -> Self {
        Self {
            head: ArcSwap::new(Arc::new(FrugalNodeSt::new(
                Some(payload),
                insert_version,
                SENTINEL_LEVEL // acts as sentinel, i.e., any coin toss matches a v_ridgy eventually
            )))
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
            = self.head.load_full();

        let new_head = Arc::new(FrugalNodeSt::new_with(
            payload,
            insert_version,
            FLAT_LEVEL,
            Some(head.clone()), // next
            Some(head) // v_ridgy
        ));

        self.head.store(new_head);

    }

    #[inline(always)]
    fn append_tower(&self, payload: Option<Payload>, insert_version: InsertVersion) {
        let mut curr
            = self.head.load_full();

        let mut new_tower_node = FrugalNodeSt::new_with(
            payload,
            insert_version,
            curr.level.get() + 1,
            Some(curr.clone()), // next
            None // v_ridgy
        );

        let new_tower_level
            = new_tower_node.level.get();

        while curr.level.get() < new_tower_level {
            curr = match curr.v_ridgy.as_ref() {
                Some(next) => next.clone(),
                None => unreachable!("weaving sentinel never seen!")
            };
        }

        new_tower_node.v_ridgy = Some(curr);
        self.head.store(Arc::new(new_tower_node));
    }

    #[inline(always)]
    pub fn find_from(mut curr: FrugalNode<Payload>,
                     look_up_version: Version,
                     allow_tombstone: bool) -> FrugalNodeLink<Payload>
    {
        while curr.level.get() < SENTINEL_LEVEL && curr.insert_version > look_up_version {
            match curr.v_ridgy.as_ref() {
                Some(v_ridgy) if v_ridgy.insert_version > look_up_version =>
                    curr = v_ridgy.clone(),
                _ => curr = curr.next.as_ref().unwrap().clone(),
            }
        }

        (curr.insert_version <= look_up_version && (!allow_tombstone && curr.is_live() || allow_tombstone))
            .then(move || curr)
    }

    #[inline]
    pub fn find(&self, look_up_version: Version) -> FrugalNodeLink<Payload> {
        Self::find_from(self.head.load_full(), look_up_version, false)
    }

    #[inline]
    pub fn find_any(&self, look_up_version: Version) -> FrugalNodeLink<Payload> {
        Self::find_from(self.head.load_full(), look_up_version, true)
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
                    next: FrugalNodeLink<Payload>,
                    v_ridgy: FrugalNodeLink<Payload>) -> Self
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

    #[inline(always)]
    pub fn find(curr: FrugalNode<Payload>, version: Version, allow_tombstone: bool)
                -> FrugalNodeLink<Payload>
    {
        AtomicFrugalList::find_from(curr, version, allow_tombstone)
    }
}

pub struct FrugalVersionIterator<
   Payload: Clone + Default + Display + Sync + Send + 'static>
{
    current: FrugalNodeLink<Payload>,
}

impl<Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for FrugalVersionIterator<Payload> {
    type Item = FrugalNode<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        loop { // skips tombstones; is that even desired?
            match self.current.take() {
                Some(curr) if curr.is_deleted() =>
                    continue,
                Some(curr) => {
                    self.current = curr.next.clone();
                    break Some(curr)
                }
                _ => break None
            }
        }
    }
}