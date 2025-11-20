use std::cell::Cell;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::{atomic, Arc};
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::Release;
use arc_swap::ArcSwap;
use CCBPlusTree::record_model::record_point::RecordPoint;
use crate::block::block::BlockGuard;
use crate::crud_model::crud_api::NodeVisits;
use crate::record_model::Version;
use crate::tree::bplus_tree::BPlusTree;
use crate::utils::interval::Interval;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::sched_yield;
use crate::version_index::version_index::InsertVersion;

type TowerLevel = usize;
type WeaverPayload<Payload> = SafeCell<Option<Payload>>; // allows tombstones
pub(crate) type WeaverNode<Key, Payload> = Arc<VWeaverNodeSt<Key, Payload>>;
// nullable ptr right away
pub(crate) type WeaverNodeLink<Key, Payload> = Option<WeaverNode<Key, Payload>>;
// wrap memory order for arc loaders and posters into a single indirection instead of 2
type WeaverHeadNodeLink<Key, Payload> = ArcSwap<VWeaverNodeSt<Key, Payload>>;

const FLAT_LEVEL: TowerLevel = 0; // all linear links
const SENTINEL_LEVEL: TowerLevel = TowerLevel::MAX; // head starter node

#[derive(Default)]
pub struct AtomicVWeaverList<
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    head: WeaverHeadNodeLink<Key, Payload>, // We use handshake for arc (not refcount) loaders/posters
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static> Clone for AtomicVWeaverList<Key, Payload>
{
    fn clone(&self) -> Self { // shallow clone; check atomicvlists, maybe shallow clone with arcswap
        AtomicVWeaverList {
            head: ArcSwap::new(self.head.load().clone()),
        }
    }
}

#[derive(Default, Clone)]
pub struct VWeaverNodeSt<
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Send + Sync + 'static>
{
    pub(crate) next: WeaverNodeLink<Key, Payload>, // linear to prev versions
    pub(crate) v_ridgy: WeaverNodeLink<Key, Payload>, // skip to prev versions
    pub(crate) k_ridgy: SafeCell<WeaverNodeLink<Key, Payload>>, // linear in key order

    pub(crate) key: Key,
    pub(crate) payload: WeaverPayload<Payload>,
    pub(crate) insert_version: Version,
    pub(crate) level: Cell<TowerLevel>
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static> AtomicVWeaverList<Key, Payload>
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
    pub fn iter(&self) -> VWeaverVersionIterator<Key, Payload> {
        VWeaverVersionIterator {
            current: Some(self.head.load_full())
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline(always)]
    pub fn new(key: Key, payload: Payload, insert_version: InsertVersion) -> Self {
        Self {
            head: ArcSwap::new(Arc::new(VWeaverNodeSt::new(
                key,
                Some(payload),
                insert_version,
                SENTINEL_LEVEL // acts as sentinel, i.e., any coin toss matches a v_ridgy eventually
            )))
        }
    }

    #[inline(always)]
    pub fn delete(&self, insert_version: InsertVersion) -> Option<Payload> {
        if self.head.load().payload.is_some() {
            self.append(None, insert_version)
        }
       else {
           None
       }
    }

    #[inline(always)]
    pub fn push(&self, payload: Option<Payload>, insert_version: InsertVersion) {
        self.append(payload, insert_version);
    }

    #[inline]
    pub fn append(&self, payload: Option<Payload>, insert_version: InsertVersion) -> Option<Payload> {
        const COIN_TOSS_PROBABILITY: f64 = 0.5;

        if rand::random_bool(COIN_TOSS_PROBABILITY) {
            self.append_tower(payload, insert_version)
        }
        else {
            self.append_next(payload, insert_version)
        }
    }

    #[inline(always)]
    fn append_next(&self, payload: Option<Payload>, insert_version: InsertVersion) -> Option<Payload> {
        let head
            = self.head.load_full();

        let old_payload
            = head.payload.as_ref().clone(); // handles tombstones, e.g. insert call

        let new_head = Arc::new(VWeaverNodeSt::new_with(
            head.key,
            payload,
            insert_version,
            FLAT_LEVEL,
            Some(head.clone()), // next
            Some(head) // v_ridgy
        ));

        self.head.store(new_head);
        old_payload
    }

    #[inline(always)]
    fn append_tower(&self, payload: Option<Payload>, insert_version: InsertVersion) -> Option<Payload> {
        let mut curr
            = self.head.load_full();

        let old_payload
            = curr.payload.as_ref().clone(); // handles tombstones, e.g. insert call

        let mut new_tower_node = VWeaverNodeSt::new_with(
            curr.key,
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
        old_payload
    }

    #[inline(always)]
    pub fn find_from(mut curr: WeaverNode<Key, Payload>,
                     look_up_version: Version,
                     allow_tombstone: bool) -> WeaverNodeLink<Key, Payload>
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
    pub fn find(&self, look_up_version: Version) -> WeaverNodeLink<Key, Payload> {
        Self::find_from(self.head.load_full(), look_up_version, false)
    }

    #[inline]
    pub fn find_any(&self, look_up_version: Version) -> WeaverNodeLink<Key, Payload> {
        Self::find_from(self.head.load_full(), look_up_version, true)
    }
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static> VWeaverNodeSt<Key, Payload>
{
    #[inline(always)]
    fn new(key: Key, payload: Option<Payload>, insert_version: InsertVersion, level: TowerLevel) -> Self {
        Self::new_with(key, payload, insert_version, level, None, None)
    }

    #[inline(always)]
    pub fn new_with(key: Key,
                    payload: Option<Payload>,
                    insert_version: InsertVersion,
                    level: TowerLevel,
                    next: WeaverNodeLink<Key, Payload>,
                    v_ridgy: WeaverNodeLink<Key, Payload>) -> Self
    {
        Self {
            next,
            v_ridgy,
            k_ridgy: SafeCell::new(None),
            key,
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
    pub fn find(curr: WeaverNode<Key, Payload>, version: Version, allow_tombstone: bool) 
        -> WeaverNodeLink<Key, Payload> 
    {
        AtomicVWeaverList::find_from(curr, version, allow_tombstone)
    }
}

pub struct VWeaverVersionIterator<
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    current: WeaverNodeLink<Key, Payload>,
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for VWeaverVersionIterator<Key, Payload> {
    type Item = WeaverNode<Key, Payload>;

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

pub struct VWeaverKeyRidgyIterator<
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    current: WeaverNodeLink<Key, Payload>,
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static> VWeaverKeyRidgyIterator<Key, Payload>
{
    #[inline(always)]
    pub const fn from(weaver_node: WeaverNode<Key, Payload>) -> Self {
        Self {
            current: Some(weaver_node)
        }
    }
}

impl<Key: Hash + Ord + Copy + Default, Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for VWeaverKeyRidgyIterator<Key, Payload>
{
    type Item = WeaverNode<Key, Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current.take() {
                Some(weaver_node) => {
                    self.current = weaver_node.k_ridgy.as_ref().clone();

                    match weaver_node.payload.as_ref() {
                        Some(..) => break Some(weaver_node),
                        _ => continue // tombstone
                    }
                }
                _ => break None,
            }
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Send + Sync + Display + 'static
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn weaver_scan_dispatch(&self, mut interval: Interval<Key>, version: Version)
                                       -> (NodeVisits, Vec<RecordPoint<Key, Payload>>)
    {
        debug_assert!(self.v_index_type.is_v_weaver());

        let mut path
            = Vec::with_capacity(self.height() as _);

        let mut node_visits
            = 0;

        let mut result = vec![];
        let mut parent_index = 0;

        loop {
            node_visits +=
                self.next_leaf_page(path.as_mut(), parent_index, interval.lower);

            parent_index
                = path.len().checked_sub(2).unwrap_or(0);

            match path.pop() {
                Some((leaf_fence, leaf_guard)) => unsafe {
                    if let Some(leaf_block) = leaf_guard.deref_unsafe() {
                        match leaf_block
                            .as_records()
                            .iter()
                            .skip_while(|record|
                                !interval.contains(record.key))
                            .find_map(|record|
                                record.find_weaver_node(version))
                        {
                            _ if !leaf_guard.is_valid() => continue,
                            Some(weaver_node) => {
                                let mut last_key = weaver_node.key;
                                VWeaverKeyRidgyIterator::from(weaver_node)
                                    .take_while(|node|
                                        interval.contains(node.key))
                                    .filter_map(|node|
                                        VWeaverNodeSt::find(node, version, false))
                                    .map(|node| RecordPoint::new(
                                        node.key,
                                        node.payload.as_ref().clone().unwrap()))
                                    .for_each(|node| {
                                        last_key = node.key; // for fence clearing
                                        result.push(node);
                                    });

                                interval.lower = (self.inc_key)(last_key)
                            },
                            _ => interval.lower = (self.inc_key)(leaf_fence.upper)
                        }

                        if interval.lower >= interval.upper { // checked after dispatch
                            break
                        }
                    }
                    else {
                        unreachable!("Weaver scan dispatch: Path doesn't contain valid blocks")
                    }
                }
                _ => break // empty index
            }
        }

        (node_visits, result)
    }

    pub(crate) fn weaver_callback( // on_modifiers callback
        &self,
        from_leaf_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        from_key: Key,
        from_version: Version) -> NodeVisits
    {
        debug_assert!(self.v_index_type.is_v_weaver());

        let from_leaf = unsafe {
            from_leaf_guard.deref_unsafe().unwrap()
        };

        let current_record_index = from_leaf
            .as_records()
            .binary_search_by_key(
                &from_key, |r| r.key())
            .unwrap();

        let current_record
            = unsafe { from_leaf.as_records().get_unchecked(current_record_index) };

        let current_weaver_node = current_record
            .find_weaver_node_or_tombstone(from_version)
            .unwrap();

        let current_next_weaver_node = match current_weaver_node.next.as_ref() {
            Some(curr_next) if curr_next.k_ridgy.is_none() => curr_next.clone(),
            _ => return 0,
        };

        if current_record_index + 1 < from_leaf.len() {
            let next_weaver_node = unsafe {
                from_leaf.as_records()
                    .get_unchecked(current_record_index + 1)
                    .find_weaver_node_or_tombstone(from_version)
            };

            if next_weaver_node.is_some() {
                *current_next_weaver_node.k_ridgy.get_mut() = next_weaver_node.clone();
            }
            return 0
        }

        let mut attempts = 0;
        let mut node_visits = 0;

        let mut path = Vec::with_capacity(self.height() as _);
        let mut parent_index = 0;
        'mainL: loop {
            node_visits
                += self.next_leaf_page(path.as_mut(), parent_index, (self.inc_key)(from_key));

            parent_index
                = path.len().checked_sub(2).unwrap_or(0);

            let (_leaf_fence, next_leaf_guard)
                = path.pop().unwrap();

            let next_leaf = {
                let next_leaf_guard = next_leaf_guard
                    .deref();

                if let None = next_leaf_guard {
                    continue 'mainL;
                }
                else {
                    next_leaf_guard.unwrap()
                }
            };

            let neighbour_r = 'neighbour_iterL: loop {
                let records = next_leaf
                    .as_records();

                for r in records.iter() {
                    if r.key > from_key && next_leaf_guard.is_valid() {
                        break 'neighbour_iterL r.find_weaver_node_or_tombstone(from_version)
                    }
                    else if !next_leaf_guard.is_valid() {
                        attempts += 1;
                        sched_yield(attempts);
                        continue 'mainL
                    }
                }

                break None
            };

            match neighbour_r {
                _ if !next_leaf_guard.is_valid() => { // valid validates only key-movers, not append-ops
                    attempts += 1;
                    sched_yield(attempts);
                    continue
                },
                Some(next_weaver_node)  => {
                    *current_next_weaver_node.k_ridgy.get_mut() = Some(next_weaver_node.clone());
                    fence(Release);
                    break node_visits
                }
                _ => break node_visits
            }
        }
    }
}