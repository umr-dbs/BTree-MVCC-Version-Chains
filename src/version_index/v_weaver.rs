use std::cell::Cell;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use arc_swap::ArcSwap;
use crate::block::block::BlockGuard;
use crate::crud_model::crud_api::NodeVisits;
use crate::record_model::Version;
use crate::tree::bplus_tree::BPlusTree;
use crate::utils::safe_cell::SafeCell;
use crate::utils::smart_cell::sched_yield;
use crate::version_index::version_index::InsertVersion;

type TowerLevel = usize;
type WeaverPayload<Payload> = SafeCell<Option<Payload>>; // allows tombstones
// nullable ptr right away
pub(crate) type WeaverNodeLink<Key, Payload> = Option<Arc<VWeaverNode<Key, Payload>>>;
// wrap memory order for arc loaders and posters into a single indirection instead of 2
type WeaverHeadNodeLink<Key, Payload> = ArcSwap<VWeaverNode<Key, Payload>>;

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
pub struct VWeaverNode<
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
            head: ArcSwap::new(Arc::new(VWeaverNode::new(
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

        let new_head = Arc::new(VWeaverNode::new_with(
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

        let mut new_tower_node = VWeaverNode::new_with(
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
                None => unreachable!("weaving sentinel never seen!") // weird; should never happen tho! maybe make it unreachable!(..)
            };
        }

        new_tower_node.v_ridgy = Some(curr);
        self.head.store(Arc::new(new_tower_node));
        old_payload
    }

    #[inline]
    pub fn find(&self, look_up_version: Version) -> WeaverNodeLink<Key, Payload> {
        let mut curr
            = self.head.load_full();

        while curr.level.get() < SENTINEL_LEVEL && curr.insert_version > look_up_version {
            match curr.v_ridgy.as_ref() {
                Some(v_ridgy) if v_ridgy.insert_version > look_up_version =>
                    curr = v_ridgy.clone(),
                _ => curr = curr.next.as_ref().unwrap().clone(),
            }
        }

        (curr.insert_version <= look_up_version && curr.is_live()).then(move || curr)
    }

    #[inline]
    pub fn find_any(&self, look_up_version: Version) -> WeaverNodeLink<Key, Payload> {
        let mut curr
            = self.head.load_full();

        while curr.level.get() < SENTINEL_LEVEL && curr.insert_version > look_up_version {
            match curr.v_ridgy.as_ref() {
                Some(v_ridgy) if v_ridgy.insert_version > look_up_version =>
                    curr = v_ridgy.clone(),
                _ => curr = curr.next.as_ref().unwrap().clone(),
            }
        }

        (curr.insert_version <= look_up_version).then(move || curr)
    }
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static> VWeaverNode<Key, Payload>
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
    type Item = Arc<VWeaverNode<Key, Payload>>;

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

pub struct VWeaverKeyIterator<
    Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static>
{
    current: WeaverNodeLink<Key, Payload>,
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static> VWeaverKeyIterator<Key, Payload>
{
    #[inline(always)]
    pub const fn from(weaver_node: Arc<VWeaverNode<Key, Payload>>) -> Self {
        Self {
            current: Some(weaver_node)
        }
    }
}

impl<Key: Hash + Ord + Copy + Default, Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for VWeaverKeyIterator<Key, Payload>
{
    type Item = Arc<VWeaverNode<Key, Payload>>;

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
    pub(crate) fn v_weaver_k_ridgy_callback(
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

        if current_record_index + 1 < from_leaf.len() {
            let next_weaver_node = unsafe {
                from_leaf.as_records()
                    .get_unchecked(current_record_index + 1)
                    .find_weaver_node_or_tombstone(from_version)
            };

            if next_weaver_node.is_some() {
                *current_weaver_node.k_ridgy.get_mut() = next_weaver_node.clone();
                match current_weaver_node.next.as_ref() {
                    Some(prev_version_current_weaver_node) => {
                        *prev_version_current_weaver_node.k_ridgy.get_mut() = next_weaver_node;
                    },
                    _ => { }
                }
            }
            return 0
        }

        let mut attempts = 0;
        let mut node_visits = 0;
        loop {
            let (next_node_visits, next_leaf_guard)
                = self.traversal_read_olc((self.inc_key)(from_key));

            node_visits += next_node_visits;
            let next_leaf = unsafe {
                next_leaf_guard
                    .deref_unsafe()
                    .unwrap()
            };

            match next_leaf
                .as_records()
                .iter()
                .find_map(|r|
                    (r.key() > from_key).then(|| r.find_weaver_node_or_tombstone(from_version)))
            {
                _ if !next_leaf_guard.is_valid() => { // valid validates only key-movers, not append-ops
                    attempts += 1;
                    sched_yield(attempts);
                    continue
                },
                Some(next_weaver_node) if next_weaver_node.is_some() => {
                    *current_weaver_node.k_ridgy.get_mut() = next_weaver_node.clone();
                    match current_weaver_node.next.as_ref() {
                        Some(prev_version_current_weaver_node) => {
                            *prev_version_current_weaver_node.k_ridgy.get_mut() = next_weaver_node;
                        },
                        _ => { }
                    }
                    break node_visits
                }
                _ => break node_visits
            }
        }
    }
}