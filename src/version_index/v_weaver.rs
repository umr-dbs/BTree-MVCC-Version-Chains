use std::cell::Cell;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use arc_swap::ArcSwap;
use crate::record_model::Version;
use crate::utils::safe_cell::SafeCell;
use crate::version_index::version_index::InsertVersion;

type TowerLevel = usize;
type WeaverPayload<Payload> = SafeCell<Option<Payload>>; // allows tombstones
// nullable ptr right away
type WeaverNodeLink<Key, Payload> = Option<Arc<VWeaverNode<Key, Payload>>>;
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

impl<
    Key: Hash + Ord + Copy + Default,
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
    pub(crate) k_ridgy: WeaverNodeLink<Key, Payload>, // linear in key order

    pub(crate) key: Key,
    pub(crate) payload: WeaverPayload<Payload>,
    pub(crate) insert_version: Version,
    pub(crate) level: Cell<TowerLevel>
}

impl<Key: Hash + Ord + Copy + Default,
    Payload: Clone + Default + Display + Sync + Send + 'static> AtomicVWeaverList<Key, Payload>
{
    // assumes caller assures present entry; or default is returned i.e. no panic
    #[inline(always)]
    pub fn newest_payload(&self) -> Payload {
        self.head.load().payload.as_ref().clone().unwrap_or_default()
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
            Some(head.clone()),
            Some(head)
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
            Some(curr.clone()),
            None
        );

        let new_tower_level
            = new_tower_node.level.get();

        while curr.level.get() < new_tower_level {
            curr = match curr.v_ridgy.as_ref() {
                Some(next) => next.clone(),
                None => break
            };
        }

        new_tower_node.v_ridgy = Some(curr);
        self.head.store(Arc::new(new_tower_node));
        old_payload
    }

    #[inline]
    pub fn find(&self, loop_up_version: Version) -> WeaverNodeLink<Key, Payload> {
        let mut curr
            = self.head.load_full();

        while curr.level.get() < SENTINEL_LEVEL && curr.insert_version > loop_up_version {
            match curr.v_ridgy.as_ref() {
                Some(v_ridgy) if v_ridgy.insert_version > loop_up_version =>
                    curr = v_ridgy.clone(),
                _ => curr = curr.next.as_ref().unwrap().clone(),
            }
        }

        (curr.insert_version <= loop_up_version && curr.is_live()).then(move || curr)
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
            k_ridgy: None,
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

impl<Key: Hash + Ord + Copy + Default, Payload: Clone + Default + Display + Sync + Send + 'static>
Iterator for VWeaverKeyIterator<Key, Payload>
{
    type Item = Arc<VWeaverNode<Key, Payload>>;
    // TODO: the k_ridgy is never set tho!
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current.take() {
                Some(weaver_node) => {
                    self.current = weaver_node.k_ridgy.clone();

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