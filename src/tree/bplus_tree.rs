use std::hash::Hash;
use std::mem;
use crate::block::block_manager::BlockManager;
use crate::tree::root::Root;
use crate::locking::locking_strategy::{LockingStrategy, LevelExtras};
use crate::page_model::{Attempts, BlockRef, Height, Level, ObjectCount};
use crate::block::block::{Block, BlockGuard};
use crate::test::{dec_key, inc_key};
use crate::utils::un_cell::UnCell;

pub type LockLevel = ObjectCount;

pub const INIT_TREE_HEIGHT: Height = 1;
pub const MAX_TREE_HEIGHT: Height = Height::MAX;

// #[derive(Serialize, Deserialize)]
pub struct BPlusTree<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + 'static,
    Payload: Default + Clone + Sync + 'static
> {
    pub(crate) root: UnCell<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    pub(crate) locking_strategy: LockingStrategy,
    pub(crate) block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) min_key: Key,
    pub(crate) max_key: Key,
    pub(crate) inc_key: fn(Key) -> Key,
    pub(crate) dec_key: fn(Key) -> Key,
}


unsafe impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> Sync for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> Send for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload> {}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Payload: Default + Clone + Sync,
> Default for BPlusTree<FAN_OUT, NUM_RECORDS, u64, Payload> {
    fn default() -> Self {
        BPlusTree::new(
            u64::MIN,
            u64::MAX,
            inc_key,
            dec_key)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync,
    Payload: Default + Clone + Sync
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) fn set_new_root(&self, new_root: Block<FAN_OUT, NUM_RECORDS, Key, Payload>, new_height: Height) {
        self.root.get_mut().height = new_height;

        mem::drop(mem::replace(
            self.root.block.unsafe_borrow_mut(),
            new_root,
        ));
    }

    fn make(block_manager: BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>,
            locking_strategy: LockingStrategy,
            min_key: Key,
            max_key: Key,
            inc_key: fn(Key) -> Key,
            dec_key: fn(Key) -> Key) -> Self
    {
        let empty_node
            = block_manager.new_empty_leaf();

        Self {
            root: UnCell::new(Root::new(
                empty_node.into_cell(locking_strategy.latch_type()),
                INIT_TREE_HEIGHT,
            )),
            locking_strategy,
            block_manager,
            min_key,
            max_key,
            inc_key,
            dec_key,
        }
    }

    pub fn new_with(locking_strategy: LockingStrategy,
                    min_key: Key,
                    max_key: Key,
                    inc_key: fn(Key) -> Key,
                    dec_key: fn(Key) -> Key) -> Self
    {
        Self::make(BlockManager::default(), locking_strategy, min_key, max_key, inc_key, dec_key)
    }

    #[inline(always)]
    pub fn new(min_key: Key, max_key: Key, inc_key: fn(Key) -> Key, dec_key: fn(Key) -> Key) -> Self {
        Self::new_with(LockingStrategy::default(), min_key, max_key, inc_key, dec_key)
    }

    #[inline(always)]
    pub const fn locking_strategy(&self) -> &LockingStrategy {
        &self.locking_strategy
    }

    #[inline(always)]
    pub fn height(&self) -> Height {
        self.root.height()
    }

    #[inline(always)]
    pub(crate) fn lock_reader(&self, node: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
                              -> BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy {
            LockingStrategy::MonoWriter => node.borrow_free(),
            LockingStrategy::LockCoupling => node.borrow_mut(),
            _ => node.borrow_read(),
        }
    }

    #[inline(always)]
    pub(crate) fn lock_reader_olc(&self,
                                  node: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
                                  curr_level: Level,
                                  attempt: Attempts,
                                  height: Height, )
                                  -> BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy() {
            LockingStrategy::MonoWriter => node.borrow_free(),
            LockingStrategy::LockCoupling => node.borrow_mut(),
            LockingStrategy::LightweightHybridLock { read_level, read_attempt, .. } =>
                if *read_level <= 1f32 && (attempt >= *read_attempt || curr_level.is_lock(height, *read_level)) {
                    node.borrow_pin()
                } else {
                    node.borrow_read()
                }
            LockingStrategy::HybridLocking { read_attempt }
            if attempt >= *read_attempt =>
                node.borrow_read_hybrid(),
            _ => node.borrow_read(),
        }
    }

    #[inline]
    pub(crate) fn apply_for_ref(&self,
                                curr_level: Level,
                                max_level: Level,
                                attempt: Attempts,
                                height: Level,
                                block_cc: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ) -> BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        match self.locking_strategy() {
            LockingStrategy::MonoWriter =>
                block_cc.borrow_free(),
            LockingStrategy::LockCoupling =>
                block_cc.borrow_mut(),
            LockingStrategy::ORWC { write_level, write_attempt }
            if curr_level >= height
                || curr_level >= max_level
                || attempt >= *write_attempt
                || curr_level.is_lock(height, *write_level) => block_cc.borrow_mut(),
            LockingStrategy::LightweightHybridLock { write_level, write_attempt, .. }
            if *write_level <= 1f32 &&
                (curr_level >= height
                    || curr_level >= max_level
                    || attempt >= *write_attempt
                    || curr_level.is_lock(height, *write_level)
                ) => block_cc.borrow_pin(),
            LockingStrategy::HybridLocking { .. } if curr_level >= max_level =>
                block_cc.borrow_mut(),
            _ => block_cc.borrow_read()
        }
    }
}