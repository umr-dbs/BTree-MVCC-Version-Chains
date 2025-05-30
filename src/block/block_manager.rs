use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
// use crate::page_model::{AtomicBlockID, BlockID};
use crate::block::block::Block;
use crate::page_model::internal_page::InternalPage;
use crate::page_model::leaf_page::LeafPage;
use crate::page_model::node::Node;
use crate::page_model::{BlockID, ObjectCount};
use crate::utils::smart_cell::{SmartCell, SmartFlavor};
// use crate::tree::settings::BlockSettings;

const ENABLE_SMALL_BLOCK: bool = false;
const MAX_ZEROS_PER_BLOCK: usize = 3964; // = data region in a block // outdated due to omitted block-id

/// Default starting numerical value for a valid BlockID.
// pub const START_BLOCK_ID: BlockID = BlockID::MIN;

pub const _1KB: usize   = 1024;
pub const _2KB: usize   = 2 * _1KB;
pub const _4KB: usize   = 4 * _1KB;
pub const _8KB: usize   = 8 * _1KB;
pub const _16KB: usize  = 16 * _1KB;
pub const _32KB: usize  = 32 * _1KB;

pub const fn bsz_alignment_min<Key, Payload>() -> usize
where Key: Default + Ord + Copy + Hash,
      Payload: Default + Clone + Send + Sync + Display + 'static
{
        mem::align_of::<Arc<()>>() + // ptr size
        mem::align_of::<usize>() + // dispatcher alignment
        mem::size_of::<usize>() * 2 + // arc extras in data area in Tree
        mem::align_of::<Block<0, 0, Key, Payload>>() + // alignment for block
        mem::size_of::<ObjectCount>() + // len indicator
        mem::size_of::<usize>() * 2 + // arc extras in data area
        mem::size_of::<SmartFlavor<()>>() + // align of SmartFlavor = size of empty data
        mem::size_of::<SmartCell<()>>() // align of SmartCell = size of usize
}

pub const fn bsz_alignment<Key, Payload>() -> usize
where Key: Default + Ord + Copy + Hash,
      Payload: Default + Clone + Send + Sync + Display + 'static
{
    bsz_alignment_min::<Key, Payload>() +
        if ENABLE_SMALL_BLOCK {  MAX_ZEROS_PER_BLOCK } else { 0 }
}

pub struct BlockManager<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone + Send + Sync + Display + 'static
> {
    // block_id_counter: AtomicBlockID,
    pub alloc_count: AtomicUsize,
    _marker: PhantomData<(Key, Payload)>
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone + Send + Sync + Display + 'static
> Clone for BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn clone(&self) -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            alloc_count: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }
}

/// Default implementation for BlockManager with default BlockSettings.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone + Send + Sync + Display + 'static
> Default for BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        BlockManager::new()
    }
}

/// Main functionality implementation for BlockManager.
impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone + Send + Sync + Display + 'static
> BlockManager<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    // /// Generates and returns a new atomic (unique across callers) BlockID.
    // #[inline(always)]
    // pub(crate) fn next_block_id(&self) -> BlockID {
    //     self.block_id_counter.fetch_add(1, Ordering::Relaxed)
    // }

    pub fn reset_alloc_reuse_counts(&self) {
        self.alloc_count.store(0, SeqCst);
    }

    #[inline(always)]
    pub const fn allocation_leaf(&self) -> usize {
        NUM_RECORDS
    }

    #[inline(always)]
    pub const fn allocation_directory(&self) -> usize {
        FAN_OUT - 1
    }

    /// Main Constructor requiring supplied BlockSettings.
    #[inline(always)]
    pub(crate) const fn new() -> Self {
        Self {
            // block_id_counter: AtomicBlockID::new(START_BLOCK_ID),
            alloc_count: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn new_empty_leaf(&self) -> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_count.fetch_add(1, Relaxed);

        Block {
            // block_id: self.next_block_id(),
            node_data: Node::Leaf(LeafPage::new())
        }
    }

    /// Crafts a new aligned Index-Block.
    #[inline(always)]
    pub(crate) fn new_empty_index_block(&self) -> Block<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.alloc_count.fetch_add(1, Relaxed);

        Block {
            // block_id: self.next_block_id(),
            node_data: Node::Index(InternalPage::new())
        }
    }
}