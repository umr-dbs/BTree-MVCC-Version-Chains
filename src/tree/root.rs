use std::fmt::{Display, Formatter};
use std::hash::Hash;
use crate::page_model::{BlockID, BlockRef, Height};

pub const LEVEL_ROOT: Height = 1;

#[derive(Default, Clone)]
pub(crate) struct Root<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> {
    pub(crate) block: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    pub(crate) height: Height
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Send for Root<FAN_OUT, NUM_RECORDS, Key, Payload> { }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Display for Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Root(height={})", self.height())
    }
}

unsafe impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Sync for Root<FAN_OUT, NUM_RECORDS, Key, Payload> { }

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Into<Root<FAN_OUT, NUM_RECORDS, Key, Payload>> for (BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, Height) {
    #[inline(always)]
    fn into(self) -> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
        Root::new(self.0, self.1)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Root<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub(crate) fn new(block: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, height: Height) -> Self {
        Self {
            block,
            height
        }
    }

    #[inline(always)]
    pub(crate) fn block(&self) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.block.clone()
    }

    #[inline(always)]
    pub(crate) const fn height(&self) -> Height {
        self.height
    }

    #[inline(always)]
    pub(crate) fn block_id(&self) -> BlockID {
        self.block.unsafe_borrow().block_id()
    }
}