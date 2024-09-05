use std::collections::VecDeque;
use std::hash::Hash;
use std::fmt::Display;
use std::{mem, ptr};
use itertools::Itertools;
use crate::locking::locking_strategy::LockingStrategy;
use crate::page_model::{Attempts, BlockRef, Height, Level};
use crate::block::block::{Block, BlockGuard};
use crate::crud_model::crud_api::NodeVisits;
use crate::page_model::node::{Node, NodeUnsafeDegree};
use crate::tree::bplus_tree::{BPlusTree, INIT_TREE_HEIGHT, LockLevel, MAX_TREE_HEIGHT};
use crate::utils::interval::Interval;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display + 'static,
    Payload: Default + Clone + Sync + 'static
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub(crate) fn has_overflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
        match node.is_leaf() {
            true => node.is_overflow(self.block_manager.allocation_leaf()),
            false => node.is_overflow(self.block_manager.allocation_directory())
        }
    }

    pub(crate) fn has_underflow(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key, Payload>) -> bool {
        match node.is_leaf() {
            true => node.is_underflow(self.block_manager.allocation_leaf() - 1),
            false => node.is_underflow(self.block_manager.allocation_directory())
        }
    }

    fn unsafe_degree_of(&self, node: &Node<FAN_OUT, NUM_RECORDS, Key, Payload>) -> NodeUnsafeDegree {
        match node.is_leaf() {
            true => node.unsafe_degree(self.block_manager.allocation_leaf()),
            false => node.unsafe_degree(self.block_manager.allocation_directory()),
        }
    }

    #[inline]
    pub(crate) fn retrieve_root(&self, mut lock_level: Level, mut attempt: Attempts)
                                -> (NodeVisits, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Height, LockLevel, Attempts)
    {
        let mut node_visits = 0;
        loop {
            match self.retrieve_root_internal(lock_level, attempt) {
                Err((n_lock_level, n_attempt)) => {
                    lock_level = n_lock_level;
                    attempt = n_attempt;
                    node_visits += 1;
                }
                Ok((guard, height)) =>
                    break (node_visits + 1, guard, height, lock_level, attempt)
            }
        }
    }

    fn merge(&self,
             block_guard: &mut BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
             from_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
             merge_guard: Option<(usize, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)>,
             child_pos: usize,
             child_key: Key,
             curr_level: Level,
             lock_level: LockLevel,
             attempt: Attempts,
    ) -> Result<(), ()> {
        // unsafe {
        //     if ptr::read_unaligned(&child_key as *const _ as *const u64) == 246 {
        //         println!("FROM ROOT");
        //         Self::log_console(self.root.block.unsafe_borrow(), 0);
        //         println!("FROM BLOCKGUARD");
        //         Self::log_console(BlockGuard::deref(block_guard).unwrap(), 0);
        //         let s = "sfsd".to_string();
        //     }
        // }
        let height
            = self.root.height();

        if !block_guard.upgrade_write_lock() {
            return Err(());
        }

        let block_deref
            = block_guard.deref_mut().unwrap();

        let mut children_latched = block_deref
            .children()
            .iter()
            .enumerate()
            .filter(|(index, ..)| *index != child_pos)
            .filter(|(index, ..)| match merge_guard {
                Some((merge_pos, ..)) => *index != merge_pos,
                _ => true
            })
            .map(|(.., c)| self.apply_for_ref(curr_level, lock_level, attempt, height, c))
            .collect_vec();
        
        if children_latched.is_empty() {
            children_latched.push(from_guard);
        } else {
            children_latched.insert(child_pos, from_guard);
        }
        // *children_latched.get_unchecked_mut(child_pos) = from_guard;

        if let Some((merge_pos, merge_guard)) = merge_guard {
            children_latched.insert(merge_pos, merge_guard);
            // *children_latched.get_unchecked_mut(merge_pos) = merge_guard;
        }
        
        if children_latched.iter_mut().any(|guard|
            !guard.upgrade_write_lock())
        {
            mem::drop(children_latched);
            return Err(());
        }
        // println!("FROM ROOT");
        // Self::log_console(self.root.block.unsafe_borrow(), 0);
        // println!("FROM BLOCKGUARD");
        // Self::log_console(BlockGuard::deref(block_guard).unwrap(), 0);

        let is_leaf_children = unsafe { children_latched.get_unchecked(0).deref_unsafe().unwrap() }
            .is_leaf();

        let block = match is_leaf_children {
            true => {
                let n_leaf
                    = self.block_manager.new_empty_leaf();
                // println!("FROM ROOT");
                // Self::log_console(self.root.block.unsafe_borrow(), 0);
                // println!("FROM BLOCKGUARD");
                // Self::log_console(BlockGuard::deref(block_guard).unwrap(), 0);
                children_latched
                    .into_iter()
                    .for_each(|leaf_guard|
                        n_leaf.records_mut().extend_from_slice(leaf_guard.deref().unwrap().as_records()));

                n_leaf
            }
            false => {
                let n_internal
                    = self.block_manager.new_empty_index_block();

                children_latched
                    .into_iter()
                    .enumerate()
                    .for_each(|(index, internal_guard)| {
                        let page = internal_guard.deref().unwrap();
                        let keys = page.keys();
                        let children = page.children();

                        n_internal.children_mut().extend_from_slice(children);

                        if index == child_pos + 1 {
                            n_internal.keys_mut().push(child_key)
                        }
                        n_internal.keys_mut().extend_from_slice(keys);
                    });

                n_internal
            }
        };

        if ptr::addr_eq(self.root.block.unsafe_borrow() as *const _,
                        BlockGuard::deref(block_guard).unwrap() as *const _)
        {
            self.root.get_mut().height -= 1;
        } else {
            unreachable!("Merge non-root into lower height")
        }

        *block_guard.deref_mut().unwrap() = block;

        // println!("AFTER MERGE BLOCKGUARD");
        // Self::log_console(BlockGuard::deref(block_guard).unwrap(), 0);
        Ok(())
    }

    #[inline]
    pub(crate) fn retrieve_root_internal(&self, lock_level: LockLevel, attempt: Attempts)
                                         -> Result<(BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Height), (LockLevel, Attempts)>
    {
        let root
            = self.root.get();

        let mut root_guard =
            self.apply_for_ref(INIT_TREE_HEIGHT,
                               lock_level,
                               attempt,
                               root.height(),
                               &root.block);

        if !root_guard.is_valid() {
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        let has_overflow_root
            = self.has_overflow(unsafe { root_guard.deref_unsafe() }.unwrap());

        let force_restart = match self.locking_strategy {
            LockingStrategy::MonoWriter | LockingStrategy::LockCoupling => false,
            _ => !root_guard.is_write_lock()
        };

        if force_restart && has_overflow_root && !root_guard.upgrade_write_lock() { // !root_guard.is_valid() ||
            mem::drop(root_guard);

            return Err((lock_level, attempt + 1));
        }

        if has_overflow_root && self.locking_strategy.is_orwc() &&
            !self.has_overflow(root_guard.deref().unwrap())
        { // Detect interferences
            // if !root_guard.deref().unwrap().is_leaf() {
            //     root_guard.downgrade(); // allow possible concurrency, instead of definitive lock
            // }
            return Ok((root_guard, root.height()));
        }

        if !has_overflow_root {
            return Ok((root_guard, root.height()));
        }

        let root_ref
            = root_guard.deref_mut().unwrap();

        let latch_type
            = self.locking_strategy.latch_type();

        let n_height
            = root.height() + 1;

        match root_ref.as_mut() {
            Node::Index(index_page) => unsafe {
                let keys = index_page.keys();
                let children = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys.get_unchecked(keys_mid);

                let index_block
                    = self.block_manager.new_empty_index_block();

                let new_node_right =
                    self.block_manager.new_empty_index_block();

                let new_root_left
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(keys_mid + 1..));

                new_node_right
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(keys_mid + 1..));

                new_root_left
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(..=keys_mid));

                new_root_left
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(..keys_mid));

                index_block.children_mut().extend([
                    new_root_left.into_cell(latch_type),
                    new_node_right.into_cell(latch_type)
                ]);

                index_block.keys_mut()
                    .push(k3);

                self.set_new_root(
                    index_block,
                    n_height);
            }
            Node::Leaf(records) => unsafe {
                let records
                    = records.as_records();

                let records_mid
                    = records.len() / 2;

                let k3 = records
                    .get_unchecked(records_mid)
                    .key;

                let new_node_right
                    = self.block_manager.new_empty_leaf();

                let new_node_left
                    = self.block_manager.new_empty_leaf();

                let new_root
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));

                new_node_left
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

                new_root.children_mut().extend([
                    new_node_left.into_cell(latch_type),
                    new_node_right.into_cell(latch_type)
                ]);

                new_root.keys_mut()
                    .push(k3);

                self.set_new_root(
                    new_root,
                    n_height);
            }
        }

        Ok((root_guard, n_height))
    }

    pub(crate) fn log_console(mufasa: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>, rec: usize) {
        if let Node::Index(internal_page) = mufasa.as_ref() {
            println!("{}Keys: [{}]", "\t".repeat(rec), internal_page
                .keys()
                .iter()
                .map(|k| k as *const _ as *const u64)
                .map(|k| unsafe { k.read_unaligned() })
                .join(","));
        }
        match mufasa.as_ref() {
            Node::Index(internal_page) => internal_page
                .children()
                .iter()
                .enumerate()
                .for_each(|(index, c)| {
                    print!("{}Child-{index}\n\t", "\t".repeat(rec + 1));
                    Self::log_console(c.unsafe_borrow(), rec + 1)
                }),
            Node::Leaf(leaf_page) => println!("{}Leaf = [{}]", "\t".repeat(rec), leaf_page
                .as_records()
                .iter()
                .map(|r| r.key())
                .join(", "))
        }
    }

    pub(crate) fn do_underflow_correction(
        &self,
        fence: &Interval<Key>,
        curr_level: Level,
        attempts: Attempts,
        lock_level: LockLevel,
        parent_guard: &mut BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        mut child_pos: usize,
        from_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)
        -> Result<(), ()>
    {
        let mufasa = parent_guard
            .deref_mut()
            .unwrap();

        let child_key = unsafe { *mufasa.keys().get_unchecked(child_pos) };

        let from_deref
            = from_guard.deref_mut().unwrap();

        let is_leaf
            = from_deref.is_leaf();

        let mut all_candidates = mufasa
            .children()
            .iter()
            .zip(mufasa.keys().iter().merge([&fence.upper]))
            .enumerate()
            .filter(|(index, ..)| *index != child_pos)
            .map(|(index, (block, key))|
                (index, block.clone(), *key))
            // .sorted_by_key(|(.., key)| *key)
            .collect_vec();

        let (mut merge_index,
            _merge_block,
            // mut merge_guard,
            merge_key
        ) = match all_candidates
            .binary_search_by_key(&child_key, |(.., key)| *key)
        {
            _ if all_candidates.len() == 1 && !is_leaf => { // copy all to mufasa
                mem::drop(all_candidates);
                return self.merge(parent_guard, from_guard, None, child_pos, child_key, curr_level, lock_level, attempts);
            }
            Ok(index) | Err(index) => if index < all_candidates.len() {
                all_candidates.remove(index)
            } else if !all_candidates.is_empty() && index == all_candidates.len() {
                all_candidates.pop().unwrap()
            } else {
                mem::drop(all_candidates);
                return self.merge(parent_guard, from_guard, None, child_pos, child_key, curr_level, lock_level, attempts);
            }
            // Err(..) if !all_candidates.is_empty() && !is_leaf => all_candidates.pop().unwrap(),
            // r => {
            //     println!("{:?}", r);
            //     // Self::log_console(self.root.block.unsafe_borrow(), 1);
            //     // println!("MUFASA");
            //     // Self::log_console(mufasa, 1);
            //     mem::drop(all_candidates);
            //     return self.merge(parent_guard, from_guard, None, child_pos, child_key, curr_level, lock_level, attempts);
            // }
        };

        all_candidates.clear();

        let merge_guard
            = _merge_block.borrow_mut();

        if !merge_guard.is_valid() { // OLC
            return Err(());
        }

        let merge_deref
            = merge_guard.deref().unwrap();

        if is_leaf {
            let fit
                = from_deref.len() + merge_deref.len() < NUM_RECORDS;

            if mufasa.len() == 1 && fit {
                return self.merge(parent_guard, from_guard, Some((merge_index, merge_guard)), child_pos, child_key, curr_level, lock_level, attempts);
            }
            if fit { // merge into one new leaf; checked
                // println!("Before Leaf Merge");
                // Self::log_console(mufasa, 0);
                let new_leaf = self.block_manager
                    .new_empty_leaf()
                    .into_cell(self.locking_strategy.latch_type());

                new_leaf
                    .unsafe_borrow_mut()
                    .records_mut()
                    .extend(from_deref
                        .as_records()
                        .iter()
                        .merge_by(merge_deref.as_records(),
                                  |r0, r1|
                                      r0.key() < r1.key())
                        .cloned());

                let mufasa_children_mut
                    = mufasa.children_mut();

                let mufasa_keys_mut
                    = mufasa.keys_mut();

                let t_child = child_pos;
                child_pos = child_pos.min(merge_index);
                merge_index = merge_index.max(t_child);

                // merge_key = merge_key.min(child_key);

                // Self::log_console(mufasa, 0);
                mufasa_keys_mut.remove(child_pos);
                mufasa_children_mut.remove(merge_index);

                *mufasa_children_mut.get_unchecked_mut(child_pos) = new_leaf;

                mem::drop(mufasa_keys_mut);
                mem::drop(mufasa_children_mut);

                // Self::log_console(mufasa, 0);

                // println!("After Leaf Merge");
                // Self::log_console(mufasa, 0);
            } else { // Leaf: key-split
                // Self::log_console(mufasa, 0);
                let new_leaf_left = self.block_manager
                    .new_empty_leaf()
                    .into_cell(self.locking_strategy.latch_type());

                let new_leaf_right = self.block_manager
                    .new_empty_leaf()
                    .into_cell(self.locking_strategy.latch_type());

                let joined = from_deref
                    .as_records()
                    .iter()
                    .merge_by(merge_deref.as_records(),
                              |r0, r1|
                                  r0.key() < r1.key())
                    .cloned()
                    .collect_vec();

                let joined_len = joined.len();
                let (first, second)
                    = joined.split_at(joined_len / 2);

                let left_key = unsafe {
                    second.get_unchecked(0).key()
                };

                let right_key
                    = merge_key.max(child_key);

                new_leaf_left.unsafe_borrow_mut()
                    .records_mut()
                    .extend_from_slice(first);

                new_leaf_right.unsafe_borrow_mut()
                    .records_mut()
                    .extend_from_slice(second);

                mem::drop(joined);

                let mufasa_children_mut
                    = mufasa.children_mut();

                let mufasa_keys_mut
                    = mufasa.keys_mut();

                let t_child_pos = child_pos;
                let child_pos = child_pos.min(merge_index);
                let merge_index = t_child_pos.max(merge_index);

                *mufasa_keys_mut.get_unchecked_mut(child_pos) = left_key;
                // *mufasa_keys_mut.get_unchecked_mut(merge_index) = right_key;
                *mufasa_children_mut.get_unchecked_mut(child_pos) = new_leaf_left;
                *mufasa_children_mut.get_unchecked_mut(merge_index) = new_leaf_right;

                // Self::log_console(mufasa, 0);
            }
        } else { // is Internal page: Combine
            if from_deref.len() + merge_deref.len() < FAN_OUT - 1 {
                // Self::log_console(mufasa, 0);

                let new_index = self.block_manager
                    .new_empty_index_block()
                    .into_cell(self.locking_strategy.latch_type());

                let keys_mut = new_index
                    .unsafe_borrow_mut()
                    .keys_mut();

                let children_mut = new_index
                    .unsafe_borrow_mut()
                    .children_mut();

                let (child_pos, merge_index) = if child_pos < merge_index {
                    children_mut.extend_from_slice(from_deref.children());
                    children_mut.extend_from_slice(merge_deref.children());

                    keys_mut.extend_from_slice(from_deref.keys());
                    keys_mut.push(child_key);
                    keys_mut.extend_from_slice(merge_deref.keys());

                    (child_pos, merge_index)
                } else {
                    children_mut.extend_from_slice(merge_deref.children());
                    children_mut.extend_from_slice(from_deref.children());

                    keys_mut.extend_from_slice(merge_deref.keys());
                    keys_mut.push(merge_key);
                    keys_mut.extend_from_slice(from_deref.keys());

                    (merge_index, child_pos)
                };

                let mufasa_children_mut
                    = mufasa.children_mut();

                let mufasa_keys_mut
                    = mufasa.keys_mut();

                mufasa_children_mut.remove(merge_index);
                mufasa_keys_mut.remove(child_pos);

                mem::drop(keys_mut);
                mem::drop(children_mut);

                *mufasa_children_mut.get_unchecked_mut(child_pos) = new_index;

                mem::drop(mufasa_keys_mut);
                mem::drop(mufasa_children_mut);
                // println!("After Internal Merge");
                // Self::log_console(mufasa, 0);
                // let s = "adasda".to_string();
            } else { // key-split: Internal Page
                // println!("aaaaa");
                let new_internal_left = self.block_manager
                    .new_empty_index_block()
                    .into_cell(self.locking_strategy.latch_type());

                let new_internal_right = self.block_manager
                    .new_empty_index_block()
                    .into_cell(self.locking_strategy.latch_type());

                let (child_pos, merge_index, joined_keys, joined_children) = if child_pos < merge_index
                {
                    (child_pos, merge_index,
                     from_deref
                         .keys()
                         .iter()
                         .merge(merge_deref.keys())
                         .cloned()
                         .collect_vec(),
                     from_deref
                         .children()
                         .iter()
                         .merge_by(merge_deref.children(), |_, _| true)
                         .cloned()
                         .collect_vec())
                } else {
                    (merge_index, child_pos,
                     merge_deref
                         .keys()
                         .iter()
                         .merge(from_deref.keys())
                         .cloned()
                         .collect_vec(),
                     merge_deref
                         .children()
                         .iter()
                         .merge_by(from_deref.children(), |_, _| true)
                         .cloned()
                         .collect_vec())
                };

                let keys_joined
                    = joined_keys.len();

                let (keys_left, keys_right)
                    = joined_keys.split_at(keys_joined / 2);

                let split_key
                    = unsafe { *keys_right.get_unchecked(0) };

                let keys_right
                    = unsafe { keys_right.get_unchecked(1..) };

                let children_left =
                    unsafe { joined_children.get_unchecked(..=keys_left.len()) };

                let children_right =
                    unsafe { joined_children.get_unchecked(keys_left.len() + 1..) };

                new_internal_left
                    .unsafe_borrow_mut()
                    .children_mut()
                    .extend_from_slice(children_left);

                new_internal_left.unsafe_borrow_mut()
                    .keys_mut()
                    .extend_from_slice(keys_left);

                new_internal_right
                    .unsafe_borrow_mut()
                    .children_mut()
                    .extend_from_slice(children_right);

                new_internal_right.unsafe_borrow_mut()
                    .keys_mut()
                    .extend_from_slice(keys_right);

                *mufasa.keys_mut().get_unchecked_mut(merge_index) = split_key;
                *mufasa.children_mut().get_unchecked_mut(child_pos) = new_internal_left;
                *mufasa.children_mut().get_unchecked_mut(merge_index) = new_internal_right;
            }
        }
        // Self::log_console(mufasa, 0);
        Ok(())
    }

    pub(crate) fn do_overflow_correction(
        &self,
        parent_guard: &mut BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_pos: usize,
        mut from_guard: BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>)
    {
        from_guard.mark_obsolete();

        let latch_type
            = self.locking_strategy.latch_type();

        match from_guard.deref_mut().unwrap().as_mut() {
            Node::Index(index_page) => unsafe {
                let keys
                    = index_page.keys();

                let children
                    = index_page.children();

                let keys_mid = keys.len() / 2;
                let k3 = *keys
                    .get_unchecked(keys_mid);

                let new_node_right
                    = self.block_manager.new_empty_index_block();

                let new_node_from
                    = self.block_manager.new_empty_index_block();

                new_node_right
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(keys_mid + 1..));

                new_node_right
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(keys_mid + 1..));

                new_node_from
                    .children_mut()
                    .extend_from_slice(children.get_unchecked(..=keys_mid));

                new_node_from
                    .keys_mut()
                    .extend_from_slice(keys.get_unchecked(..keys_mid));

                let parent_mut = parent_guard
                    .deref_mut()
                    .unwrap();

                let mut parent_children
                    = parent_mut.children_mut();

                parent_children
                    .insert(child_pos + 1, new_node_right.into_cell(latch_type));

                mem::drop(mem::replace(parent_children.get_unchecked_mut(child_pos),
                                       new_node_from.into_cell(latch_type)));

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);
            }
            Node::Leaf(records) => unsafe {
                let records
                    = records.as_records();

                let records_mid = records.len() / 2;
                let k3 = records
                    .get_unchecked(records_mid)
                    .key;

                let new_node
                    = self.block_manager.new_empty_leaf();

                let new_node_from
                    = self.block_manager.new_empty_leaf();

                new_node
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(records_mid..));

                new_node_from
                    .records_mut()
                    .extend_from_slice(records.get_unchecked(..records_mid));

                let parent_mut = parent_guard
                    .deref_mut()
                    .unwrap();

                let parent_children
                    = parent_mut.children_mut();

                parent_children
                    .insert(child_pos + 1, new_node.into_cell(latch_type));

                mem::drop(mem::replace(parent_children.get_unchecked_mut(child_pos),
                                       new_node_from.into_cell(latch_type)));

                parent_mut
                    .keys_mut()
                    .insert(child_pos, k3);
            }
        }
    }

    #[inline]
    fn traversal_write_internal(&self, lock_level: LockLevel, attempt: Attempts, key: Key)
                                -> (NodeVisits, Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, (LockLevel, Attempts)>)
    {
        let mut curr_level = INIT_TREE_HEIGHT;

        let (mut node_visits,
            mut current_guard,
            height,
            lock_level,
            attempt
        ) = self.retrieve_root(lock_level, attempt);

        let mut _curr_block
            = self.root.block();

        // if unsafe { ptr::read_unaligned(&key as *const _ as *const u64) } == 23 {
        //     Self::log_console(current_guard.deref().unwrap(), 0);
        //     let s = "daasd".to_string();
        // }
        let mut fence
            = Interval::new(self.min_key, self.max_key);

        let key = (self.inc_key)(key);
        loop {
            let current_guard_result
                = current_guard.deref();

            match current_guard_result.unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    node_visits += 1;

                    // let keys = index_page.keys();
                    // if keys.len() == 0 {
                    //     let s = "afasdf".to_string();
                    // }
                    let (child_pos, next_node)
                        = match index_page.keys().binary_search(&key)
                    {
                        Ok(pos) | Err(pos) => (pos, index_page.get_child_unsafe_cloned(pos)),
                    };

                    curr_level += 1;

                    let mut next_guard = self.apply_for_ref(
                        curr_level,
                        lock_level,
                        attempt,
                        height,
                        &next_node);

                    let has_overflow_next
                        = self.has_overflow(next_guard.deref().unwrap());

                    let has_underflow_next
                        = self.has_underflow(next_guard.deref().unwrap());

                    if has_overflow_next || has_underflow_next {
                        let current_exclusive
                            = current_guard.is_write_lock();

                        if self.locking_strategy.additional_lock_required() &&
                            (!next_guard.upgrade_write_lock() || !current_guard.upgrade_write_lock())
                        {
                            mem::drop(next_guard);
                            mem::drop(current_guard);

                            return (node_visits, Err((curr_level - 1, attempt + 1)));
                        } else if self.locking_strategy.is_orwc() &&
                            !current_exclusive &&
                            (self.has_overflow(current_guard.deref().unwrap()) ||
                             self.has_underflow(current_guard.deref().unwrap()))
                        { // this maps the obsolete check within an is_valid/deref auto call
                            mem::drop(next_guard);
                            mem::drop(current_guard);
                            return (node_visits, Err((curr_level - 1, attempt + 1)));
                        }

                        debug_assert!(self.locking_strategy.additional_lock_required() &&
                            current_guard.is_write_lock() && next_guard.is_write_lock() ||
                            !self.locking_strategy.additional_lock_required());

                        if has_overflow_next {
                            self.do_overflow_correction(
                                &mut current_guard,
                                child_pos,
                                next_guard)
                        } else {
                            match self.do_underflow_correction(
                                &fence,
                                curr_level,
                                attempt,
                                lock_level,
                                &mut current_guard,
                                child_pos,
                                next_guard)
                            {
                                Err(..) => return (node_visits, Err((curr_level - 1, attempt + 1))),
                                _ => {}
                            }
                        }
                    } else {
                        if child_pos < index_page.len() {
                            fence.upper = index_page.get_key(child_pos);
                        }

                        if child_pos > 0 {
                            fence.lower = index_page.get_key(child_pos - 1)
                        }

                        current_guard = next_guard;
                        _curr_block = next_node;
                    }
                }
                _ => return if current_guard.upgrade_write_lock() {
                    (node_visits, Ok(current_guard))
                } else {
                    (node_visits, Err((curr_level - 1, attempt + 1)))
                },
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_write(&self, key: Key) -> (NodeVisits, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;
        let mut node_visits = 0;

        loop {
            match self.traversal_write_internal(lock_level, attempt, key) {
                (visits, Err((n_lock_level, n_attempt))) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;
                    node_visits += visits;
                }
                (visits, Ok(guard)) => break (node_visits + visits, guard),
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_read(&self, key: Key) -> (NodeVisits, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let mut current_block
            = self.root.block();

        let mut current_guard
            = self.lock_reader(&current_block);

        let mut node_visits = 1;

        let key = (self.inc_key)(key);
        loop {
            match current_guard.deref().unwrap().as_ref() {
                Node::Index(index_page) => {
                    node_visits += 1;

                    match index_page.keys().binary_search(&key) {
                        Ok(pos) => unsafe {
                            let next_block
                                = index_page.get_child_unsafe_cloned(pos);

                            let next_guard
                                = self.lock_reader(&next_block);

                            current_guard = next_guard;
                            current_block = next_block;
                        },
                        Err(pos) => unsafe {
                            let next_block
                                = index_page.get_child_unsafe_cloned(pos);

                            let next_guard
                                = self.lock_reader(&next_block);

                            current_guard = next_guard;
                            current_block = next_block;
                        }
                    }
                }
                _ => break (node_visits, current_guard),
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_read_range(
        &self,
        current_range: &Interval<Key>)
        -> (NodeVisits,
            Vec<(BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>, BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>)>)
    {
        let mut current_block
            = self.root.block();

        let mut current_guard
            = self.lock_reader(&current_block);

        let mut node_visits
            = 1;

        let mut path
            = VecDeque::new();

        path.push_back((current_block, current_guard));

        let mut results
            = Vec::new();

        while !path.is_empty() {
            let (n_current_block, n_current_guard)
                = path.pop_front().unwrap();

            current_guard = n_current_guard;
            current_block = n_current_block;

            match current_guard.deref().unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    let keys
                        = index_page.keys();

                    let first_pos = match keys.binary_search(&current_range.lower) {
                        Ok(pos) => pos,
                        Err(pos) => pos
                    };

                    let last_pos = match keys.binary_search(&(self.inc_key)(current_range.upper)) {
                        Ok(pos) => pos,
                        Err(pos) => pos
                    };

                    node_visits += last_pos - first_pos + 1;
                    path.extend(index_page.children()
                        .get_unchecked(first_pos..=last_pos)
                        .iter()
                        .map(|child| (child.clone(), self.lock_reader(child))));
                }
                _ => results.push((current_block, current_guard)),
            }
        }

        (node_visits, results)
    }
}