use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::mem;
use crate::page_model::{Attempts, Height, Level};
use crate::block::block::BlockGuard;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::page_model::node::Node;
use crate::record_model::record_point::RecordPoint;
use crate::record_model::unsafe_clone::UnsafeClone;
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::{BPlusTree, INIT_TREE_HEIGHT, LockLevel, MAX_TREE_HEIGHT};
use crate::utils::interval::Interval;
use crate::utils::smart_cell::sched_yield;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn retrieve_root_olc(&self, mut lock_level: Level, mut attempt: Attempts)
    -> (NodeVisits, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, Height, LockLevel, Attempts)
    {
        let mut node_visits = 0usize;
        loop {
            match self.retrieve_root_internal(lock_level, attempt) {
                Err((n_lock_level, n_attempt)) => {
                    lock_level = n_lock_level;
                    attempt = n_attempt;
                    node_visits += 1;

                    sched_yield(attempt);
                }
                Ok((guard, height)) =>
                    break (node_visits + 1, guard, height, lock_level, attempt)
            }
        }
    }

    #[inline(always)]
    pub(crate) fn range_query_olc(&self,
                                  path: &mut Vec<(Interval<Key>, BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                  org_key_interval: Interval<Key>,
                                  mut node_visits: NodeVisits
    ) -> (NodeVisits, CRUDOperationResult<Key, Payload>)
    {
        let mut key_interval
            = org_key_interval.clone();

        let mut all_results
            = vec![];

        let mut history_path
            = VecDeque::with_capacity(2);

        loop {
            history_path.push_back(path.to_vec());

            let (visits, local_results) =
                self.range_query_leaf_results(path, &key_interval);

            node_visits += visits;

            if history_path.len() >= 2 {
                let prev_path
                    = history_path.pop_front().unwrap();

                match prev_path.get(prev_path.len() - 2) {
                    Some((.., parent_leaf)) if !parent_leaf.is_valid() => {
                        mem::drop(prev_path);
                        mem::drop(history_path);
                        mem::drop(local_results);
                        mem::drop(all_results);

                        *path = Vec::with_capacity(0);
                        let (visits, retry)
                            = self.dispatch(CRUDOperation::Range(org_key_interval));

                        return (visits + node_visits, retry)
                    }
                    _ => {}
                };
            }

            if !local_results.is_empty() {
                all_results.extend(local_results);

                let (leaf_space, ..)
                    = path.last().unwrap();

                key_interval.set_lower((self.inc_key)(leaf_space.upper()));

                if key_interval.lower > key_interval.upper {
                    break;
                }

                node_visits += self.next_leaf_page(
                    path,
                    path.len() - 2,
                    key_interval.lower());
            } else {
                break;
            }
        }

        (node_visits, CRUDOperationResult::MatchedRecords(all_results))
    }

    #[inline]
    pub(crate) fn next_leaf_page(&self,
                                 path: &mut Vec<(Interval<Key>, BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                 mut parent_index: usize,
                                 next_key: Key) -> NodeVisits
    {
        let mut attempts = 0;
        let mut node_visits = 0;
        loop {
            node_visits += 1;

            if attempts > 0 {
                sched_yield(attempts);
            }

            if parent_index >= path.len() { // when all path is invalid, we run stacking path function again!
                path.clear();

                let root_read = self.lock_reader_olc(
                    &self.root.block,
                    0,
                    attempts,
                    0);

                if !root_read.is_read_not_obsolete() {
                    attempts += 1;
                    continue
                }

                path.push((Interval::new(self.min_key, self.max_key), root_read));

                attempts = 0;
                parent_index = 0;
            }

            let (curr_interval, curr_parent)
                = path.get_mut(parent_index).unwrap();

            let mut curr_interval
                = curr_interval;

            let mut curr_parent
                = curr_parent;

            while !curr_interval.contains(next_key) {
                parent_index -= 1;
                let (n_curr_interval, n_curr_parent)
                    = path.get_mut(parent_index).unwrap();

                curr_interval = n_curr_interval;
                curr_parent = n_curr_parent;
            }

            let curr_deref
                = unsafe { curr_parent.deref_unsafe() };

            let (read, current_reader_version)
                = curr_parent.is_read_not_obsolete_result();

            if curr_deref.is_none() || !read {
                path.truncate(parent_index);
                attempts += 1;
                parent_index -= 1;
                continue;
            }

            match curr_deref.unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    node_visits += 1;
                    let keys
                        = index_page.keys();

                    let (curr_interval, next_page)
                        = match keys.binary_search(&(self.inc_key)(next_key))
                    {
                        Ok(pos) => (Interval::new(
                            keys.get(pos - 1).cloned()
                                .unwrap_or(curr_interval.lower()),
                            keys.get(pos).cloned()
                                .map(|max| (self.dec_key)(max)).unwrap_or(curr_interval.upper())),
                                    index_page.get_child_result(pos)),
                        Err(pos) => (Interval::new(
                            keys.get(pos - 1).cloned()
                                .unwrap_or(curr_interval.lower()),
                            keys.get(pos).cloned()
                                .map(|max| (self.dec_key)(max)).unwrap_or(curr_interval.upper())),
                                     index_page.get_child_result(pos))
                    };

                    let (read, read_version)
                        = curr_parent.is_read_not_obsolete_result();

                    if !read || read_version != current_reader_version {
                        path.truncate(parent_index);
                        parent_index -= 1;
                        attempts += 1;
                        continue;
                    }

                    curr_parent.update_read_latch(read_version);

                    attempts = 0;
                    parent_index += 1;
                    path.insert(parent_index, (curr_interval, self.lock_reader_olc(
                        next_page.assume_init_ref(),
                        parent_index as _,
                        attempts,
                        self.height())));
                }
                Node::Leaf(..) => {
                    path.truncate(parent_index + 1);
                    return node_visits;
                }
            }
        }
    }

    #[inline(always)]
    fn range_query_leaf_results(&self,
                                path: &mut Vec<(Interval<Key>, BlockGuard<'static, FAN_OUT, NUM_RECORDS, Key, Payload>)>,
                                key_interval: &Interval<Key>)
                                -> (NodeVisits, Vec<RecordPoint<Key, Payload>>)
    {
        let mut node_visits
            = 0;

        loop {
            let (.., leaf)
                = path.last().unwrap();

            let leaf_unchecked = unsafe { leaf.deref_unsafe() }.unwrap().as_ref();

            match leaf_unchecked {
                Node::Leaf(leaf_page) => unsafe {
                    let (read, current_read_version)
                        = leaf.is_read_not_obsolete_result();

                    if read {
                        let mut potential_results = leaf_page
                            .as_records()
                            .iter()
                            .skip_while(|record| record.key().lt(&key_interval.lower()))
                            .take_while(|record| record.key().le(&key_interval.upper()))
                            .map(|record| record.unsafe_clone())
                            .collect::<Vec<_>>();

                        let (read, n_current_read_version)
                            = leaf.is_read_not_obsolete_result();

                        if read && n_current_read_version == current_read_version { // avoid write in-between
                            return (node_visits + 1, potential_results)
                        } else {
                            potential_results.set_len(0);
                        }
                    }

                    node_visits += 1 + self.next_leaf_page(
                        path,
                        path.len() - 2,
                        key_interval.lower());
                }
                _ => unreachable!("Found Index but expected leaf = {}", leaf_unchecked)
            }
        }
    }

    #[inline]
    fn traversal_read_olc_internal(&self, key: Key) -> (NodeVisits, Option<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>>) {
        let mut current_guard
            = self.lock_reader(&self.root.block);

        let key = (self.inc_key)(key);
        let mut node_visits = 0;
        loop {
            let current
                = unsafe { current_guard.deref_unsafe() };

            let (read, current_reader_version)
                = current_guard.is_read_not_obsolete_result();

            if current.is_none() || !read {
                mem::drop(current_guard);

                return (node_visits + 1, None);
            }

            match current.unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    node_visits += 1;

                    let next_node = match index_page.keys().binary_search(&key) {
                        Ok(pos) => index_page.get_child_result(pos),
                        Err(pos) => index_page.get_child_result(pos)
                    };

                    let (read, read_version)
                        = current_guard.is_read_not_obsolete_result();

                    if !read || read_version != current_reader_version {
                        return (node_visits, None);
                    }

                    current_guard
                        = self.lock_reader(next_node.assume_init_ref());
                }
                _ => break (node_visits + 1, Some(current_guard)),
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_read_olc(&self, key: Key) -> (NodeVisits, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let mut attempt = 0;
        let mut node_visits = 0;

        loop {
            match self.traversal_read_olc_internal(key) {
                (nv, Some(guard)) if guard.is_valid() => 
                    break (node_visits + nv, guard),
                (nv, ..) => {
                    node_visits += nv;
                    attempt += 1;
                    sched_yield(attempt)
                }
            }
        }
    }

    #[inline]
    pub(crate) fn traversal_write_olc(&self, key: Key) -> (NodeVisits, BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>) {
        let mut attempt = 0;
        let mut lock_level = MAX_TREE_HEIGHT;
        let mut node_visits = 0usize;

        loop {
            match self.traversal_write_olc_internal(lock_level, attempt, key) {
                (visits, Err((n_lock_level, n_attempt))) => {
                    attempt = n_attempt;
                    lock_level = n_lock_level;
                    node_visits += visits;

                    sched_yield(attempt);
                }
                (visits, Ok(guard)) => break (node_visits + visits, guard),
            }
        }
    }

    #[inline]
    fn traversal_write_olc_internal(&self, lock_level: LockLevel, attempt: Attempts, key: Key)
    -> (NodeVisits, Result<BlockGuard<FAN_OUT, NUM_RECORDS, Key, Payload>, (LockLevel, Attempts)>)
    {
        let mut curr_level = INIT_TREE_HEIGHT;

        let (mut node_visits,
            mut current_guard,
            height,
            lock_level,
            attempt
        ) = self.retrieve_root_olc(lock_level, attempt);

        let mut fence
            = Interval::new(self.min_key, self.max_key);
        
        let key = (self.inc_key)(key);
        
        loop {
            let current_guard_result
                = current_guard.deref();

            if current_guard_result.is_none() {
                mem::drop(current_guard);

                return (node_visits, Err((curr_level - 1, attempt + 1)));
            }

            match current_guard_result.unwrap().as_ref() {
                Node::Index(index_page) => unsafe {
                    node_visits += 1;

                    let (child_pos, next_node)
                        = match index_page.keys().binary_search(&key)
                    {
                        Ok(pos) | Err(pos) => (pos, index_page.get_child_result(pos)),
                    };

                    if !current_guard.is_valid() {
                        mem::drop(current_guard);

                        return (node_visits, Err((curr_level - 1, attempt + 1)));
                    }

                    curr_level += 1;

                    let mut next_guard = self.apply_for_ref(
                        curr_level,
                        lock_level,
                        attempt,
                        height,
                        next_node.assume_init_ref());

                    let next_guard_result
                        = next_guard.deref_unsafe();

                    if next_guard_result.is_none() || !current_guard.is_valid() {
                        mem::drop(next_guard);
                        mem::drop(current_guard);

                        return (node_visits, Err((curr_level - 1, attempt + 1)));
                    }

                    let has_overflow_next
                        = self.has_overflow(next_guard_result.unwrap());
                    
                    let has_underflow_next
                        = self.has_underflow(next_guard_result.unwrap());
                    
                    if has_overflow_next || has_underflow_next {
                        if !current_guard.upgrade_write_lock() || !next_guard.upgrade_write_lock() {
                            mem::drop(next_guard);
                            mem::drop(current_guard);

                            return (node_visits, Err((curr_level - 1, attempt + 1)));
                        }

                        debug_assert!(current_guard.upgrade_write_lock() &&
                            next_guard.upgrade_write_lock());

                        if has_overflow_next {
                            self.do_overflow_correction(
                                &mut current_guard,
                                child_pos,
                                next_guard)
                        }
                        else {
                            match self.do_underflow_correction(
                                &fence,
                                curr_level,
                                attempt,
                                lock_level,
                                &mut current_guard,
                                child_pos,
                                next_guard)
                            {
                                Err(_) => return (node_visits, Err((curr_level - 1, attempt + 1))),
                                _ => { }
                            }
                        }
                    }
                    else {
                        if child_pos < index_page.len() {
                            fence.upper = index_page.get_key(child_pos);
                        }

                        if child_pos > 0 {
                            fence.lower = index_page.get_key(child_pos - 1)
                        }
                        
                        current_guard = next_guard;
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
}