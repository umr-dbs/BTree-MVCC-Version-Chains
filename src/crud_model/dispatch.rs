use std::hash::Hash;
use std::fmt::Display;
use std::mem;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Sync + Display
> CRUDDispatcher<Key, Payload> for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn dispatch(&self, crud_operation: CRUDOperation<Key, Payload>)
                -> (NodeVisits, CRUDOperationResult<Key, Payload>) {
        let olc
            = self.locking_strategy.is_optimistic();

        match crud_operation {
            CRUDOperation::Delete(key) if olc => {
                let (node_visits, guard) = self
                    .traversal_write_olc(key);

                (node_visits,
                 guard.deref_mut()
                     .unwrap()
                     .delete_key(key)
                     .map(|payload| CRUDOperationResult::Deleted(key, payload))
                     .unwrap_or_default())
            }
            CRUDOperation::Delete(key) => {
                let (node_visits, guard) = self
                    .traversal_write(key);

                (node_visits,
                 guard.deref_mut()
                     .unwrap()
                     .delete_key(key)
                     .map(|payload| CRUDOperationResult::Deleted(key, payload))
                     .unwrap_or_default())
            }
            CRUDOperation::Insert(key, payload) if olc => {
                let (node_visits, guard) = self
                    .traversal_write_olc(key);

                (node_visits, guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| CRUDOperationResult::Inserted(key))
                    .unwrap_or_default())
            }
            CRUDOperation::Insert(key, payload) => {
                let (node_visits, guard) = self
                    .traversal_write(key);

                (node_visits, guard.deref_mut()
                    .unwrap()
                    .push_record_point(key, payload)
                    .then(|| CRUDOperationResult::Inserted(key))
                    .unwrap_or_default())
            }
            CRUDOperation::Update(key, payload) if olc => {
                let (node_visits, guard) = self
                    .traversal_write_olc(key);

                (node_visits, guard
                    .deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| CRUDOperationResult::Updated(key, old))
                    .unwrap_or_default())
            }
            CRUDOperation::Update(key, payload) => {
                let (node_visits, guard) = self
                    .traversal_write(key);

                (node_visits, guard.deref_mut()
                    .unwrap()
                    .update_record_point(key, payload)
                    .map(|old| CRUDOperationResult::Updated(key, old))
                    .unwrap_or_default())
            }
            CRUDOperation::Point(key) if olc => match self.dispatch(
                CRUDOperation::Range((key..=key).into()))
            {
                (node_visits,
                    CRUDOperationResult::MatchedRecords(mut records))
                if records.len() <= 1 => (node_visits, records.pop().into()),
                (node_visits, ..) => (node_visits, CRUDOperationResult::Error)
            },
            CRUDOperation::Point(key) => match self.traversal_read(key) {
                (node_visits, leaf_guard) => {
                    let leaf_page =  leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    (node_visits, leaf_page
                        .as_records()
                        .binary_search_by_key(&key, |record| record.key)
                        .ok()
                        .map(|pos| unsafe { leaf_page.as_records().get_unchecked(pos) }.clone())
                        .into())
                }
            },
            CRUDOperation::Range(key_interval) if olc => {
                let mut path
                    = Vec::with_capacity(self.root.height() as _);

                let node_visits = self.next_leaf_page(path.as_mut(),
                                                      0,
                                                      key_interval.lower);

                self.range_query_olc(path.as_mut(), key_interval, node_visits)
            }
            CRUDOperation::Range(interval) => {
                let (node_visits, guards)
                    =  self.traversal_read_range(&interval);

                (node_visits,
                 guards.into_iter()
                    .flat_map(|(_block, guard)| guard
                        .deref()
                        .unwrap()
                        .as_ref()
                        .as_records()
                        .iter()
                        .skip_while(|record| !interval.contains(record.key))
                        .take_while(|record| interval.contains(record.key))
                        .cloned()
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>()
                    .into())
            },
            CRUDOperation::PeekMin if olc => match self.traversal_read_olc(self.min_key) {
                (node_visits, leaf_guard) => unsafe {
                    let leaf_page = leaf_guard
                        .deref_unsafe()
                        .unwrap()
                        .as_ref();

                    let result = leaf_page
                        .as_records()
                        .first()
                        .cloned();
                    
                    if leaf_guard.is_valid() {
                        (node_visits, result.into())
                    }
                    else {
                        mem::drop(leaf_guard);
                        self.dispatch(CRUDOperation::PeekMin)
                    }
                }
            }
            CRUDOperation::PeekMin => match self.traversal_read(self.min_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    let result = leaf_page
                        .as_records()
                        .first()
                        .cloned()
                        .into();
                    
                    (node_visits, result)
                }
            }
            CRUDOperation::PeekMax if olc => match self.traversal_read_olc(self.max_key) {
                (node_visits, leaf_guard) => unsafe {
                    let leaf_page = leaf_guard
                        .deref_unsafe()
                        .unwrap()
                        .as_ref();

                    let result = leaf_page
                        .as_records()
                        .last()
                        .cloned();
                    
                    if leaf_guard.is_valid() {
                        (node_visits, result.into())
                    }
                    else {
                        mem::drop(leaf_guard);
                        self.dispatch(CRUDOperation::PeekMax)
                    }
                }
            },
            CRUDOperation::PeekMax => match self.traversal_read(self.max_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    let result = leaf_page
                        .as_records()
                        .last()
                        .cloned()
                        .into();
                    
                    (node_visits, result)
                }
            }
            CRUDOperation::PopMin if olc => match self.traversal_write_olc(self.min_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    if !leaf_page.as_records().is_empty() {
                        let r
                            = leaf_page.records_mut().remove(0);

                        (node_visits, CRUDOperationResult::Deleted(r.key(), r.payload.clone()))
                    }
                    else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::PopMin => match self.traversal_write(self.min_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page =  leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    if !leaf_page.as_records().is_empty() {
                        let r 
                            = leaf_page.records_mut().remove(0);
                        
                        (node_visits, CRUDOperationResult::Deleted(r.key, r.payload))
                    }
                    else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::PopMax if olc => match self.traversal_write_olc(self.max_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page =  leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    let len = leaf_page.len();
                    if len > 0 {
                        let r
                            = leaf_page.records_mut().pop();

                        (node_visits, CRUDOperationResult::Deleted(r.key(), r.payload.clone()))
                    }
                    else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::PopMax => match self.traversal_write(self.max_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page =  leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    let len = leaf_page.len();
                    if len > 0 {
                        let r
                            = leaf_page.records_mut().pop();
                        
                        (node_visits, CRUDOperationResult::Deleted(r.key(), r.payload.clone()))
                    }
                    else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::Empty => (NodeVisits::MIN, CRUDOperationResult::Error),
        }
    }
}