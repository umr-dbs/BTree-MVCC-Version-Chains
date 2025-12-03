use std::hash::Hash;
use std::fmt::Display;
use std::mem;
use CCBPlusTree::record_model::record_point::RecordPoint;
use crate::crud_model::crud_api::{CRUDDispatcher, NodeVisits};
use crate::crud_model::crud_operation::CRUDOperation;
use crate::crud_model::crud_operation_result::CRUDOperationInnerReason::{KeyAlreadyDeleted, KeyDoesNotExist};
use crate::crud_model::crud_operation_result::CRUDOperationResult;
use crate::tree::bplus_tree::BPlusTree;

const DEBUG_VERIFY: bool = false;
const WEAVER_RANGE_SCAN_ENABLED: bool = true;
const LAZY_RANGE_SCAN_ENABLED: bool = true;

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + Display,
    Payload: Default + Clone + Send + Sync + Display + 'static
> CRUDDispatcher<Key, Payload> for BPlusTree<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline]
    fn dispatch(&self, crud_operation: CRUDOperation<Key, Payload>)
                -> (NodeVisits, CRUDOperationResult<Key, Payload>) {
        let olc
            = self.locking_strategy.is_optimistic();

        let is_weaver
            = self.v_index_type.is_v_weaver();

        // let is_frugal
        //     = self.v_index_type.is_frugal();

        match crud_operation {
            CRUDOperation::Delete(key) => {
                let (node_visits, guard) = if olc {
                    self.traversal_write_olc_append(key)
                } else {
                    self.traversal_write(key)
                };

                let del_version
                    = self.next_version();

                 match guard.deref_mut()
                     .unwrap()
                     .delete_key(key, del_version)
                 {
                     Ok(Some(payload)) =>
                         (node_visits + if is_weaver {
                             node_visits + self.weaver_callback(guard, key, del_version)
                         } else { 0 }, CRUDOperationResult::Deleted(key, payload, del_version)),
                     Ok(None) => (node_visits, CRUDOperationResult::ZeroAffected(KeyDoesNotExist)),
                     Err(..) => (node_visits, CRUDOperationResult::ZeroAffected(KeyAlreadyDeleted)),
                 }
            }
            CRUDOperation::Insert(key, payload) => {
                let (node_visits, guard) = if olc {
                    self.traversal_write_olc_append(key)
                } else {
                    self.traversal_write(key)
                };

                let ingest_version = self.next_version();

                if DEBUG_VERIFY {
                    let res =  (node_visits, guard.deref_mut()
                        .unwrap()
                        .push_record_point(key, payload, ingest_version, self.v_index_type)
                        .then(|| CRUDOperationResult::Inserted(key, ingest_version))
                        .unwrap_or_default());

                    mem::drop(guard);
                    let (_, r)
                        = self.dispatch(CRUDOperation::Point(key, ingest_version));

                    match r {
                        CRUDOperationResult::MatchedRecord(..) => {},
                        _ => println!("Error finding new Inserted Version")
                    }

                    res
                }
                else {
                    if guard.deref_mut()
                        .unwrap()
                        .push_record_point(key, payload, ingest_version, self.v_index_type)
                    {
                        (node_visits + if is_weaver {
                            node_visits + self.weaver_callback(guard, key, ingest_version)
                        } else { 0 }, CRUDOperationResult::Inserted(key, ingest_version))
                    }
                    else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::Update(key, payload) => {
                let (node_visits, guard) = if olc {
                    self.traversal_write_olc_append(key)
                } else {
                    self.traversal_write(key)
                };

                let update_version = self.next_version();
                if DEBUG_VERIFY {
                    let res = (node_visits, match guard
                        .deref_mut()
                        .unwrap()
                        .update_record_point(key, payload, update_version)
                    {
                        Ok(Some(payload)) =>
                            CRUDOperationResult::Updated(key, payload, update_version),
                        Ok(None) =>
                            CRUDOperationResult::ZeroAffected(KeyDoesNotExist),
                        Err(..) =>
                            CRUDOperationResult::Error
                    });

                    mem::drop(guard);
                    let (_, r)
                        = self.dispatch(CRUDOperation::Point(key, update_version));

                    match r {
                        CRUDOperationResult::MatchedRecord(..) => {},
                        _ => println!("Error finding new Updated Version")
                    }

                    res
                }
                else {
                    match guard
                        .deref_mut()
                        .unwrap()
                        .update_record_point(key, payload, update_version)
                    {
                        Ok(Some(payload)) =>
                            (node_visits + if is_weaver {
                                node_visits + self.weaver_callback(guard, key, update_version)
                            } else { 0 }, CRUDOperationResult::Updated(key, payload, update_version)),
                        Ok(None) =>
                            (node_visits, CRUDOperationResult::ZeroAffected(KeyDoesNotExist)),
                        Err(..) =>
                            (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::Point(key, version) if olc => match self.traversal_read_olc(key) {
                (node_visits, leaf_guard) => unsafe {
                    let leaf_page = leaf_guard
                        .deref_unsafe()
                        .unwrap()
                        .as_ref();

                    let records
                        = leaf_page.as_records();

                    if leaf_guard.is_valid() {
                        (node_visits, match records
                            .binary_search_by_key(&key, |r| r.key)
                            .ok()
                        {
                            Some(v_record) => records
                                .get_unchecked(v_record)
                                .find(version)
                                .map(|v|
                                    RecordPoint::new(key, v.payload.clone()))
                                .into(),
                            _ => CRUDOperationResult::MatchedRecord(None)
                        })
                    } else {
                        mem::drop(leaf_guard);
                        self.dispatch(CRUDOperation::Point(key, version))
                    }
                }
            }
            CRUDOperation::PointSi(key) => self.dispatch(CRUDOperation::Point(
                key,
                self.committed_version())
            ),
            CRUDOperation::Point(key, version) => match self.traversal_read(key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    (node_visits, leaf_page
                        .as_records()
                        .binary_search_by_key(&key, |record| record.key)
                        .ok()
                        .map(|pos| unsafe {
                            leaf_page
                                .as_records()
                                .get_unchecked(pos)
                                .version_index()
                                .find(version)
                                .map(|found|
                                    RecordPoint::new(key, found.payload.clone()))
                                .unwrap_or_default()
                        })
                        .into())
                }
            },
            CRUDOperation::RangeSi(key_interval) => self.dispatch(CRUDOperation::Range(
                key_interval,
                self.committed_version()
            )),
            CRUDOperation::Range(interval, version)
            if WEAVER_RANGE_SCAN_ENABLED && is_weaver => {
                let (node_visits, matches)
                    = self.weaver_scan_dispatch(interval, version);

                (node_visits, matches.into())
            }
            // CRUDOperation::Range(interval, version) if is_frugal => {
            //     let mut path
            //             = Vec::with_capacity(self.root.height() as _);
            //
            //     let node_visits = self.next_leaf_page(
            //         path.as_mut(),
            //         0,
            //         interval.lower);
            //
            //     self.range_query_olc(path.as_mut(), interval, version, node_visits)
            // }
            CRUDOperation::Range(key_interval, version)
            if LAZY_RANGE_SCAN_ENABLED => {
                let mut path
                    = Vec::with_capacity(self.root.height() as _);

                let node_visits = self.next_leaf_page(path.as_mut(),
                                                      0,
                                                      key_interval.lower);

                self.range_query_olc(path.as_mut(), key_interval, version, node_visits)
            }
            CRUDOperation::Range(interval, version) => {
                let (node_visits, guards)
                    = self.traversal_read_range(&interval);

                (node_visits,
                 guards.into_iter()
                     .flat_map(|(_block, guard)| guard
                         .deref()
                         .unwrap()
                         .as_ref()
                         .as_records()
                         .iter()
                         .skip_while(|record| !interval.contains(record.key))
                         .filter_map(|record| {
                             if let Some(v_e) = record.find(version) {
                                 Some((record.key(), v_e.payload.clone()))
                             } else {
                                 None
                             }
                         })
                         .take_while(|(key, ..)| interval.contains(*key))
                         .map(|(key, v_payload)| RecordPoint::new(key, v_payload.clone()))
                         .collect::<Vec<_>>())
                     .collect::<Vec<_>>()
                     .into())
            }
            CRUDOperation::PeekMin if olc => match self.traversal_read_olc(self.min_key) {
                (node_visits, leaf_guard) => unsafe {
                    let leaf_page = leaf_guard
                        .deref_unsafe()
                        .unwrap()
                        .as_ref();

                    if leaf_guard.is_valid() {
                        (node_visits, match leaf_page
                            .as_records()
                            .first()
                        {
                            Some(v_record) if v_record.is_live() =>
                                CRUDOperationResult::MatchedRecord(Some(
                                    RecordPoint::new(v_record.key(), v_record.newest_payload()))
                                ),
                            _ => CRUDOperationResult::MatchedRecord(None)
                        })
                    } else {
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

                    (node_visits, match leaf_page
                        .as_records()
                        .first()
                    {
                        Some(v_record) if v_record.is_live() =>
                            CRUDOperationResult::MatchedRecord(Some(
                                RecordPoint::new(v_record.key(), v_record.newest_payload()))
                            ),
                        _ => CRUDOperationResult::MatchedRecord(None)
                    })
                }
            }
            CRUDOperation::PeekMax if olc => match self.traversal_read_olc(self.max_key) {
                (node_visits, leaf_guard) => unsafe {
                    let leaf_page = leaf_guard
                        .deref_unsafe()
                        .unwrap()
                        .as_ref();

                    if leaf_guard.is_valid() {
                        (node_visits, match leaf_page
                            .as_records()
                            .last()
                        {
                            Some(v_record) if v_record.is_live() =>
                                CRUDOperationResult::MatchedRecord(Some(
                                    RecordPoint::new(v_record.key(), v_record.newest_payload()))
                                ),
                            _ => CRUDOperationResult::MatchedRecord(None)
                        })
                    } else {
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

                    (node_visits, match leaf_page
                        .as_records()
                        .last()
                    {
                        Some(v_record) if v_record.is_live() =>
                            CRUDOperationResult::MatchedRecord(Some(
                                RecordPoint::new(v_record.key(), v_record.newest_payload()))
                            ),
                        _ => CRUDOperationResult::MatchedRecord(None)
                    })
                }
            }
            CRUDOperation::PopMin if olc => match self.traversal_write_olc_append(self.min_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    if !leaf_page.as_records().is_empty() {
                        let leaf_page_records
                            = leaf_page.records_mut();

                        let v_record
                            = leaf_page_records.get_unchecked_mut(0);

                        let del_version
                            = self.next_version();

                        (node_visits, match v_record.delete(del_version) {
                            Some(old_payload) => CRUDOperationResult::Deleted(
                                v_record.key(),
                                old_payload,
                                del_version),
                            _ => CRUDOperationResult::Error
                        })
                    } else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::PopMin => match self.traversal_write(self.min_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    if !leaf_page.as_records().is_empty() {
                        let leaf_page_records
                            = leaf_page.records_mut();

                        let v_record
                            = leaf_page_records.get_unchecked_mut(0);

                        let del_version
                            = self.next_version();

                        (node_visits, match v_record.delete(del_version) {
                            Some(old_payload) => CRUDOperationResult::Deleted(
                                v_record.key(),
                                old_payload,
                                del_version),
                            _ => CRUDOperationResult::Error
                        })
                    } else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::PopMax if olc => match self.traversal_write_olc_append(self.max_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    let len = leaf_page.len();
                    if len > 0 {
                        let leaf_page_records
                            = leaf_page.records_mut();

                        let v_record
                            = leaf_page_records.get_unchecked_mut(len - 1);

                        let del_version
                            = self.next_version();

                        (node_visits, match v_record.delete(del_version) {
                            Some(old_payload) => CRUDOperationResult::Deleted(
                                v_record.key(),
                                old_payload,
                                del_version),
                            _ => CRUDOperationResult::Error
                        })
                    } else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::PopMax => match self.traversal_write(self.max_key) {
                (node_visits, leaf_guard) => {
                    let leaf_page = leaf_guard
                        .deref()
                        .unwrap()
                        .as_ref();

                    let len = leaf_page.len();
                    if len > 0 {
                        let leaf_page_records
                            = leaf_page.records_mut();

                        let v_record
                            = leaf_page_records.get_unchecked_mut(len - 1);

                        let del_version
                            = self.next_version();

                        (node_visits, match v_record.delete(del_version) {
                            Some(old_payload) => CRUDOperationResult::Deleted(
                                v_record.key(),
                                old_payload,
                                del_version),
                            _ => CRUDOperationResult::Error
                        })
                    } else {
                        (node_visits, CRUDOperationResult::Error)
                    }
                }
            }
            CRUDOperation::Empty => (NodeVisits::MIN, CRUDOperationResult::Error),
            CRUDOperation::UpdateRand => {
                unimplemented!()
            }
            CRUDOperation::DeleteRand => {
                unimplemented!()
            }
            CRUDOperation::InsertRand => {
                unimplemented!()
            }
        }
    }
}