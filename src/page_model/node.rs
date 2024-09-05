use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::mem;
use itertools::Itertools;
use crate::utils::shadow_vec::ShadowVec;
use serde::{Serialize, Deserialize};
use crate::page_model::BlockRef;
use crate::page_model::internal_page::InternalPage;
use crate::page_model::leaf_page::LeafPage;
// use crate::record_model::record::Record;
// use crate::record_model::record_like::RecordLike;
// use crate::record_model::record_list::{PayloadVersioned, RecordList};
use crate::record_model::record_point::RecordPoint;
// use crate::record_model::Version;
// use crate::record_model::version_info::VersionInfo;

// #[repr(u8)]
pub enum Node<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> {
    Index(InternalPage<FAN_OUT, NUM_RECORDS, Key, Payload>),
    Leaf(LeafPage<NUM_RECORDS, Key, Payload>),
    // MultiVersionLeaf(LeafPage<NUM_RECORDS, Key, Payload, RecordList<Key, Payload>>),
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Default + Clone + Display
> Display for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Node::Index(index_page) =>
                write!(f, "Index {{\n\tkeys: [{}]\n\tChildren: [{}]\n}}",
                       index_page.keys()
                           .iter()
                           .join(","),
                       index_page.children()
                           .iter()
                           .map(|c| format!("{}", c.unsafe_borrow().to_string()))
                           .join(",")),
            Node::Leaf(records) => write!(f, "Leaf({})", records
                .as_records()
                .iter()
                .join(",")),
            // Node::MultiVersionLeaf(record_lists) =>
            //     write!(f, "MultiVersionLeaf({})", record_lists
            //         .as_records()
            //         .iter()
            //         .join(","))
        }
    }
}

#[repr(u8)]
pub enum NodeUnsafeDegree {
    Ok,
    Overflow,
    Underflow,
}

impl NodeUnsafeDegree {
    #[inline]
    pub const fn is_ok(&self) -> bool {
        match self {
            Self::Ok => true,
            _ => false
        }
    }
    
    #[inline(always)]
    pub const fn is_unsafe(&self) -> bool {
        !self.is_ok()
    }

    pub const fn is_overflow(&self) -> bool {
        match self {
            Self::Overflow => true,
            _ => false
        }
    }

    pub const fn is_underflow(&self) -> bool {
        match self {
            Self::Underflow => true,
            _ => false
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    #[inline(always)]
    pub fn is_overflow(&self, allocation: usize) -> bool {
        debug_assert!(allocation >= self.len());

        self.len() >= allocation
    }

    #[inline(always)]
    pub fn is_underflow(&self, allocation: usize) -> bool {
        // debug_assert!(allocation > 0 && allocation >= self.len());

        self.len() < allocation / 2
    }

    #[inline(always)]
    pub fn unsafe_degree(&self, allocation: usize) -> NodeUnsafeDegree {
        let len = self.len();

        if len >= allocation {
            NodeUnsafeDegree::Overflow
        } else if len < allocation / 2 {
            NodeUnsafeDegree::Underflow
        } else {
            NodeUnsafeDegree::Ok
        }
    }

    #[inline(always)]
    pub const fn is_leaf(&self) -> bool {
        match self {
            Node::Index(..) => false,
            _ => true
        }
    }

    #[inline(always)]
    pub fn children_mut(&self) -> ShadowVec<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        match self {
            Node::Index(index_page) => index_page.children_mut(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .children_mut")
        }
    }

    #[inline(always)]
    pub fn keys_mut(&self) -> ShadowVec<Key> {
        match self {
            Node::Index(index_page) => index_page.keys_mut(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys_mut")
        }
    }

    #[inline(always)]
    pub fn keys(&self) -> &[Key] {
        match self {
            Node::Index(index_page) => index_page.keys(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .keys")
        }
    }

    #[inline(always)]
    pub fn children(&self) -> &[BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>] {
        match self {
            Node::Index(index_page) => index_page.children(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .children")
        }
    }

    #[inline(always)]
    pub fn records_mut(&self) -> ShadowVec<RecordPoint<Key, Payload>> {
        match self {
            Node::Leaf(records_page) =>
                records_page.as_records_mut(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .records_mut")
        }
    }

    #[inline(always)]
    pub fn as_records(&self) -> &[RecordPoint<Key, Payload>] {
        match self {
            Node::Leaf(records_page) =>
                records_page.as_records(),
            _ => unreachable!("Sleepy Joe hit me -> Not tree Page .records_mut")
        }
    }

    // #[inline(always)]
    // pub fn record_lists_mut(&self) -> ShadowVec<RecordList<Key, Payload>> {
    //     match self {
    //         Node::MultiVersionLeaf(record_lists) =>
    //             record_lists.as_records_mut(),
    //         _ => unreachable!("Sleepy Joe hit me -> Not tree Page .record_lists_mut")
    //     }
    // }

    #[inline(always)]
    pub const fn is_directory(&self) -> bool {
        !self.is_leaf()
    }

    #[inline]
    pub fn delete_key(&mut self, key: Key) -> Option<Payload> {
        match self {
            Node::Leaf(events_page) => events_page
                .as_records()
                .binary_search_by_key(&key, |event| event.key)
                .map(|found| events_page.as_records_mut().remove(found).payload)
                .ok(),
            // Node::MultiVersionLeaf(records_lists) => records_lists
            //     .as_records_mut()
            //     .iter_mut()
            //     .rev()
            //     .find(|record_list| record_list.key() == key)
            //     .map(|record_list| record_list.delete(del_version.unwrap()))
            //     .unwrap_or(false),
            _ => None,
        }
    }

    #[inline]
    pub fn update_record_point(&mut self, key: Key, payload: Payload) -> Option<Payload> {
        match self {
            Node::Leaf(events_page) => events_page
                .as_records()
                .binary_search_by_key(&key, |e| e.key)
                .map(|found| unsafe {
                    mem::replace(events_page
                    .as_records_mut()
                    .get_unchecked_mut(found)
                    .payload_mut(), payload)
                })
                .ok(),
            _ => None
        }
    }

    // #[inline]
    // pub fn update_record(&mut self, key: Key, payload: Payload, version: Version) -> bool {
    //     match self {
    //         Node::MultiVersionLeaf(records_lists) => records_lists
    //             .as_records()
    //             .binary_search_by_key(&key, |record_list| record_list.key())
    //             .map(|pos| unsafe {
    //                 let mut records_mut
    //                     = records_lists.as_records_mut();
    //
    //                 let list
    //                     = records_mut.get_unchecked_mut(pos);
    //
    //                 if list.delete(version) {
    //                     list.push_payload(payload, Some(version));
    //                     true
    //                 } else {
    //                     false
    //                 }
    //             }).unwrap_or_default(),
    //         _ => false
    //     }
    // }

    #[inline]
    pub fn push_record_point(&mut self, key: Key, payload: Payload) -> bool {
        match self {
            Node::Leaf(records_page) => match records_page
                .as_records()
                .binary_search_by_key(&key, |event| event.key)
            {
                Err(pos) => {
                    records_page
                        .as_records_mut()
                        .insert(pos, RecordPoint::new(key, payload));

                    true
                }
                _ => false
            }
            _ => false
        }
    }

    // #[inline]
    // pub fn push_record(&mut self, key: Key, payload: Payload, version: Version) -> bool {
    //     match self {
    //         Node::MultiVersionLeaf(records_lists, ..) => match records_lists
    //             .as_records()
    //             .binary_search_by_key(&key, |record_list| record_list.key())
    //         {
    //             Err(pos) => {
    //                 records_lists
    //                     .as_records_mut()
    //                     .insert(pos,
    //                             RecordList::new(key, payload, VersionInfo::new(version))
    //                     );
    //
    //                 true
    //             }
    //             _ => false
    //         }
    //         _ => false
    //     }
    // }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            Node::Index(index_page) => index_page.keys_len(),
            Node::Leaf(records_page) => records_page.len(),
            // Node::MultiVersionLeaf(record_lists_page) => record_lists_page.len()
        }
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        &self
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash,
    Payload: Default + Clone
> Default for Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
    fn default() -> Self {
        Self::Leaf(LeafPage::default())
    }
}