use crate::heap_page::HeapPage;
use crate::heap_page::HeapPageIntoIter;
use crate::heapfile::HeapFile;
use common::prelude::*;
use std::fs::File;
use std::sync::Arc;
use std::sync::RwLock;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    //TODO milestone hs
    hf: Arc<HeapFile>,
    page_index: PageId,
    page_count: PageId,
    curr_page_iter: Option<HeapPageIntoIter>,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let page_count = hf.num_pages();
        // if there are no pages, we can't iterate through anything
        if page_count == 0 {
            HeapFileIterator {
                hf,
                page_index: 0,
                page_count,
                curr_page_iter: None,
            }
        } else {
            // if there are pages, we start from the beginning
            let first_page = hf.read_page_from_file(0).unwrap();

            HeapFileIterator {
                hf,
                page_index: 0,
                page_count,
                curr_page_iter: Some(first_page.into_iter()),
            }
        }
    }

    pub(crate) fn new_from(tid: TransactionId, hf: Arc<HeapFile>, value_id: ValueId) -> Self {
        let page_count = hf.num_pages();

        // we set the start page and slot to the given ones
        let start_page = match value_id.page_id {
            Some(pid) => pid,
            None => 0,
        };
        let start_slot = match value_id.slot_id {
            Some(sid) => sid,
            None => 0,
        };

        // edge cases; can't iterate
        if page_count == 0 || start_page >= page_count {
            return HeapFileIterator {
                hf,
                page_index: start_page,
                page_count,
                curr_page_iter: None,
            };
        }

        let page = hf.read_page_from_file(start_page).unwrap();

        // regular case; initialize iterator at given values
        HeapFileIterator {
            hf,
            page_index: start_page,
            page_count,
            curr_page_iter: Some(HeapPageIntoIter::new_from(page, start_slot)),
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // try to go to the next slot in the page
            if let Some(page_iter) = &mut self.curr_page_iter {
                if let Some((value, slot_id)) = page_iter.next() {
                    return Some((
                        value,
                        ValueId {
                            container_id: self.hf.container_id,
                            segment_id: None,
                            page_id: Some(self.page_index),
                            slot_id: Some(slot_id),
                        },
                    ));
                }
            }

            // if we can't go to the next slot in the page, we have to go to the next page if possible
            self.page_index += 1;
            if self.page_index >= self.page_count {
                return None;
            }
            let page = self.hf.read_page_from_file(self.page_index).unwrap();
            self.curr_page_iter = Some(page.into_iter());
        }
    }
}
