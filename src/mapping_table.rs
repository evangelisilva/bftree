// src/mapping_table.rs

use std::rc::Rc;
use std::cell::RefCell;
use crate::mini_page::MiniPage;

use crate::mini_page::MiniPage;
use crate::page_id_allocator::PageIdAllocator;

/// The MappingTable maps logical page IDs to:
/// - an optional in-memory MiniPage (cached hot records)
/// - the disk offset of the base leaf page (always exists)
pub struct MappingTable {
    table: Vec<Option<(Option<Rc<RefCell<MiniPage>, u64)>>, // Vec acts as indirection array
}

impl MappingTable {
    /// Create a new MappingTable with an initial capacity for page IDs.
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            table: vec![None; initial_capacity],
        }
    }

    /// Insert or update the mapping for a logical page ID.
    pub fn insert(&mut self, page_id: usize, mini_page_rc: Option<Rc<RefCell<MiniPage>>>, disk_offset: u64) {
        if page_id >= self.table.len() {
            self.table.resize(page_id + 1, None);
        }
        self.table[page_id] = Some((mini_page_rc, disk_offset));
    }

    /// Update just the MiniPage for a given logical page ID.
    pub fn update_mini_page(&mut self, page_id: usize, mini_page_rc: Rc<RefCell<MiniPage>>) {
        if let Some((_, disk_offset)) = self.get(page_id) {
            self.table[page_id] = Some((Some(mini_page_rc), disk_offset));
        } else {
            panic!("Cannot update mini-page: page_id not found in mapping table");
        }
    }

    /// Get (mini_page, disk_offset) for the given page ID.
    pub fn get(&self, page_id: usize) -> Option<(Option<Rc<RefCell<MiniPage>>>, u64)> {
        self.table.get(page_id).and_then(|entry| entry.clone())
    }

    /// Check if the mapping table contains an entry for the page ID.
    pub fn contains(&self, page_id: usize) -> bool {
        page_id < self.table.len() && self.table[page_id].is_some()
    }

    pub fn clear_mini_page(&mut self, page_id: usize) {
        if let Some((_, disk_offset)) = self.get(page_id) {
            self.table[page_id] = Some((None, disk_offset));
        }
    }

}
