// src/mapping_table.rs

use std::rc::Rc;
use std::cell::RefCell;

use crate::mini_page::MiniPage;

/// The MappingTable maps logical page IDs to:
/// - an optional in-memory MiniPage (cached hot records)
/// - the disk offset of the base leaf page (always exists)
pub struct MappingTable {
    table: Vec<Option<(Option<Rc<RefCell<MiniPage>>>, u64)>>,// Vec acts as indirection array
}

impl MappingTable {
    /// Create a new MappingTable with an initial capacity for page IDs.
    pub fn new() -> Self {
        Self {
            table: Vec::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use std::cell::RefCell;
    use crate::mini_page::MiniPage;

    #[test]
    fn test_mapping_table_dynamic_growth() {
        let mut table = MappingTable::new();

        let dummy_page = Rc::new(RefCell::new(MiniPage::new(42)));
        let page_id = 5;
        let disk_offset = 1000;

        // Insert beyond initial zero-length
        table.insert(page_id, Some(dummy_page.clone()), disk_offset);

        // Should grow and store the entry
        assert!(table.contains(page_id));

        let (mini_opt, offset) = table.get(page_id).expect("Missing entry");
        assert_eq!(offset, disk_offset);
        assert!(mini_opt.is_some());

        // Update mini-page to None and verify
        table.clear_mini_page(page_id);
        let (mini_opt, offset) = table.get(page_id).expect("Missing entry after clear");
        assert!(mini_opt.is_none());
        assert_eq!(offset, disk_offset);
    }
}
