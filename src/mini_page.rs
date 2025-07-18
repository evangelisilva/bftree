// src/mini_page.rs

use crate::page::{Page, NodeMeta, PageType, RecordType};
use crate::config::{MINI_PAGE_MIN_SIZE, MINI_PAGE_MAX_SIZE};
use crate::leaf_page::LeafPage;

#[derive(Clone)]
pub struct MiniPage {
    pub page: Page,
}

impl MiniPage {
    /// Creates a new MiniPage for a given leaf disk offset.
    pub fn new(leaf_offset: u64) -> Self {
        let node_meta = NodeMeta::new(
            MINI_PAGE_MIN_SIZE as u16, // Minimal initial size (can grow dynamically)
            PageType::MiniPage,
            false, // split flag initially false
            0,     // record count
            leaf_offset, // link to associated leaf page
        );

        let page = Page::new(node_meta);
        Self { page }
    }

    /// Binary search delegated to internal Page.
    pub fn binary_search(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.page.binary_search(key)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], record_type: Option<RecordType>) -> bool {
        self.page.insert(key, value, record_type)
    }

    pub fn next_size(&self) -> u16 {
        let current = self.page.node_meta.node_size;
        let next = current.saturating_mul(2);
        if (next as usize) <= MINI_PAGE_MAX_SIZE {
            next
        } else {
            0
        }
    }

    pub fn resize(&mut self, new_size: usize) {
        let old_page = &self.page;

        // Create new node meta with updated size
        let new_meta = NodeMeta::new(
            new_size as u16,
            PageType::MiniPage,
            old_page.node_meta.split_flag,
            old_page.node_meta.record_count,
            old_page.node_meta.leaf,
        );

        // Pre-allocate memory for new vectors with the expected new capacity
        let mut new_data = Vec::with_capacity(new_size);
        new_data.extend_from_slice(&old_page.data);

        let mut new_kv_metas = Vec::with_capacity(old_page.kv_metas.len());
        new_kv_metas.extend_from_slice(&old_page.kv_metas);

        // Replace the current page with the resized version
        self.page = Page {
            node_meta: new_meta,
            kv_metas: new_kv_metas,
            data: new_data,
        };
    }

    /// Merge mini-page into its corresponding leaf page on disk.
    ///
    /// This happens when the mini-page becomes too large or cold.
    /// The merge ensures dirty records are flushed, and only hot records are retained.
    pub fn merge(&mut self) -> Option<(Vec<u8>, u64, u64)> {
        // Step 1: Load leaf page
        let leaf_offset = self.page.node_meta.leaf;
        let mut leaf_page = LeafPage::load_from_disk(leaf_offset);

        // Step 2: Classify records
        let mut hot_records = vec![];

        for kv_meta in &self.page.kv_metas {
            if kv_meta.ref_flag == 1 {
                hot_records.push(kv_meta.clone());
            } else {
                match kv_meta.type_flag {
                    0 | 2 => {
                        let key = self.page.read_key(kv_meta);
                        let value = self.page.read_value(kv_meta);

                        if !leaf_page.can_fit(&key, &value) {
                            // Step 2b: Split leaf if needed
                            let (mut left, mut right, split_key) = leaf_page.split();

                            if key < split_key {
                                left.insert(&key, &value, None);
                            } else {
                                right.insert(&key, &value, None);
                            }

                            let left_offset = leaf_offset;
                            let right_offset = get_next_offset();

                            left.flush_to_disk(left_offset);
                            right.flush_to_disk(right_offset);

                            // Caller must update mapping table and inner node
                            return Some((split_key, left_offset, right_offset));
                        }

                        leaf_page.insert(&key, &value, None);
                    }
                    _ => {} // cache/phantom → no merge needed
                }
            }
        }

        // Step 3: Flush updated leaf page
        leaf_page.flush_to_disk(leaf_offset);

        // Step 4: Rebuild mini-page with hot records only
        self.page.kv_metas.clear();
        self.page.data.clear();
        self.page.node_meta.record_count = 0;

        for kv in hot_records {
            let key = self.page.read_key(&kv);
            let value = self.page.read_value(&kv);
            self.page.insert(&key, &value, kv.type_flag);
        }

        // Step 5: Reset ref_flag
        for meta in &mut self.page.kv_metas {
            meta.ref_flag = 0;
        }

        None // No split occurred
    }

}
