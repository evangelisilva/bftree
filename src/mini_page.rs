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
        if next <= MINI_PAGE_MAX_SIZE {
            next as u16
        } else {
            0 // cannot grow further
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

    // pub fn merge(&mut self) {
    //     let leaf_offset = self.page.node_meta.leaf;
    //     let mut leaf_page = LeafPage::load_from_disk(leaf_offset);

    //     let mut dirty_records = Vec::new();
    //     let mut hot_records = Vec::new();

    //     for kv in &self.page.kv_metas {
    //         let key_start = kv.offset as usize;
    //         let key_end = key_start + kv.key_size as usize;
    //         let val_end = key_end + kv.value_size as usize;

    //         let key = &self.page.data[key_start..key_end];
    //         let value = &self.page.data[key_end..val_end];

    //         if kv.ref_flag != 0 {
    //             // Hot record → retain in mini-page (copy into new buffer if needed later)
    //             hot_records.push((key.to_vec(), value.to_vec(), kv.clone()));
    //         } else if kv.type_flag == 0 {
    //             // Dirty insert → merge into leaf
    //             dirty_records.push((key.to_vec(), value.to_vec()));
    //         } else {
    //             // Cold phantom/read cache → drop without writing to disk
    //         }
    //     }

    //     let needs_split = dirty_records.iter().any(|(k, v)| !leaf_page.can_fit(k, v));

    //     if needs_split {
    //         // Split the leaf and insert accordingly
    //         let (mut left, mut right, split_key) = leaf_page.split();

    //         for (k, v) in dirty_records {
    //             if k < split_key {
    //                 let _ = left.insert(&k, &v);
    //             } else {
    //                 let _ = right.insert(&k, &v);
    //             }
    //         }

    //         left.flush_to_disk();
    //         right.flush_to_disk();
    //     } else {
    //         for (k, v) in dirty_records {
    //             let _ = leaf_page.insert(&k, &v);
    //         }
    //         leaf_page.flush_to_disk();
    //     }

    //     // Replace mini-page content with only hot records (optional optimization)
    //     self.page.kv_metas.clear();
    //     self.page.data.clear();
    //     self.page.node_meta.record_count = 0;

    //     for (key, value, mut kv) in hot_records {
    //         let offset = self.page.data.len() as u16;
    //         self.page.data.extend_from_slice(&key);
    //         self.page.data.extend_from_slice(&value);
    //         kv.offset = offset;
    //         kv.ref_flag = 0; // clear reference bit for future tracking
    //         self.page.kv_metas.push(kv);
    //         self.page.node_meta.record_count += 1;
    //     }
    // }

    /// Merge mini-page into its corresponding leaf page.
    /// This is triggered when the mini-page is too large or cold.
    pub fn merge(&mut self) {
        // Step 1: Locate corresponding leaf page;
        let leaf_offset = self.node_meta.leaf;
        let mut leaf_page = LeafPage::load_from_disk(leaf_disk_offset);
    }

}
