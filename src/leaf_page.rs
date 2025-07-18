// src/leaf_page.rs

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};

use crate::page::{Page, NodeMeta, PageType};
use crate::config::{LEAF_PAGE_SIZE};

#[derive(Clone)]
pub struct LeafPage {
    pub page: Page,
}

impl LeafPage {
    /// Loads a LeafPage from disk at given offset.
    pub fn load_from_disk(disk_offset: u64) -> Self {
        // In real implementation:
        // 1. Seek to disk_offset in file.
        // 2. Read PAGE_SIZE bytes.
        // 3. Parse NodeMeta, KVMeta array, and data block.
        //
        // Here is a placeholder with unimplemented:

        let node_meta = NodeMeta::new(
            LEAF_PAGE_SIZE as u16, // default leaf page size
            PageType::LeafPage,
            false,
            0,
            0, // leaf field not used for leaf pages
        );

        let page = Page::new(node_meta);

        Self { page }
    }

    /// Loads a LeafPage from disk at the given offset.
    pub fn load_from_disk(disk_offset: u64) -> Self {
        let mut file = File::open("storage.bftree").expect("Failed to open file");
        file.seek(SeekFrom::Start(disk_offset)).expect("Failed to seek");

        let mut buffer = vec![0u8; LEAF_PAGE_SIZE];
        file.read_exact(&mut buffer).expect("Failed to read full page");

        // 1. Deserialize NodeMeta (first 12 bytes)
        let meta_bytes: [u8; 12] = buffer[0..12].try_into().unwrap();
        let node_meta = NodeMeta::deserialize(&meta_bytes).unwrap();

        // 2. Deserialize KVMetas
        let mut kv_metas = Vec::new();
        let mut offset = 12;
        for _ in 0..node_meta.record_count {
            let kv_bytes: [u8; 8] = buffer[offset..offset + 8].try_into().unwrap();
            let kv = KVMeta::deserialize(&kv_bytes).unwrap();
            kv_metas.push(kv);
            offset += 8;
        }

        // 3. Remaining bytes are the data block
        let data = buffer[offset..].to_vec();

        let page = Page {
            node_meta,
            kv_metas,
            data,
        };

        Self { page }
    }

    /// Binary search delegated to internal Page.
    pub fn binary_search(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.page.binary_search(key)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.page.insert(key, value)
    }

    pub fn can_fit(&self, key: &[u8], value: &[u8]) -> bool {
        let kv_meta_size = 8;
        let total_size = self.page.kv_metas.len() * kv_meta_size
            + self.page.data.len()
            + key.len()
            + value.len()
            + 12; // NodeMeta size

        total_size <= LEAF_PAGE_SIZE
    }

    pub fn flush_to_disk(&self, offset: u64) {
        let mut file = OpenOptions::new()
            .write(true)
            .open("storage.bftree") // example file
            .expect("Failed to open file");

        file.seek(SeekFrom::Start(offset)).unwrap();

        let meta_bytes = self.page.node_meta.serialize().unwrap();
        file.write_all(&meta_bytes).unwrap();

        for kv in &self.page.kv_metas {
            let kv_bytes = kv.serialize().unwrap();
            file.write_all(&kv_bytes).unwrap();
        }

        file.write_all(&self.page.data).unwrap();
    }

    pub fn split(&mut self) -> (LeafPage, LeafPage, Vec<u8>) {
        let mid = self.page.kv_metas.len() / 2;
        let split_key = {
            let kv = &self.page.kv_metas[mid];
            let start = kv.offset as usize;
            let end = start + kv.key_size as usize;
            self.page.data[start..end].to_vec()
        };

        let mut left = Page::new(self.page.node_meta.clone());
        let mut right = Page::new(self.page.node_meta.clone());

        for (i, kv) in self.page.kv_metas.iter().enumerate() {
            let start = kv.offset as usize;
            let end = start + kv.key_size as usize + kv.value_size as usize;
            let key = &self.page.data[start..start + kv.key_size as usize];
            let val = &self.page.data[start + kv.key_size as usize..end];

            if i < mid {
                left.insert(key, val);
            } else {
                right.insert(key, val);
            }
        }

        (
            LeafPage { page: left },
            LeafPage { page: right },
            split_key,
        )
    }

}
