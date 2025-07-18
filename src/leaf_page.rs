// src/leaf_page.rs

use std::io::Write;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Read};

use crate::page::{Page, NodeMeta, PageType, KVMeta, RecordType};
use crate::config::LEAF_PAGE_SIZE;

#[derive(Clone)]
pub struct LeafPage {
    pub page: Page,
}

impl LeafPage {
    /// Loads a LeafPage from disk at given offset.
    // pub fn load_from_disk(disk_offset: u64) -> Self {
    //     // In real implementation:
    //     // 1. Seek to disk_offset in file.
    //     // 2. Read PAGE_SIZE bytes.
    //     // 3. Parse NodeMeta, KVMeta array, and data block.
    //     //
    //     // Here is a placeholder with unimplemented:

    //     let node_meta = NodeMeta::new(
    //         LEAF_PAGE_SIZE as u16, // default leaf page size
    //         PageType::LeafPage,
    //         false,
    //         0,
    //         0, // leaf field not used for leaf pages
    //     );

    //     let page = Page::new(node_meta);

    //     Self { page }
    // }

    /// Loads a LeafPage from disk at given offset.
    pub fn load_from_disk(disk_offset: u64) -> Self {
        let mut file = OpenOptions::new()
            .read(true)
            .open("storage.bftree") // consistent with flush_to_disk
            .expect("Failed to open file");

        file.seek(SeekFrom::Start(disk_offset)).expect("Seek failed");

        let mut meta_buf = [0u8; 12];
        file.read_exact(&mut meta_buf).expect("Failed to read NodeMeta");
        let node_meta = NodeMeta::deserialize(&meta_buf).expect("Invalid NodeMeta");

        let mut kv_metas = Vec::with_capacity(node_meta.record_count as usize);
        let mut total_kv_data_size = 0usize;

        for _ in 0..node_meta.record_count {
            let mut kv_buf = [0u8; 8];
            file.read_exact(&mut kv_buf).expect("Failed to read KVMeta");
            let kv = KVMeta::deserialize(&kv_buf).expect("Invalid KVMeta");
            total_kv_data_size += kv.key_size as usize + kv.value_size as usize;
            kv_metas.push(kv);
        }

        let mut data = vec![0u8; total_kv_data_size];
        file.read_exact(&mut data).expect("Failed to read key-value data");

        let page = Page {
            node_meta,
            kv_metas,
            data,
        };

        Self { page }
    }


    /// Loads a LeafPage from disk at the given offset.
    // pub fn load_from_disk(disk_offset: u64) -> Self {
    //     let mut file = File::open("storage.bftree").expect("Failed to open file");
    //     file.seek(SeekFrom::Start(disk_offset)).expect("Failed to seek");

    //     let mut buffer = vec![0u8; LEAF_PAGE_SIZE];
    //     file.read_exact(&mut buffer).expect("Failed to read full page");

    //     // 1. Deserialize NodeMeta (first 12 bytes)
    //     let meta_bytes: [u8; 12] = buffer[0..12].try_into().unwrap();
    //     let node_meta = NodeMeta::deserialize(&meta_bytes).unwrap();

    //     // 2. Deserialize KVMetas
    //     let mut kv_metas = Vec::new();
    //     let mut offset = 12;
    //     for _ in 0..node_meta.record_count {
    //         let kv_bytes: [u8; 8] = buffer[offset..offset + 8].try_into().unwrap();
    //         let kv = KVMeta::deserialize(&kv_bytes).unwrap();
    //         kv_metas.push(kv);
    //         offset += 8;
    //     }

    //     // 3. Remaining bytes are the data block
    //     let data = buffer[offset..].to_vec();

    //     let page = Page {
    //         node_meta,
    //         kv_metas,
    //         data,
    //     };

    //     Self { page }
    // }

    /// Binary search delegated to internal Page.
    pub fn binary_search(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.page.binary_search(key)
    }

    // pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
    //     self.page.insert(key, value)
    // }

    pub fn insert(&mut self, key: &[u8], value: &[u8], record_type: Option<RecordType>) -> bool {
        self.page.insert(key, value, record_type)
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
                left.insert(key, val, None);
            } else {
                right.insert(key, val, None);
            }
        }

        (
            LeafPage { page: left },
            LeafPage { page: right },
            split_key,
        )
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File};

    #[test]
    fn test_leaf_page_round_trip() {
        let path = "storage.bftree";
        let offset: u64 = 0;

        // Clean the test file
        File::create(path).expect("Failed to clear test file");

        // Create and populate a LeafPage
        let node_meta = NodeMeta::new(
            LEAF_PAGE_SIZE as u16,
            PageType::LeafPage,
            false,
            0,
            0,
        );
        let mut original = LeafPage {
            page: Page::new(node_meta),
        };

        let kvs = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"carrot".to_vec(), b"vegetable".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
        ];

        for (k, v) in &kvs {
            assert!(original.insert(k, v, None), "Insert failed");
        }

        original.flush_to_disk(offset);

        // Load the page back
        let mut loaded = LeafPage::load_from_disk(offset);

        // Ensure metadata matches
        assert_eq!(
            loaded.page.kv_metas.len(),
            original.page.kv_metas.len(),
            "KVMeta count mismatch"
        );

        // Check each key-value pair
        for (k, v) in &kvs {
            let result = loaded.binary_search(k);
            match result {
                Some(val) => {
                    println!(
                        "Key: {:?}, Expected: {:?}, Found: {:?}",
                        String::from_utf8_lossy(k),
                        String::from_utf8_lossy(v),
                        String::from_utf8_lossy(&val)
                    );
                    assert_eq!(val, *v, "Value mismatch for key {:?}", k);
                }
                None => {
                    println!("Key not found: {:?}", String::from_utf8_lossy(k));
                    panic!("Test failed: key not found");
                }
            }
        }

    }
}
