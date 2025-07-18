use bftree::*;
use log::info;
mod test_util;

#[test]
fn test_page_binary_search_correctness() {

        info!("[TEST] page::binary_search()");

        // Create dummy NodeMeta for a MiniPage
        let node_meta = NodeMeta::new(1024, PageType::MiniPage, false, 0, 0);
        let mut page = Page::new(node_meta);

        // Insert keys in random order
        let keys = vec![
            b"banana".to_vec(),
            b"apple".to_vec(),
            b"cherry".to_vec(),
            b"date".to_vec(),
        ];
        info!("[Setup] Inserting keys in random order: {:?}", keys);

        for key in &keys {
            let value = format!("val_{}", String::from_utf8_lossy(key)).into_bytes();
            let inserted = page.insert(key, &value);
            assert!(inserted, "Insertion should succeed for key {:?}", key);
            info!("Inserted key={:?} with value={:?}", key, value);
        }

        // Verify kv_metas are sorted by key
        let sorted_keys: Vec<_> = page.kv_metas.iter()
            .map(|kv| {
                let start = kv.offset as usize;
                let end = start + kv.key_size as usize;
                page.data[start..end].to_vec()
            })
            .collect();

        let mut expected_keys = keys.clone();
        expected_keys.sort();

        info!("[Assert] Expected sorted keys: {:?}", expected_keys);
        info!("[Assert] Actual sorted keys from kv_metas: {:?}", sorted_keys);

        assert_eq!(sorted_keys, expected_keys, "Keys should remain sorted in kv_metas");

        // Test binary search finds all existing keys
        for key in &expected_keys {
            let result = page.binary_search(key);
            assert!(result.is_some(), "binary_search should find key {:?}", key);

            let value = result.unwrap();
            let expected_value = format!("val_{}", String::from_utf8_lossy(key)).into_bytes();

            info!("[Search] key={:?} ➔ found value={:?}", key, value);

            assert_eq!(value, expected_value, "Value mismatch for key {:?}", key);
        }

        // Test binary search for a non-existing key
        let missing_key = b"fig".to_vec();
        let result = page.binary_search(&missing_key);
        info!("[Search] key={:?} ➔ result: {:?}", missing_key, result);
        assert!(result.is_none(), "binary_search should return None for missing key {:?}", missing_key);

        info!("[TEST] Page binary_search correctness passed");
}
