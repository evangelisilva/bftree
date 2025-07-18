// src/bf_tree.rs

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::mini_page::MiniPage;
use crate::leaf_page::LeafPage;
use crate::mapping_table::MappingTable;
use crate::inner_node::InnerNode;
use crate::page::RecordType;

pub struct BfTree {
    pub mapping_table: MappingTable,
    pub root_inner_node: InnerNode, 
    pub inner_nodes: HashMap<u64, InnerNode>, 
}

impl BfTree {

    /// Get operation as per Bf-Tree design.
    /// Supports caching positive and negative lookups into mini-pages with small probability.
    /// - Searches mini-page first (if present).
    /// - Falls back to leaf page on disk.
    /// - With 1% chance, caches result (as Cache or Phantom).
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        // Traverse the tree to get the mini-page (if cached), leaf disk offset, and page ID.
        let (mini_page_rc_opt, leaf_disk_offset, page_id) = self.traverse(key);

        // Step 1: Search mini-page (memory cache)
        if let Some(ref mini_page_rc) = mini_page_rc_opt {
            let mut mini_page = mini_page_rc.borrow_mut();
            if let Some(value) = mini_page.binary_search(key) {
                // Found in mini-page → return immediately
                return Some(value);
            }
        }

        // Step 2: Search leaf page on disk
        let mut leaf_page = LeafPage::load_from_disk(leaf_disk_offset);
        if let Some(value) = leaf_page.binary_search(key) {
            // Found in leaf page
            // Step 3: With small probability, cache it in the mini-page
            if rand::random::<f64>() < 0.01 {
                if let Some(ref mini_page_rc) = mini_page_rc_opt {
                    let mut mini_page = mini_page_rc.borrow_mut();
                    if mini_page.insert(key, &value, Some(RecordType::Cache)) {
                        // Successfully cached in mini-page
                        return Some(value);
                    }

                    // If mini-page is full and cannot insert, try resizing
                    let new_size = mini_page.next_size();
                    if new_size == 0 {
                        // // Mini-page cannot grow further → merge and reset
                        // mini_page.merge();
                        // self.mapping_table.clear_mini_page(page_id);

                        // let mut new_mini = MiniPage::new(leaf_disk_offset);
                        // if new_mini.insert(key, &value, Some(RecordType::Cache)) {
                        //     self.mapping_table.update_mini_page(
                        //         page_id,
                        //         Rc::new(RefCell::new(new_mini)),
                        //     );
                        // }
                        panic!("merge() not yet implemented");
                    } else {
                        // Resize and reattempt insert
                        mini_page.resize(new_size as usize);
                        mini_page.insert(key, &value, Some(RecordType::Cache));
                    }
                } else {
                    // No existing mini-page → create one and insert
                    let mut new_mini = MiniPage::new(leaf_disk_offset);
                    if new_mini.insert(key, &value, Some(RecordType::Cache)) {
                        self.mapping_table.update_mini_page(
                            page_id,
                            Rc::new(RefCell::new(new_mini)),
                        );
                    }
                }
            }

            // Return the value retrieved from leaf
            return Some(value);
        }

        // Step 4: Not found in mini or leaf → it's a negative search
        // With small probability, cache the negative result as a Phantom record
        if rand::random::<f64>() < 0.01 {
            if let Some(ref mini_page_rc) = mini_page_rc_opt {
                let mut mini_page = mini_page_rc.borrow_mut();
                mini_page.insert(key, &[], Some(RecordType::Phantom));
            } else {
                let mut new_mini = MiniPage::new(leaf_disk_offset);
                if new_mini.insert(key, &[], Some(RecordType::Phantom)) {
                    self.mapping_table.update_mini_page(
                        page_id,
                        Rc::new(RefCell::new(new_mini)),
                    );
                }
            }
        }

        // Final result: not found
        None
    }

    /// Insert operation as per Bf-Tree design.
    /// Buffers inserts into mini-pages before flushing to the leaf page.
    /// If no mini-page exists or current one is full, handles growth, merge, and replacement.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        // Step 1: Traverse the tree to locate:
        // - mini_page_rc_opt: in-memory cached mini-page (if any)
        // - leaf_disk_offset: disk location of the associated leaf page
        // - page_id: logical page ID (used for mapping table updates)
        let (mini_page_rc_opt, leaf_disk_offset, page_id) = self.traverse(key);

        // Step 2: If a mini-page is already cached
        if let Some(ref mini_page_rc) = mini_page_rc_opt {
            let mut mini_page = mini_page_rc.borrow_mut();

            // Try to insert into the existing mini-page
            if mini_page.insert(key, value, Some(RecordType::Insert)) {
                // Insert succeeded — done
                return;
            }

            // Step 3: If mini-page is full, try to grow its size
            let new_size = mini_page.next_size();

            if new_size == 0 {
                // // Cannot grow further — must merge dirty records into the leaf page
                // mini_page.merge();

                // // Clear the old mini-page from the mapping table
                // self.mapping_table.clear_mini_page(page_id);

                // // Create a new mini-page and insert into it
                // let mut new_mini = MiniPage::new(leaf_disk_offset);
                // if new_mini.insert(key, value, Some(RecordType::Insert)) {
                //     self.mapping_table.update_mini_page(
                //         page_id,
                //         Rc::new(RefCell::new(new_mini)),
                //     );
                // }
                panic!("merge() not yet implemented");
            } else {
                // Resize the mini-page to a larger size and retry the insert
                mini_page.resize(new_size as usize);
                mini_page.insert(key, value, Some(RecordType::Insert));
            }

            return; // Done after handling existing mini-page
        }

        // Step 4: No mini-page exists → create one and insert into it
        let mut new_mini = MiniPage::new(leaf_disk_offset);
        if new_mini.insert(key, value, Some(RecordType::Insert)) {
            self.mapping_table.update_mini_page(
                page_id,
                Rc::new(RefCell::new(new_mini)),
            );
        }
    }
    
    /// Traverses the tree to resolve to mini-page (if cached) and leaf page disk offset.
    /// Returns (Option<Rc<RefCell<MiniPage>>>, u64 disk_offset, usize page_id)
    pub fn traverse(&self, key: &[u8]) -> (Option<Rc<RefCell<MiniPage>>>, u64, usize) {
    // pub fn traverse(&self, key: &[u8]) -> (Option<MiniPage>, u64, usize) {
        let mut current_node = &self.root_inner_node;

        loop {
            let child_page_id_opt = current_node.find_child_page_id(key);

            if let Some(child_page_id) = child_page_id_opt {
                // Try resolving child_page_id as an inner node first
                if let Some(inner_node) = self.get_inner_node(child_page_id) {
                    // Descend further in the tree
                    current_node = inner_node;
                } else {
                    // Reached last-level inner node ➔ child_page_id references a mini/leaf page
                    // Use mapping table to resolve to (mini-page pointer, disk offset)
                    let page_id = child_page_id as usize;
                    let mapping_entry = self.mapping_table.get(page_id);
                    if let Some((mini_page_rc_opt, disk_offset)) = mapping_entry {
                        // Return (mini-page pointer if cached, leaf page disk offset)
                        return (mini_page_rc_opt.map(|rc| Rc::clone(&rc)), disk_offset, page_id);
                    } else {
                        panic!("Page ID {} not found in mapping table", child_page_id);
                    }
                }
            } else {
                panic!("Invalid tree state: no child page ID found for key {:?}", key);
            }
        }
    }

    /// Helper to get inner node by page ID.
    ///
    /// In Bf-Tree, inner nodes are pinned in memory and referenced directly by page_id.
    /// Returns Some(&InnerNode) if page_id exists in pinned nodes, else None.
    pub fn get_inner_node(&self, page_id: u64) -> Option<&InnerNode> {
        // Check if page_id is the root node
        if page_id == 0 {
            Some(&self.root_inner_node)
        } else {
            // Lookup in the pinned inner_nodes HashMap
            self.inner_nodes.get(&page_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bftree_get_basic() {
        use crate::page::PageType;
        use crate::mapping_table::MappingTable;
        use crate::leaf_page::LeafPage;
        use crate::page::NodeMeta;
        use std::fs::File;

        const TEST: &str = "[test_bftree_get_basic]";

        std::fs::remove_file("storage.bftree").ok(); // ignore error if file doesn't exist

        // Clean slate
        let _ = File::create("storage.bftree").expect("Failed to init test file");

        // Step 1: Create a dummy leaf page and flush to disk
        let offset = std::fs::metadata("storage.bftree").map(|m| m.len()).unwrap_or(0);
        println!("{TEST} Using offset {} for leaf page", offset);

        let node_meta = NodeMeta::new(4096, PageType::LeafPage, false, 0, 0);
        let mut leaf = LeafPage { page: crate::page::Page::new(node_meta) };

        println!("{TEST} Inserting key-value pairs into leaf:");
        leaf.insert(b"hello", b"world", None);
        println!("{TEST}  - inserted (hello, world)");
        leaf.insert(b"foo", b"bar", None);
        println!("{TEST}  - inserted (foo, bar)");

        leaf.flush_to_disk(offset);
        println!("{TEST} Leaf page flushed to disk at offset {offset}\n");

        // Step 2: Set up dummy mapping table pointing to this leaf page
        let mut mapping_table = MappingTable::new();
        mapping_table.insert(42, None, offset); // page_id = 42
        println!("{TEST} Mapping table updated with page_id 42 -> offset {offset}\n");

        // Step 3: Create a BfTree with that mapping
        let bftree = crate::bf_tree::BfTree {
            mapping_table,
            root_inner_node: crate::inner_node::InnerNode::mock_single_child(42), // child page_id = 42
            inner_nodes: HashMap::new(),
        };
        println!("{TEST} BfTree initialized with root child page_id 42\n");

        // Step 4: Perform get
        let mut bftree = bftree;

        let result = bftree.get(b"hello");
        println!("{TEST} GET hello => {:?}", result);
        assert_eq!(result, Some(b"world".to_vec()));

        let result = bftree.get(b"foo");
        println!("{TEST} GET foo => {:?}", result);
        assert_eq!(result, Some(b"bar".to_vec()));

        let result = bftree.get(b"nonexistent");
        println!("{TEST} GET nonexistent => {:?}", result);
        assert_eq!(result, None);

        println!("{TEST} All lookups returned expected results.");
    }

    #[test]
    fn test_bftree_insert_and_get() {
        use crate::page::{PageType, NodeMeta};
        use crate::leaf_page::LeafPage;
        use crate::mapping_table::MappingTable;
        use std::fs::File;

        const TEST: &str = "[test_bftree_insert_and_get]";

        std::fs::remove_file("storage.bftree").ok();
        File::create("storage.bftree").expect("Failed to reset test file");

        let offset = std::fs::metadata("storage.bftree").map(|m| m.len()).unwrap_or(0);
        println!("{TEST} Using offset {offset} for initial leaf");

        // Step 1: Create a dummy leaf and flush it
        let node_meta = NodeMeta::new(4096, PageType::LeafPage, false, 0, 0);
        let leaf = LeafPage { page: crate::page::Page::new(node_meta) };
        leaf.flush_to_disk(offset);
        println!("{TEST} Flushed empty leaf page to disk");

        // Step 2: Set up mapping table
        let mut mapping_table = MappingTable::new();
        mapping_table.insert(99, None, offset); // page_id = 99

        // Step 3: Create BfTree
        let mut bftree = BfTree {
            mapping_table,
            root_inner_node: crate::inner_node::InnerNode::mock_single_child(99),
            inner_nodes: HashMap::new(),
        };

        println!("{TEST} BfTree created with child page_id 99");

        // Step 4: Insert values
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"dog", b"bark"),
            (b"cat", b"meow"),
            (b"cow", b"moo"),
        ];

        for (k, v) in &kvs {
            println!("{TEST} Inserting ({:?}, {:?})", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
            bftree.insert(k, v);
        }

        // Step 5: Query them back using get
        for (k, v) in &kvs {
            let res = bftree.get(k);
            println!("{TEST} GET {:?} => {:?}", String::from_utf8_lossy(k), res);
            assert_eq!(res, Some(v.to_vec()), "{TEST} Mismatch for key {:?}", k);
        }

        // Negative test
        let res = bftree.get(b"bird");
        println!("{TEST} GET bird => {:?}", res);
        assert_eq!(res, None);

        println!("{TEST} Insert and get test completed successfully.");
    }

}

