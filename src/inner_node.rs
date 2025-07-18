// src/inner_node.rs

pub struct InnerNode {
    pub keys: Vec<Vec<u8>>, // Sorted separator keys
    pub children: Vec<u64>, // Child page IDs 
}

impl InnerNode {
    /// Creates a new empty InnerNode.
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Finds the child page ID for the given key using binary search.
    ///
    /// Returns Some(child_page_id) if found, or None if invalid tree state.
    pub fn find_child_page_id(&self, key: &[u8]) -> Option<u64> {
        if self.keys.is_empty() {
            // Edge case: no keys, single child only
            return self.children.first().copied();
        }

        let mut left = 0;
        let mut right = self.keys.len();

        while left < right {
            let mid = (left + right) / 2;
            match key.cmp(&self.keys[mid]) {
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Equal => return self.children.get(mid + 1).copied(),
                std::cmp::Ordering::Greater => left = mid + 1,
            }
        }

        // If key < all separator keys ➔ return first child.
        // If key > all separator keys ➔ return last child.
        if left == 0 {
            self.children.first().copied()
        } else {
            self.children.get(left).copied()
        }
    }

    /// Inserts a separator key and child pointer at the appropriate position.
    /// For tree building and splits (basic version).
    pub fn insert(&mut self, key: Vec<u8>, child_page_id: u64) {
        let pos = self.keys.binary_search(&key).unwrap_or_else(|e| e);
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, child_page_id);
    }



    // Creates a mock inner node with a single fence key and child page ID.
    /// Used for testing traversal logic in BfTree.
    pub fn mock_single_child(child_page_id: u64) -> Self {
        Self {
            keys: vec![], // No keys, since everything routes to the one child
            children: vec![child_page_id],
        }
    }

}
