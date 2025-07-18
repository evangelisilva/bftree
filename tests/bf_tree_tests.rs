use bftree::{BfTree, InnerNode, MappingTable, MiniPage};
use std::collections::HashMap;
use log::{info, debug};
mod test_util;

#[test]
fn test_get() {
    info!("[TEST] bf_tree::get()");

    // Setup root inner node
    let mut root = InnerNode::new();
    root.keys.push(vec![50]);
    root.children.push(1);
    root.children.push(2);
    debug!("[Setup] Root InnerNode");
    debug!("keys     = {:?}", root.keys);
    debug!("children = {:?}", root.children);

    // Setup inner node at page_id=1
    let mut layer1 = InnerNode::new();
    layer1.keys.push(vec![10]);
    layer1.children.push(3);
    layer1.children.push(4);
    debug!("[Setup] InnerNode page_id=1");
    debug!("keys     = {:?}", layer1.keys);
    debug!("children = {:?}", layer1.children);

    // Inner nodes map
    let mut inner_nodes = HashMap::new();
    inner_nodes.insert(1, layer1);

    // Mapping table setup 
    let mut mapping_table = MappingTable::new(5);
    mapping_table.insert(3, None, 3000); // page_id=3 ➔ leaf only
    
    let mut dummy_mini_page = MiniPage::new(4000);
    let key2 = vec![15];
    let value2 = b"value_15".to_vec();
    dummy_mini_page.insert(&key2, &value2);
    mapping_table.insert(4, Some(dummy_mini_page.clone()), 4000); // page_id=4 ➔ mini-page + leaf

    debug!("[Setup] Mapping table entries:");
    for page_id in 3..5 {
        match mapping_table.get(page_id) {
            Some((mini_page_opt, disk_offset)) => {
                debug!(
                    "page_id={:<2} | mini_page_present={:<5} | disk_offset={}",
                    page_id,
                    mini_page_opt.is_some(),
                    disk_offset
                );
            }
            None => debug!("page_id={:<2} | <empty>", page_id),
        }
    }

    // Build BfTree
    let tree = BfTree {
        mapping_table,
        root_inner_node: root,
        inner_nodes,
    };

    // Scenario 1: key=5
    let key1 = vec![5];
    let result1 = tree.get(&key1);
    assert!(result1.is_none());

    // Scenario 2: key=15
    let result2 = tree.get(&key2);
    assert!(result2.is_some());
    assert_eq!(result2.unwrap(), value2);

    info!("[TEST] All bf_tree::traverse() assertions passed");
}


