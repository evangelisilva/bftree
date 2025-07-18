// src/config.rs

pub const INNER_NODE_SIZE: usize = 4096; // size of inner nodes (fixed)
pub const LEAF_PAGE_SIZE: usize = 4096; // size of leaf pages (fixed)
pub const MINI_PAGE_MIN_SIZE: usize = 64; // minimum size of a mini-page
pub const MINI_PAGE_MAX_SIZE: usize = 4096; // maximum size of a mini-page
