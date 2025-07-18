// src/page.rs

use std::cmp::Ordering;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Result, Read, Write};

/// Distinguishes between mini-pages and leaf pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    MiniPage,
    LeafPage,
}

/// NodeMeta (12 bytes total) matches Bf-Tree layout.
#[derive(Debug, Clone)]
pub struct NodeMeta {
    pub node_size: u16,      // 16 bits
    pub page_type: bool,     // 1 bit (true = mini, false = leaf)
    pub split_flag: bool,    // 1 bit
    pub record_count: u16,   // 16 bits
    pub leaf: u64,           // 48 bits used
}

impl NodeMeta {
    pub fn new(node_size: u16, page_type: PageType, split_flag: bool, record_count: u16, leaf: u64) -> Self {
        Self {
            node_size,
            page_type: matches!(page_type, PageType::MiniPage),
            split_flag,
            record_count,
            leaf,
        }
    }

    /// Serializes NodeMeta to 12-byte array.
    pub fn serialize(&self) -> Result<[u8; 12]> {
        let mut buf = [0u8; 12];
        let mut cursor = Cursor::new(&mut buf[..]);

        cursor.write_u16::<LittleEndian>(self.node_size)?;

        // Pack page_type (1 bit) and split_flag (1 bit) into u8
        let flags = ((self.page_type as u8) << 1) | (self.split_flag as u8);
        cursor.write_u8(flags)?;

        cursor.write_u8(0)?; // padding byte

        cursor.write_u16::<LittleEndian>(self.record_count)?;

        let leaf_bytes = self.leaf.to_le_bytes();
        cursor.write_all(&leaf_bytes[..6])?;

        Ok(buf)
    }

    /// Deserializes NodeMeta from 12-byte array.
    pub fn deserialize(buf: &[u8; 12]) -> Result<Self> {
        let mut cursor = Cursor::new(&buf[..]);

        let node_size = cursor.read_u16::<LittleEndian>()?;

        let flags = cursor.read_u8()?;
        let page_type = ((flags >> 1) & 0x01) != 0;
        let split_flag = (flags & 0x01) != 0;

        cursor.read_u8()?; // skip padding

        let record_count = cursor.read_u16::<LittleEndian>()?;

        let mut leaf_bytes = [0u8; 8];
        cursor.read_exact(&mut leaf_bytes[..6])?;
        let leaf = u64::from_le_bytes(leaf_bytes);

        Ok(Self {
            node_size,
            page_type,
            split_flag,
            record_count,
            leaf,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Insert = 0,
    Cache = 1,
    Tombstone = 2,
    Phantom = 3,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            0 => RecordType::Insert,
            1 => RecordType::Cache,
            2 => RecordType::Tombstone,
            3 => RecordType::Phantom,
            _ => panic!("Invalid type_flag"),
        }
    }
}

impl Into<u8> for RecordType {
    fn into(self) -> u8 {
        self as u8
    }
}


/// KVMeta (8 bytes total) matches Bf-Tree layout.
#[derive(Debug, Clone)]
pub struct KVMeta {
    pub key_size: u16,    // 14 bits
    pub value_size: u16,  // 14 bits
    pub offset: u16,      // 16 bits
    pub type_flag: u8,    // 2 bits
    pub is_fence: bool,   // 1 bit
    pub ref_flag: u8,     // 2 bits
    pub lookahead: u16,   // 16 bits
}

impl KVMeta {
    pub fn new(key_size: u16, value_size: u16, offset: u16, type_flag: u8, is_fence: bool, ref_flag: u8, lookahead: u16) -> Self {
        Self {
            key_size: key_size & 0x3FFF,
            value_size: value_size & 0x3FFF,
            offset,
            type_flag: type_flag & 0x03,
            is_fence,
            ref_flag: ref_flag & 0x03,
            lookahead,
        }
    }

    /// Serializes KVMeta to 8-byte array.
    pub fn serialize(&self) -> Result<[u8; 8]> {
        let mut packed: u64 = 0;

        packed |= (self.key_size as u64 & 0x3FFF) << 0;
        packed |= (self.value_size as u64 & 0x3FFF) << 14;
        packed |= (self.offset as u64 & 0xFFFF) << 28;
        packed |= (self.type_flag as u64 & 0x03) << 44;
        packed |= (self.is_fence as u64 & 0x01) << 46;
        packed |= (self.ref_flag as u64 & 0x03) << 47;
        packed |= (self.lookahead as u64) << 49;

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&packed.to_le_bytes()[..8]);
        Ok(buf)
    }

    /// Deserializes KVMeta from 8-byte array.
    pub fn deserialize(buf: &[u8; 8]) -> Result<Self> {
        let packed = u64::from_le_bytes(*buf);

        let key_size = ((packed >> 0) & 0x3FFF) as u16;
        let value_size = ((packed >> 14) & 0x3FFF) as u16;
        let offset = ((packed >> 28) & 0xFFFF) as u16;
        let type_flag = ((packed >> 44) & 0x03) as u8;
        let is_fence = ((packed >> 46) & 0x01) != 0;
        let ref_flag = ((packed >> 47) & 0x03) as u8;
        let lookahead = ((packed >> 49) & 0xFFFF) as u16;

        Ok(Self {
            key_size,
            value_size,
            offset,
            type_flag,
            is_fence,
            ref_flag,
            lookahead,
        })
    }
}

/// Generic Page struct shared by mini-pages and leaf pages.
#[derive(Clone)]
pub struct Page {
    pub node_meta: NodeMeta,
    pub kv_metas: Vec<KVMeta>,
    pub data: Vec<u8>, // key-value data block
}

impl Page {
    /// Creates a new empty page.
    pub fn new(node_meta: NodeMeta) -> Self {
        Self {
            node_meta,
            kv_metas: Vec::new(),
            data: Vec::new(),
        }
    }

    /// Performs binary search for target_key.
    pub fn binary_search(&mut self, target_key: &[u8]) -> Option<Vec<u8>> {
        let mut left = 0;
        let mut right = self.kv_metas.len();

        while left < right {
            let mid = (left + right) / 2;
            let mid_meta = &mut self.kv_metas[mid];

            let key_start = mid_meta.offset as usize;
            let key_end = key_start + mid_meta.key_size as usize;
            let mid_key = &self.data[key_start..key_end];

            match mid_key.cmp(target_key) {
                Ordering::Equal => {
                    mid_meta.ref_flag = 1;

                    let value_start = key_end;
                    let value_end = value_start + mid_meta.value_size as usize;
                    return Some(self.data[value_start..value_end].to_vec());
                },
                Ordering::Less => left = mid + 1,
                Ordering::Greater => {
                    if mid == 0 { break; }
                    right = mid;
                },
            }
        }
        None
    }

    /// Inserts key-value while keeping KVMeta sorted.
    pub fn insert(&mut self, key: &[u8], value: &[u8], record_type: Option<RecordType>) -> bool {

        let record_type_u8: u8 = match record_type {
            Some(r) => r.into(),
            None => 0, // Default to Insert
        };

        let kv_meta_size = 8;
        let total_size = self.kv_metas.len() * kv_meta_size + self.data.len() + key.len() + value.len() + 12; // NodeMeta size

        if total_size > self.node_meta.node_size as usize {
            return false; // no space
        }

        // Append key and value data
        let offset = self.data.len() as u16;
        self.data.extend_from_slice(key);
        self.data.extend_from_slice(value);

        let new_kv = KVMeta::new(
            key.len() as u16, 
            value.len() as u16, 
            offset, 
            record_type_u8,
            false, 
            0, 
            0
        );

        // Insert in sorted order
        let pos = self.kv_metas.binary_search_by(|kv| {
            let k_start = kv.offset as usize;
            let k_end = k_start + kv.key_size as usize;
            self.data[k_start..k_end].cmp(key)
        }).unwrap_or_else(|e| e);
        self.kv_metas.insert(pos, new_kv);

        self.node_meta.record_count += 1;
        true
    }
}
