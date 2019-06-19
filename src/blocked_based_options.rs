use {BlockBasedOptions, BlockBasedIndexType};
use libc::{c_int, size_t, c_uchar};


pub fn new_cache(capacity: size_t) -> *mut ffi::rocksdb_cache_t {
    unsafe { ffi::rocksdb_cache_create_lru(capacity) }
}


impl Drop for BlockBasedOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_block_based_options_destroy(self.inner);
        }
    }
}


impl BlockBasedOptions {
    pub fn set_block_size(&mut self, size: usize) {
        unsafe {
            ffi::rocksdb_block_based_options_set_block_size(self.inner, size);
        }
    }

    pub fn set_lru_cache(&mut self, size: size_t) {
        let cache = new_cache(size);
        unsafe {
            // Since cache is wrapped in shared_ptr, we don't need to
            // call rocksdb_cache_destroy explicitly.
            ffi::rocksdb_block_based_options_set_block_cache(self.inner, cache);
        }
    }

    pub fn disable_cache(&mut self) {
        unsafe {
            ffi::rocksdb_block_based_options_set_no_block_cache(self.inner, true as c_uchar);
        }
    }

    pub fn set_bloom_filter(&mut self, bits_per_key: c_int, block_based: bool) {
        unsafe {
            let bloom = if block_based {
                ffi::rocksdb_filterpolicy_create_bloom(bits_per_key)
            } else {
                ffi::rocksdb_filterpolicy_create_bloom_full(bits_per_key)
            };

            ffi::rocksdb_block_based_options_set_filter_policy(self.inner, bloom);
        }
    }

    pub fn set_cache_index_and_filter_blocks(&mut self, v: bool) {
        unsafe {
            ffi::rocksdb_block_based_options_set_cache_index_and_filter_blocks(self.inner, v as u8);
        }
    }

    /// Defines the index type to be used for SS-table lookups.
    ///
    /// # Example
    ///
    /// ```
    /// use rocksdb::{BlockBasedOptions, BlockBasedIndexType, Options};
    ///
    /// let mut opts = Options::default();
    /// let mut block_opts = BlockBasedOptions::default();
    /// block_opts.set_index_type(BlockBasedIndexType::HashSearch);
    /// ```
    pub fn set_index_type(&mut self, index_type: BlockBasedIndexType) {
        let index = index_type as i32;
        unsafe {
            ffi::rocksdb_block_based_options_set_index_type(self.inner, index);
        }
    }
}

impl Default for BlockBasedOptions {
    fn default() -> BlockBasedOptions {
        let block_opts = unsafe { ffi::rocksdb_block_based_options_create() };
        if block_opts.is_null() {
            panic!("Could not create RocksDB block based options");
        }
        BlockBasedOptions { inner: block_opts }
    }
}