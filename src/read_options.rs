use {ReadOptions, Snapshot};
use libc::{c_char, size_t, c_uchar};

impl Drop for ReadOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_readoptions_destroy(self.inner) }
    }
}

impl ReadOptions {
    // TODO add snapshot setting here
    // TODO add snapshot wrapper structs with proper destructors;
    // that struct needs an "iterator" impl too.
    #[allow(dead_code)]
    fn fill_cache(&mut self, v: bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_fill_cache(self.inner, v as c_uchar);
        }
    }

    pub fn set_snapshot(&mut self, snapshot: &Snapshot) {
        unsafe {
            ffi::rocksdb_readoptions_set_snapshot(self.inner, snapshot.inner);
        }
    }

    pub fn set_iterate_upper_bound(&mut self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_readoptions_set_iterate_upper_bound(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    pub fn set_prefix_same_as_start(&mut self, v: bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_prefix_same_as_start(self.inner, v as c_uchar)
        }
    }

    pub fn set_total_order_seek(&mut self, v:bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_total_order_seek(self.inner, v as c_uchar)
        }
    }
}

impl Default for ReadOptions {
    fn default() -> ReadOptions {
        unsafe { ReadOptions { inner: ffi::rocksdb_readoptions_create() } }
    }
}
