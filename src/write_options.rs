use {WriteOptions};
use libc::{c_int, c_uchar};

/// Optionally disable WAL or sync for this write.
///
/// # Examples
///
/// Making an unsafe write of a batch:
///
/// ```
/// use rocksdb::{DB, WriteBatch, WriteOptions};
///
/// let db = DB::open_default("path/for/rocksdb/storageY").unwrap();
///
/// let mut batch = WriteBatch::default();
/// batch.put(b"my key", b"my value");
/// batch.put(b"key2", b"value2");
/// batch.put(b"key3", b"value3");
///
/// let mut write_options = WriteOptions::default();
/// write_options.set_sync(false);
/// write_options.disable_wal(true);
///
/// db.write_opt(batch, &write_options);
/// ```

impl Drop for WriteOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_writeoptions_destroy(self.inner);
        }
    }
}


impl WriteOptions {
    pub fn new() -> WriteOptions {
        WriteOptions::default()
    }

    pub fn set_sync(&mut self, sync: bool) {
        unsafe {
            ffi::rocksdb_writeoptions_set_sync(self.inner, sync as c_uchar);
        }
    }

    pub fn disable_wal(&mut self, disable: bool) {
        unsafe {
            ffi::rocksdb_writeoptions_disable_WAL(self.inner, disable as c_int);
        }
    }
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        let write_opts = unsafe { ffi::rocksdb_writeoptions_create() };
        if write_opts.is_null() {
            panic!("Could not create RocksDB write options");
        }
        WriteOptions { inner: write_opts }
    }
}