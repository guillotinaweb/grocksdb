use libc::{c_uchar, size_t, int64_t};

use ffi;
use {TransactionOptions};

unsafe impl Send for TransactionOptions {}

impl Drop for TransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_options_destroy(self.inner);
        }
    }
}

impl TransactionOptions {
    pub fn new() -> TransactionOptions {
        TransactionOptions::default()
    }

    pub fn set_snapshot(&mut self, value: bool) {
        unsafe {
            ffi::rocksdb_transaction_options_set_set_snapshot(self.inner, value as c_uchar);
        }
    }

    pub fn set_deadlock_detect(&mut self, value: bool) {
        unsafe {
            ffi::rocksdb_transaction_options_set_deadlock_detect(self.inner, value as c_uchar);
        }
    }

    pub fn set_lock_timeout(&mut self, value: usize) {
        unsafe {
            ffi::rocksdb_transaction_options_set_lock_timeout(self.inner, value as int64_t);
        }
    }

    pub fn set_expiration(&mut self, value: usize) {
        unsafe {
            ffi::rocksdb_transaction_options_set_expiration(self.inner, value as int64_t);
        }
    }

    pub fn set_deadlock_detect_depth(&mut self, value: usize) {
        unsafe {
            ffi::rocksdb_transaction_options_set_deadlock_detect_depth(self.inner, value as int64_t);
        }
    }

    pub fn set_max_write_batch_size(&mut self, value: size_t) {
        unsafe {
            ffi::rocksdb_transaction_options_set_max_write_batch_size(self.inner, value as size_t);
        }
    }
}


impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        let transaction_opts = unsafe { ffi::rocksdb_transaction_options_create() };
        if transaction_opts.is_null() {
            panic!("Could not create RocksDB transaction options");
        }
        TransactionOptions { inner: transaction_opts }
    }
}

