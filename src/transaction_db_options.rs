use libc::{size_t, int64_t};

use ffi;
use {TransactionDBOptions};

unsafe impl Send for TransactionDBOptions {}

impl Drop for TransactionDBOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_options_destroy(self.inner);
        }
    }
}

impl TransactionDBOptions {

    pub fn set_max_num_locks(&mut self, size: usize) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_max_num_locks(self.inner, size as int64_t);
        }
    }

    pub fn set_num_stripes(&mut self, num_stripes: size_t) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_num_stripes(self.inner, num_stripes as size_t);
        }
    }

    pub fn set_transaction_lock_timeout(&mut self, timeout: usize) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_transaction_lock_timeout(self.inner, timeout as int64_t);
        }
    }

    pub fn set_default_lock_timeout(&mut self,  default_timeout: usize) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_default_lock_timeout(self.inner, default_timeout as int64_t);
        }
    }

}

impl Default for TransactionDBOptions {
    fn default() -> TransactionDBOptions {
        let transactiondb_opts = unsafe { ffi::rocksdb_transactiondb_options_create() };
        if transactiondb_opts.is_null() {
            panic!("Could not create RocksDB transaction db options");
        }
        TransactionDBOptions { inner: transactiondb_opts }
    }
}
