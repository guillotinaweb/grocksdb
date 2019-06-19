use {TransactionDB, Error, Options, TransactionDBOptions, TransactionSnapshot, TransactionOptions, Transaction, IteratorMode, Direction, WriteBatch, ReadOptions, DBVector, TransactionDBIterator, TransactionDBRawIterator, WriteOptions, ColumnFamily};
use ffi;

use libc::{c_char, c_int, size_t};
use std::ffi::CString;
use std::fmt;
use std::fs;
use std::path::Path;
use std::str;
use std::ffi::CStr;
use std::slice;
use std::ptr;

pub fn new_bloom_filter(bits: c_int) -> *mut ffi::rocksdb_filterpolicy_t {
    unsafe { ffi::rocksdb_filterpolicy_create_bloom(bits) }
}

unsafe impl Send for TransactionDB {}
unsafe impl Sync for TransactionDB {}


impl TransactionDB {
   
    /// Open a database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<TransactionDB, Error> {
        let mut txopts = TransactionDBOptions::default();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        TransactionDB::open(&opts, &txopts, path)
    }

    /// Open a database with the given database options and column family names/options.
    pub fn open<P: AsRef<Path>>(opts: &Options, txopts: &TransactionDBOptions, path: P) -> Result<TransactionDB, Error> {
        let path = path.as_ref();
        let cpath = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening DB."
                        .to_owned(),
                ))
            }
        };

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB\
                                           directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_transactiondb_t;

        unsafe {
            db = ffi_try!(ffi::rocksdb_transactiondb_open(opts.inner, txopts.inner, cpath.as_ptr() as *const _,));
        }

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(TransactionDB {
            inner: db,
            path: path.to_path_buf(),
        })
    }

    pub fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        let cpath = match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening DB."
                        .to_owned(),
                ))
            }
        };

        let mut length = 0;

        unsafe {
            let ptr = ffi_try!(ffi::rocksdb_list_column_families(
                opts.inner,
                cpath.as_ptr() as *const _,
                &mut length,
            ));

            let vec = slice::from_raw_parts(ptr, length)
                .iter()
                .map(|ptr| CStr::from_ptr(*ptr).to_string_lossy().into_owned())
                .collect();
            ffi::rocksdb_list_column_families_destroy(ptr, length);
            Ok(vec)
        }
    }


    pub fn begin(&self, write_options: &WriteOptions, txn_options: &TransactionOptions) -> Result<Transaction, Error> {
        unsafe {
            let mut val_len: size_t = 0;
            let transaction: *mut ffi::rocksdb_transaction_t;
            let p: *mut ffi::rocksdb_transaction_t = ptr::null_mut();
            transaction = ffi::rocksdb_transaction_begin(self.inner, write_options.inner, txn_options.inner, p);
            println!("Transaction {:?}", ffi::get_transaction_id(transaction));
            if transaction.is_null() {
                return Err(Error::new(
                    "Failed to Create transaction"
                        .to_owned(),
                ))
            } else {
                Ok(Transaction {
                    inner: transaction
                })
            }
        }
    }

    pub fn begin_with_txn(&self, write_options: &WriteOptions, txn_options: &TransactionOptions, old_txn: &Transaction) -> Result<Transaction, Error> {
        unsafe {
            let mut val_len: size_t = 0;
            let transaction: *mut ffi::rocksdb_transaction_t;
            transaction = ffi::rocksdb_transaction_begin(self.inner, write_options.inner, txn_options.inner, old_txn.inner);
            if transaction.is_null() {
                return Err(Error::new(
                    "Failed to Create transaction"
                        .to_owned(),
                ))
            } else {
                Ok(Transaction {
                    inner: transaction,
                })
            }
        }
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }

    pub fn repair<P: AsRef<Path>>(opts: Options, path: P) -> Result<(), Error> {
        let cpath = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_repair_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }

    pub fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_write(self.inner, writeopts.inner, batch.inner,));
        }
        Ok(())
    }

    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        self.write_opt(batch, &WriteOptions::default())
    }

    pub fn write_without_wal(&self, batch: WriteBatch) -> Result<(), Error> {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        self.write_opt(batch, &wo)
    }

    pub fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DBVector>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                                   This is a fairly trivial call, and its \
                                   failure may be indicative of a \
                                   mis-compiled or mis-loaded RocksDB \
                                   library."
                    .to_owned(),
            ));
        }

        unsafe {
            let mut _val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transactiondb_get(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut _val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, _val_len)))
            }
        }
    }

    /// Return the bytes associated with a key value
    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.get_opt(key, &ReadOptions::default())
    }

    pub fn get_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DBVector>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                                   This is a fairly trivial call, and its \
                                   failure may be indicative of a \
                                   mis-compiled or mis-loaded RocksDB \
                                   library."
                    .to_owned(),
            ));
        }

        unsafe {
            let mut _val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transactiondb_get_cf(
                self.inner,
                readopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut _val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, _val_len)))
            }
        }
    }

    pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.get_cf_opt(cf, key, &ReadOptions::default())
    }

    pub fn create_cf(&mut self, name: &str, opts: &Options) -> Result<ColumnFamily, Error> {
        let cname = match CString::new(name.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening rocksdb"
                        .to_owned(),
                ))
            }
        };
        let cf = unsafe {
            let cf_handler = ffi_try!(ffi::rocksdb_transactiondb_create_column_family(
                self.inner,
                opts.inner,
                cname.as_ptr(),
            ));
            let cf = ColumnFamily { inner: cf_handler };
            cf
        };
        Ok(cf)
    }

    pub fn iterator(&self, mode: IteratorMode) -> TransactionDBIterator {
        let opts = ReadOptions::default();
        TransactionDBIterator::new(self, &opts, mode)
    }

    /// Opens an interator with `set_total_order_seek` enabled.
    /// This must be used to iterate across prefixes when `set_memtable_factory` has been called
    /// with a Hash-based implementation.
    pub fn full_iterator(&self, mode: IteratorMode) -> TransactionDBIterator {
        let mut opts = ReadOptions::default();
        opts.set_total_order_seek(true);
        TransactionDBIterator::new(self, &opts, mode)
    }

    pub fn prefix_iterator<'a>(&self, prefix: &'a [u8]) -> TransactionDBIterator {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        TransactionDBIterator::new(self, &opts, IteratorMode::From(prefix, Direction::Forward))
    }

    pub fn raw_iterator(&self) -> TransactionDBRawIterator {
        let opts = ReadOptions::default();
        TransactionDBRawIterator::new(self, &opts)
    }

    // pub fn snapshot(&self) -> TransactionSnapshot {
    //     TransactionSnapshot::new(self)
    // }

    pub fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_put(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_put_cf(
                self.inner,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_merge(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_delete(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_delete_cf(
                self.inner,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.put_opt(key, value, &WriteOptions::default())
    }

    pub fn put_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.put_cf_opt(cf, key, value, &WriteOptions::default())
    }

    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.merge_opt(key, value, &WriteOptions::default())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.delete_opt(key, &WriteOptions::default())
    }

    pub fn delete_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        self.delete_cf_opt(cf, key, &WriteOptions::default())
    }
}

impl Drop for TransactionDB {
    fn drop(&mut self) {
        unsafe {
            // for cf in self.cfs.values() {
            //     ffi::rocksdb_column_family_handle_destroy(cf.inner);
            // }
            ffi::rocksdb_transactiondb_close(self.inner);
        }
    }
}

impl fmt::Debug for TransactionDB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksDB {{ path: {:?} }}", self.path())
    }
}
