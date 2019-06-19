use {TransactionDBRawIterator, TransactionDBIterator, Direction, ColumnFamily, ReadOptions, Error, DBVector, TransactionSnapshot, Transaction};
use libc::{c_uchar, size_t, c_char};

unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

impl Transaction {

    pub fn get_id(&mut self) -> Result<u64, Error> {
        unsafe {
            let tid = ffi::get_transaction_id(self.inner);
            Ok(tid)
        }
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(
                self.inner,
            ));
            Ok(())
        }
    }

    pub fn rollback(&mut self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_rollback(
                self.inner,
            ));
            Ok(())
        }
    }

    pub fn set_savepoint(&mut self)  -> Result<(), Error>{
        unsafe {
            ffi::rocksdb_transaction_set_savepoint(self.inner);
            Ok(())
        }
    }

    pub fn rollback_to_savepoint(&mut self) -> Result<(), Error>{
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_rollback_to_savepoint(
                self.inner,
            ));
            Ok(())
        }
    }

    pub fn destroy(&mut self) -> Result<(), Error>{
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
            Ok(())
        }
    }

    pub fn get_snapshot(&self) -> Result<TransactionSnapshot, Error> {
        unsafe {
            Ok(TransactionSnapshot{
                inner: ffi::rocksdb_transaction_get_snapshot(self.inner),
            })
        }
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
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

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
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_cf(
                self.inner,
                readopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.get_cf_opt(cf, key, &ReadOptions::default())
    }

    pub fn get_for_update_opt(&self, key: &[u8], readopts: &ReadOptions, exclusive: bool) -> Result<Option<DBVector>, Error> {
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
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_for_update(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
                exclusive as c_uchar,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn get_for_update(&self, key: &[u8], exclusive: bool) -> Result<Option<DBVector>, Error> {
        self.get_for_update_opt(key, &ReadOptions::default(), exclusive)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if key.len() == 0 {
            return Err(Error::new(
                format!("Invalid key").to_owned(),
            ));
        }
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if key.len() == 0 {
            return Err(Error::new(
                format!("Invalid key").to_owned(),
            ));
        }
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
       unsafe {
            ffi_try!(ffi::rocksdb_transaction_merge(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
       unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        if key.len() == 0 {
            return Err(Error::new(
                format!("Invalid key").to_owned(),
            ));
        }

        unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn create_iterator(&self, readopts: &ReadOptions) -> TransactionDBIterator {
        unsafe {
            TransactionDBIterator {
                raw: TransactionDBRawIterator { inner: ffi::rocksdb_transaction_create_iterator(self.inner, readopts.inner) },
                direction: Direction::Forward,
                just_seeked: false,
            }
        }
    }

    pub fn create_iterator_cf(&self, readopts: &ReadOptions, cf: ColumnFamily) -> TransactionDBIterator {
        unsafe {
            TransactionDBIterator {
                raw: TransactionDBRawIterator { inner: ffi::rocksdb_transaction_create_iterator_cf(self.inner, readopts.inner, cf.inner) },
                direction: Direction::Forward,
                just_seeked: false,
            }
        }
    }
}