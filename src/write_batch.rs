use {WriteBatch, Error, ColumnFamily};
use libc::{size_t, c_char};
/// An atomic batch of write operations.
///
/// Making an atomic commit of several writes:
///
/// ```
/// use rocksdb::{DB, WriteBatch};
///
/// let db = DB::open_default("path/for/rocksdb/storage1").unwrap();
/// {
///     let mut batch = WriteBatch::default();
///     batch.put(b"my key", b"my value");
///     batch.put(b"key2", b"value2");
///     batch.put(b"key3", b"value3");
///     db.write(batch); // Atomically commits the batch
/// }
/// ```
impl WriteBatch {
    pub fn len(&self) -> usize {
        unsafe { ffi::rocksdb_writebatch_count(self.inner) as usize }
    }

    /// Return WriteBatch serialized size (in bytes).
    pub fn size_in_bytes(&self) -> usize {
        unsafe {
            let mut batch_size: size_t = 0;
            ffi::rocksdb_writebatch_data(self.inner, &mut batch_size);
            batch_size as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert a value into the database under the given key.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn put_cf(&mut self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_put_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_merge(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn merge_cf(&mut self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_merge_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    /// Remove the database entry for key.
    ///
    /// Returns an error if the key was not found.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn delete_cf(&mut self, cf: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_delete_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
            Ok(())
        }
    }
}

impl Default for WriteBatch {
    fn default() -> WriteBatch {
        WriteBatch { inner: unsafe { ffi::rocksdb_writebatch_create() } }
    }
}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_writebatch_destroy(self.inner) }
    }
}



#[test]
fn writebatch_works() {
    let path = "_rust_rocksdb_writebacktest";
    {
        let db = DB::open_default(path).unwrap();
        {
            // test put
            let mut batch = WriteBatch::default();
            assert!(db.get(b"k1").unwrap().is_none());
            assert_eq!(batch.len(), 0);
            assert!(batch.is_empty());
            let _ = batch.put(b"k1", b"v1111");
            assert_eq!(batch.len(), 1);
            assert!(!batch.is_empty());
            assert!(db.get(b"k1").unwrap().is_none());
            let p = db.write(batch);
            assert!(p.is_ok());
            let r: Result<Option<DBVector>, Error> = db.get(b"k1");
            assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        }
        {
            // test delete
            let mut batch = WriteBatch::default();
            let _ = batch.delete(b"k1");
            assert_eq!(batch.len(), 1);
            assert!(!batch.is_empty());
            let p = db.write(batch);
            assert!(p.is_ok());
            assert!(db.get(b"k1").unwrap().is_none());
        }
        {
            // test size_in_bytes
            let mut batch = WriteBatch::default();
            let before = batch.size_in_bytes();
            let _ = batch.put(b"k1", b"v1234567890");
            let after = batch.size_in_bytes();
            assert!(before + 10 <= after);
        }
    }
    let opts = Options::default();
    assert!(DB::destroy(&opts, path).is_ok());
}