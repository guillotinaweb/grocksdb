
use {Error, DB, ColumnFamily, DBIterator, Snapshot, ReadOptions, DBVector, DBRawIterator, IteratorMode};

/// A consistent view of the database at the point of creation.
///
/// ```
/// use rocksdb::{DB, IteratorMode};
///
/// let db = DB::open_default("path/for/rocksdb/storage3").unwrap();
/// let snapshot = db.snapshot(); // Creates a longer-term snapshot of the DB, but closed when goes out of scope
/// let mut iter = snapshot.iterator(IteratorMode::Start); // Make as many iterators as you'd like from one snapshot
/// ```
///

impl<'a> Snapshot<'a> {
    pub fn new(db: &DB) -> Snapshot {
        let snapshot = unsafe { ffi::rocksdb_create_snapshot(db.inner) };
        Snapshot {
            db: db,
            inner: snapshot,
        }
    }

    pub fn iterator(&self, mode: IteratorMode) -> DBIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBIterator::new(self.db, &readopts, mode)
    }

    pub fn iterator_cf(
        &self,
        cf_handle: ColumnFamily,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBIterator::new_cf(self.db, cf_handle, &readopts, mode)
    }

    pub fn raw_iterator(&self) -> DBRawIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBRawIterator::new(self.db, &readopts)
    }

    pub fn raw_iterator_cf(&self, cf_handle: ColumnFamily) -> Result<DBRawIterator, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBRawIterator::new_cf(self.db, cf_handle, &readopts)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        self.db.get_opt(key, &readopts)
    }

    pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        self.db.get_cf_opt(cf, key, &readopts)
    }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_release_snapshot(self.db.inner, self.inner);
        }
    }
}


#[test]
fn snapshot_test() {
    let path = "_rust_rocksdb_snapshottest";
    {
        let db = DB::open_default(path).unwrap();
        let p = db.put(b"k1", b"v1111");
        assert!(p.is_ok());

        let snap = db.snapshot();
        let r: Result<Option<DBVector>, Error> = snap.get(b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");

        let p = db.put(b"k2", b"v2222");
        assert!(p.is_ok());

        assert!(db.get(b"k2").unwrap().is_some());
        assert!(snap.get(b"k2").unwrap().is_none());
    }
    let opts = Options::default();
    assert!(DB::destroy(&opts, path).is_ok());
}
