use {Options, ColumnFamily, ColumnFamilyDescriptor};


unsafe impl Send for ColumnFamily {}


impl ColumnFamilyDescriptor {
    // Create a new column family descriptor with the specified name and options.
    pub fn new<S>(name: S, options: Options) -> Self where S: Into<String> {
        ColumnFamilyDescriptor {
            name: name.into(),
            options
        }
    }
}
