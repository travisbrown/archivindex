use chrono::{DateTime, TimeZone, Utc};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBIteratorWithThreadMode, IteratorMode, Options,
    Transaction, TransactionDB, TransactionDBOptions,
};
use std::path::Path;
use std::sync::Arc;

const TWEET0_CF_NAME: &str = "tweet0";
const TWEET1_CF_NAME: &str = "tweet1";
const USER0_CF_NAME: &str = "user0";
const RETWEET0_CF_NAME: &str = "retweet0";
const RETWEET1_CF_NAME: &str = "retweet1";
const REPLY0_CF_NAME: &str = "reply0";
const REPLY1_CF_NAME: &str = "reply1";
const QUOTE0_CF_NAME: &str = "quote0";
const QUOTE1_CF_NAME: &str = "quote1";
const MENTION0_CF_NAME: &str = "mention0";
const MENTION1_CF_NAME: &str = "mention1";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RocksDB error")]
    Db(#[from] rocksdb::Error),
    #[error("Invalid key")]
    InvalidKey(Vec<u8>),
    #[error("Invalid value")]
    InvalidValue(Vec<u8>),
    #[error("Duplicate values")]
    DuplicateValues {
        key: u64,
        old: Vec<u8>,
        new: Vec<u8>,
    },
}
#[derive(Clone)]
pub struct Database {
    db: Arc<TransactionDB>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_missing_column_families(true);
        options.create_if_missing(true);

        let transactions_options = TransactionDBOptions::new();

        let tweet0_cf = ColumnFamilyDescriptor::new(TWEET0_CF_NAME, Self::default_cf_options());
        let tweet1_cf = ColumnFamilyDescriptor::new(TWEET1_CF_NAME, Self::default_cf_options());
        let user0_cf = ColumnFamilyDescriptor::new(USER0_CF_NAME, Self::default_cf_options());
        let retweet0_cf = ColumnFamilyDescriptor::new(RETWEET0_CF_NAME, Self::default_cf_options());
        let retweet1_cf = ColumnFamilyDescriptor::new(RETWEET1_CF_NAME, Self::default_cf_options());
        let reply0_cf = ColumnFamilyDescriptor::new(REPLY0_CF_NAME, Self::default_cf_options());
        let reply1_cf = ColumnFamilyDescriptor::new(REPLY1_CF_NAME, Self::default_cf_options());
        let quote0_cf = ColumnFamilyDescriptor::new(QUOTE0_CF_NAME, Self::default_cf_options());
        let quote1_cf = ColumnFamilyDescriptor::new(QUOTE1_CF_NAME, Self::default_cf_options());
        let mention0_cf = ColumnFamilyDescriptor::new(MENTION0_CF_NAME, Self::default_cf_options());
        let mention1_cf = ColumnFamilyDescriptor::new(MENTION1_CF_NAME, Self::default_cf_options());

        let cfs = vec![
            tweet0_cf,
            tweet1_cf,
            user0_cf,
            retweet0_cf,
            retweet1_cf,
            reply0_cf,
            reply1_cf,
            quote0_cf,
            quote1_cf,
            mention0_cf,
            mention1_cf,
        ];

        let db = TransactionDB::open_cf_descriptors(&options, &transactions_options, path, cfs)?;

        Ok(Self { db: Arc::new(db) })
    }

    fn default_cf_options() -> Options {
        Options::default()
    }

    /// Panics on invalid name.
    ///
    /// Only for internal use.
    fn cf_handle(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }
}
