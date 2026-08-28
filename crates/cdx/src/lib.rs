//! Data models for web archive capture indexes.
//!
//! Format-specific models for classic CDX, CDXJ, and CDX server JSON live in
//! [`format`](mod@format). The other modules define representation-neutral capture data, field
//! names, extension properties, timestamps, and the CDX server's query model.
//!
//! Reading files, sorting indexes, looking up captures, and resolving WARC byte ranges belong to
//! higher-level crates.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod capture;
pub mod field;
pub mod format;
pub mod properties;
pub mod query;
pub mod timestamp;

#[cfg(test)]
mod strategies;
