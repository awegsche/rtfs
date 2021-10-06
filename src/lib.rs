//! # Introduction
//!
//! This crate provides simple I/O of TFS files. Basic dataframe features are also included.
//!
//! # Starting Points
//!
//! - The documentation of [`TfsDataFrame`](tfsdataframe/struct.TfsDataFrame.html) provides examples and API reference
//! for the main struct.
//!
//! - The dataframe namespace (see below) contains a very general trait `DataFrame` that has to be implemented
//! by all dataframe-like objects.
pub mod dataframe;
pub mod join;
pub mod numerical;
pub mod tfsdataframe;

pub use dataframe::*;
pub use tfsdataframe::*;

// The following is tests

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn loading() {
        assert!(TfsDataFrame::<f32>::open("not_there").is_err());

        assert!(TfsDataFrame::<f32>::open("test/test.tfs").is_ok());
    }

    #[test]
    fn load_all_data() {
        assert_eq!(TfsDataFrame::<f32>::open_expect("test/test.tfs").len(), 5);
    }
}
