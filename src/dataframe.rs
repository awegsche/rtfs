use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Div, Mul, Sub};

/// The necessary functions to implement for any DataFrame object.
///
/// For now this includes:
/// * `col(colname)` to get the reference to a certain column
/// * `loc(key, colname)` to get the reference to a certain cell
pub trait DataFrame {
    /// Returns a reference to the column `column`.
    fn col<'a>(&self, column: &str) -> &DataVector;

    /// Returns a DataView to the cell at column `column` and index `key`.
    fn loc<'a, Key>(&self, key: Key, column: &str) -> DataView
    where
        Key: Into<Indexer<'a>>;

    /// Convenience function for accessing a real number cell. Will panic if anything goes wrong
    /// (the cell doesn't exist, it is not a real number etc.)
    fn loc_real<'a, Key>(&self, key: Key, column: &str) -> &f64
    where
        Key: Into<Indexer<'a>>,
    {
        if let DataView::Real(ref v) = self.loc(key, column) {
            v
        } else {
            panic!("couldn't find the value");
        }
    }

    /// Convenience function for accessing a text cell. Will panic if anything goes wrong
    /// (the cell doesn't exist, it is not a text etc.)
    fn loc_text<'a, Key>(&self, key: Key, column: &str) -> &String
    where
        Key: Into<Indexer<'a>>,
    {
        if let DataView::Text(ref v) = self.loc(key, column) {
            v
        } else {
            panic!("couldn't find the value");
        }
    }

    fn col_real<'a>(&self, column: &str) -> &Vec<f64> {
        if let DataVector::RealVector(v) = self.col(column) {
            v
        } else {
            panic!("couldn't find the data column")
        }
    }
    fn col_text<'a>(&self, column: &str) -> &Vec<String> {
        if let DataVector::TextVector(v) = self.col(column) {
            &v
        } else {
            panic!("couldn't find the data column")
        }
    }
}

/// Helps deciding if we access by key (a valid String index has to be setup with `set_index`) or
/// by an integer index
pub enum Indexer<'a> {
    Index(usize),
    Key(&'a str),
}

impl<'a> From<&'a str> for Indexer<'a> {
    fn from(string: &'a str) -> Self {
        Indexer::Key(string)
    }
}

impl<'a> From<&'a String> for Indexer<'a> {
    fn from(string: &'a String) -> Self {
        Indexer::Key(string)
    }
}

// the following can be replaced by a macro

impl<'a> From<usize> for Indexer<'a> {
    fn from(idx: usize) -> Self {
        Indexer::Index(idx)
    }
}
impl<'a> From<i32> for Indexer<'a> {
    fn from(idx: i32) -> Self {
        Indexer::Index(idx as usize)
    }
}
impl<'a> From<u32> for Indexer<'a> {
    fn from(idx: u32) -> Self {
        Indexer::Index(idx as usize)
    }
}

pub enum DataValue {
    Text(String),
    Real(f64),
    //Complex(c128),
}

#[derive(Debug)]
pub enum DataView<'a> {
    Text(&'a String),
    Real(&'a f64),
    //Complex(c128),
}

impl<'a> Display for DataView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use DataView::*;
        match self {
            Text(t) => write!(f, "{}", t),
            Real(r) => write!(f, "{}", r),
        }
    }
}

impl Into<String> for DataValue {
    fn into(self) -> String {
        if let DataValue::Text(t) = self {
            t
        } else {
            panic!("The data value is not a Text");
        }
    }
}

impl Into<f64> for DataValue {
    fn into(self) -> f64 {
        if let DataValue::Real(r) = self {
            r
        } else {
            panic!("The data value is not a real number");
        }
    }
}

impl<'a> Into<&'a String> for DataView<'a> {
    fn into(self) -> &'a String {
        if let DataView::Text(t) = self {
            t
        } else {
            panic!("The data value is not a Text");
        }
    }
}

impl<'a> Into<&'a f64> for DataView<'a> {
    fn into(self) -> &'a f64 {
        if let DataView::Real(r) = self {
            r
        } else {
            panic!("The data value is not a real number");
        }
    }
}

/// The columns of a data frame are stored in `DataVector`s.
#[derive(PartialEq)]
pub enum DataVector {
    TextVector(Vec<String>),
    RealVector(Vec<f64>),
}

impl<'a> Into<&'a Vec<f64>> for &'a DataVector {
    fn into(self) -> &'a Vec<f64> {
        if let DataVector::RealVector(v) = self {
            &v
        } else {
            panic!("not a RealVector")
        }
    }
}

impl<'a> Into<&'a Vec<String>> for &'a DataVector {
    fn into(self) -> &'a Vec<String> {
        if let DataVector::TextVector(v) = self {
            &v
        } else {
            panic!("not a TextVector")
        }
    }
}

impl<'a> Add for &'a DataVector {
    type Output = DataVector;

    /// Implementation for Addition of two `DataVector`s.
    /// Yields element-wise addition of the two Vectors if they are both `DataVector::RealVector`.
    /// ```
    /// # use tfs::DataVector;
    ///
    /// let a = DataVector::RealVector((0..100).map(|i| i as f64).collect::<Vec<f64>>());
    /// let b = DataVector::RealVector((0..100).map(|_| 1.0).collect::<Vec<f64>>());
    ///
    /// let c = &a + &b;
    ///
    /// let test_c = DataVector::RealVector((0..100).map(|i| i as f64 + 1.0).collect::<Vec<f64>>());
    ///
    /// assert_eq!(c, test_c);
    /// ```
    fn add(self, other: &'a DataVector) -> DataVector {
        if let &DataVector::RealVector(ref a) = self {
            if let &DataVector::RealVector(ref b) = other {
                DataVector::RealVector(
                    a.iter()
                        .zip(b.iter())
                        .map(|(x, y)| x + y)
                        .collect::<Vec<f64>>(),
                )
            } else {
                panic!("rhs has to be data")
            }
        } else {
            panic!("lhs has to be data")
        }
    }
}

impl<'a> Sub for &'a DataVector {
    type Output = DataVector;

    /// Implementation for Subtraction of two `DataVector`s.
    /// ```
    /// use tfs::DataVector;
    ///
    /// let a = DataVector::RealVector((0..100).map(|i| i as f64).collect::<Vec<_>>());
    /// let b = DataVector::RealVector((0..100).map(|_| 1.0).collect::<Vec<_>>());
    ///
    /// let c = &a - &b;
    /// ```
    fn sub(self, other: &'a DataVector) -> DataVector {
        if let &DataVector::RealVector(ref a) = self {
            if let &DataVector::RealVector(ref b) = other {
                if a.len() == b.len() {
                    DataVector::RealVector(
                        a.iter()
                            .zip(b.iter())
                            .map(|(x, y)| x - y)
                            .collect::<Vec<f64>>(),
                    )
                } else {
                    panic!("Vectors have to have the same length")
                }
            } else {
                panic!("rhs has to be data")
            }
        } else {
            panic!("lhs has to be data")
        }
    }
}

impl Debug for DataVector {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataVector::RealVector(v) => {
                write!(f, "RealVector[{}] {{ ", v.len())?;
                for i in 0..v.len().min(5) {
                    write!(f, "{}, ", v[i])?;
                }
                write!(f, "}}")?;
            }
            DataVector::TextVector(v) => {
                write!(f, "TextVector[{}] {{ ", v.len())?;
                for i in 0..v.len().min(5) {
                    write!(f, "'{}', ", v[i])?;
                }
                write!(f, "}}")?;
            }
        }
        Ok(())
    }
}

/*
impl Iterator for (DataVector, DataVector) {
    type Item = (DataValue, DataValue);
    fn next(&mut self) -> Option<Self::Item> {
        let (A, B) = self;
        (A.next(), B.next())
    }
}

impl<A, B> Iterator for (A, B)
where
    A: Iterator,
    B: Iterator,
{
    type Item = (A, B);
    fn next(&mut self) -> Option<Self::Item> {
        let (A, B) = self;
        (A.next(), B.next())
    }
}
*/
