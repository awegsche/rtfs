extern crate num_traits;

use num_traits::Float;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Div, Mul, Sub};

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

#[derive(Debug)]
pub enum DataValue<T> {
    Text(String),
    Real(T),
    //Complex(c128),
}

impl<T: fmt::Display> fmt::Display for DataValue<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::Text(s) => write!(f, "'{}'", s),
            DataValue::Real(r) => write!(f, "{}", r),
        }
    }
}

#[derive(Debug)]
pub enum DataView<'a, T> {
    Text(&'a String),
    Real(&'a T),
    //Complex(c128),
}

impl<'a, T: Display> Display for DataView<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use DataView::*;
        match self {
            Text(t) => write!(f, "{}", t),
            Real(r) => write!(f, "{}", r),
        }
    }
}

impl<T> Into<String> for DataValue<T> {
    fn into(self) -> String {
        if let DataValue::Text(t) = self {
            t
        } else {
            panic!("The data value is not a Text");
        }
    }
}

macro_rules! impl_data_into {
    ($a:ident) => {
        impl<T: Into<$a>> Into<$a> for DataValue<T> {
            fn into(self) -> $a {
                if let DataValue::Real(r) = self {
                    r.into()
                } else {
                    panic!("The data value is not a real value")
                }
            }
        }
    };
}

impl_data_into!(f64);
impl_data_into!(f32);

//impl<T: Into<f64>> Into<f64> for DataValue<T> {
//    fn into(self) -> f64 {
//        if let DataValue::Real(r) = self {
//            r.into()
//        } else {
//            panic!("The data value is not a real number");
//        }
//    }
//}

impl<'a, T> Into<&'a String> for DataView<'a, T> {
    fn into(self) -> &'a String {
        if let DataView::Text(t) = self {
            t
        } else {
            panic!("The data value is not a Text");
        }
    }
}

impl<'a, T: Copy + Into<f64>> Into<f64> for DataView<'a, T> {
    fn into(self) -> f64 {
        if let DataView::Real(r) = self {
            (*r).into()
        } else {
            panic!("The data value is not a real number");
        }
    }
}

/// The columns of a data frame are stored in `DataVector`s.
#[derive(PartialEq)]
pub enum DataVector<T> {
    TextVector(Vec<String>),
    RealVector(Vec<T>),
}

//impl<'a, T> Into<&'a Vec<T>> for &'a DataVector<T> {
//    fn into(self) -> &'a Vec<T> {
//        if let DataVector::RealVector(v) = self {
//            &v
//        } else {
//            panic!("not a RealVector")
//        }
//    }
//}
//

macro_rules! impl_datavec_into {
    ($a:ident) => {
        impl<'a> Into<&'a Vec<$a>> for &'a DataVector<$a> {
            fn into(self) -> &'a Vec<$a> {
                if let DataVector::RealVector(v) = self {
                    &v
                } else {
                    panic!("The data value is not a real value")
                }
            }
        }
    };
}

impl_datavec_into!(f64);
impl_datavec_into!(f32);

impl<'a, T> Into<&'a Vec<String>> for &'a DataVector<T> {
    fn into(self) -> &'a Vec<String> {
        if let DataVector::TextVector(v) = self {
            &v
        } else {
            panic!("not a TextVector")
        }
    }
}

impl<'a, T> Add for &'a DataVector<T>
where
    T: Copy + Add + From<<T as Add>::Output>,
{
    type Output = DataVector<T>;

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
    fn add(self, other: &'a DataVector<T>) -> DataVector<T> {
        if let &DataVector::RealVector(ref a) = self {
            if let &DataVector::RealVector(ref b) = other {
                DataVector::RealVector(
                    a.iter()
                        .zip(b.iter())
                        .map(|(x, y)| T::from(*x + *y))
                        .collect::<Vec<T>>(),
                )
            } else {
                panic!("rhs has to be data")
            }
        } else {
            panic!("lhs has to be data")
        }
    }
}

impl<'a, T> Sub for &'a DataVector<T>
where
    T: Copy + Sub + From<<T as Sub>::Output>,
{
    type Output = DataVector<T>;

    /// Implementation for Subtraction of two `DataVector`s.
    /// ```
    /// use tfs::DataVector;
    ///
    /// let a = DataVector::RealVector((0..100).map(|i| i as f64).collect::<Vec<_>>());
    /// let b = DataVector::RealVector((0..100).map(|_| 1.0).collect::<Vec<_>>());
    ///
    /// let c = &a - &b;
    /// ```
    fn sub(self, other: &'a DataVector<T>) -> DataVector<T> {
        if let &DataVector::RealVector(ref a) = self {
            if let &DataVector::RealVector(ref b) = other {
                if a.len() == b.len() {
                    DataVector::RealVector(
                        a.iter()
                            .zip(b.iter())
                            .map(|(x, y)| T::from(*x - *y))
                            .collect::<Vec<T>>(),
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

impl<T: Debug> Debug for DataVector<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataVector::RealVector(v) => {
                write!(f, "RealVector[{}] {{ ", v.len())?;
                for i in 0..v.len().min(5) {
                    write!(f, "{:?}, ", v[i])?;
                }
                write!(f, "}}")?;
            }
            DataVector::TextVector(v) => {
                write!(f, "TextVector[{}] {{ ", v.len())?;
                for i in 0..v.len().min(5) {
                    write!(f, "'{:?}', ", v[i])?;
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
