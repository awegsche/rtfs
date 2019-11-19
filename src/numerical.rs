extern crate num;
use std::ops::{Add, Div, Mul, Sub};

pub trait IsNumeric: Add + Sub + Mul + Div + Sized + Copy {}

impl<T: Add + Sub + Mul + Div + Sized + Copy> IsNumeric for T {}

/// A convenience struct for storing numerical data
/// Todo: replace Vec<f64> in RealVector by NumericalVec<T>
pub struct NumericalVec<T>
where
    T: IsNumeric,
{
    data: Vec<T>,
}

impl<T: IsNumeric> From<Vec<T>> for NumericalVec<T> {
    fn from(v: Vec<T>) -> Self {
        NumericalVec { data: v }
    }
}

impl<'a, T> Add for &'a NumericalVec<T>
where
    T: IsNumeric,
    <T as Add>::Output: IsNumeric,
{
    type Output = NumericalVec<<T as Add>::Output>;

    fn add(self, b: Self) -> Self::Output {
        if self.data.len() != b.data.len() {
            panic!("vectors don't have the same lengths")
        } else {
            NumericalVec {
                data: self
                    .data
                    .iter()
                    .zip(b.data.iter())
                    .map(|(x, y)| *x + *y)
                    .collect(),
            }
        }
    }
}

impl<'a, T> Sub for &'a NumericalVec<T>
where
    T: IsNumeric,
    <T as Sub>::Output: IsNumeric,
{
    type Output = NumericalVec<<T as Sub>::Output>;

    fn sub(self, b: Self) -> Self::Output {
        if self.data.len() != b.data.len() {
            panic!("vectors don't have the same lengths")
        } else {
            NumericalVec {
                data: self
                    .data
                    .iter()
                    .zip(b.data.iter())
                    .map(|(x, y)| *x - *y)
                    .collect(),
            }
        }
    }
}

/*
impl<T> NumericalVec<T>
where
    T: IsNumeric,
    <T as Mul>::Output: IsNumeric,
    T: std::iter::Sum<<T as std::ops::Mul>::Output>,
{
    pub fn rms(&self) -> T {
        let f = 0.0;
        self.data.iter().map(|x| *x * *x).sum::<T>().sqrt()
    }
}
*/
