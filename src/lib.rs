use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::path::Path;
use std::ops::{Index, IndexMut};
use std::fmt;
use std::fmt::{Display, Debug, Formatter};


/// The necessary functions to implement for any DataFrame object
/// for now this includes:
/// * `loc(key, column)` to access a certain element
/// * `col(colname)` to get the reference to a certain column
pub trait DataFrame<Key> {

    /// Returns the element at column 'column' and row 'key' of a known data column (data being of type f64 usually).
    /// Returns `Err("Column 'column' is not a data column")` if COLNAME exists in the dataframe but is not a data column
    /// but a string column.
    fn locd(&self, key: Key, column: &str) -> Result<f64, &str>;

    /// Returns the element at column 'column' and row 'key' of a known string column (data being of type f64 usually).
    /// Returns Err("Column 'COLNAME' is not a data column") if COLNAME exists in the dataframe but is not a string column
    /// but a data column.
    fn locs(&self, key: Key, column: &str) -> Result<&String, &str>;

    /// Returns the column 'column' if it exists in the data frame and is indeed a data column.
    fn cold(&self, column: &str) -> Result<&Vec<f64>, &str>;

    /// Returns the column 'column' if it exists in the data frame and is indeed a string column.
    fn cols(&self, column: &str) -> Result<&Vec<String>, &str>;

}



/// A DataFrame object that holds the data in a 2D matrix and some property entries from the header.  
/// _Note_: this should later be replaced by a real dataframe and the tfs module should just copy data
/// into the dataframe and extend it by the header information.
pub struct TfsDataFrame{
    columns: Vec<TfsDataVector>,
    column_headers: HashMap<String, usize>,
    properties: HashMap<String, TfsProperty>,
    index_str: HashMap<String, usize>,
    colnames: Vec<String>,
    coltypes: Vec<String>,
}

/// Data column that can hold either a column of strings or data values (with type f64 for the moment)
pub enum TfsDataVector {
    FloatVec(Vec<f64>),
    StringVec(Vec<String>),
}

/// An entry of the TFS header, defined by
/// PROPERTYNAME %<type> VALUE.
pub enum TfsProperty {
    Text(String),
    Value(f64),
}

/// A viewer into a TFS cell. This can either be a `String` or a data value (`f64`)
pub enum TfsValue<'a> {
    Text(&'a String),
    Float(&'a f64),
}

impl TfsDataFrame {

    /// Opens a tfs file and stores the content in a TfsDataFrame.
    pub fn open<P>(path: P) -> TfsDataFrame 
    where P: AsRef<Path> {
        let mut reader = BufReader::new(File::open(path).unwrap()).lines();

        let mut df = TfsDataFrame{
            columns: Vec::new(),
            column_headers: HashMap::new(),
            properties: HashMap::new(),
            index_str: HashMap::new(),
            colnames: Vec::new(),
            coltypes: Vec::new(),
        };

        loop {
            let line = reader.next().unwrap().unwrap();
             
            let mut line_it = line.split_whitespace();

            match line_it.next().unwrap() {
                "*" => df.colnames.extend(line_it.map(|x| String::from(x))),
                "$" => df.coltypes.extend(line_it.map(|x| String::from(x))),
                "@" => { 
                    let name = String::from(line_it.next().unwrap());
                    match line_it.next().unwrap() {
                        "%le" => df.properties.insert(name, TfsProperty::Value(line_it.next().unwrap().parse().unwrap())),
                        _ => df.properties.insert(name, TfsProperty::Text(line_it.collect())),
                    };
                },
                _ => {},
            }
            if df.colnames.len() > 0 && df.coltypes.len() > 0 {
                break; // we have parsed the header, pass on to reading the data lines
            }
        }

        // setup columns
        for (ia, ib) in df.colnames.iter().zip(df.coltypes.iter()) {
            df.column_headers.insert(String::from(ia), df.columns.len());
            match ib.as_ref() {
                "%le" => df.columns.push(TfsDataVector::FloatVec(Vec::new())),
                _ => df.columns.push(TfsDataVector::StringVec(Vec::new())),
            };
        }

        for line in reader {
            if let Ok(l) = line {
                let line_it = l.split_whitespace();
                for (idata, icolumn) in line_it.into_iter().zip(df.columns.iter_mut()) {
                    match icolumn {
                        TfsDataVector::FloatVec(ref mut vec) => vec.push((*idata).parse().unwrap()),
                        TfsDataVector::StringVec(ref mut vec) => vec.push(String::from(idata)),
                    }
                }
            }

        }

        df
    }



}

impl DataFrame<usize> for TfsDataFrame {
    fn locd(&self, key: usize, column: &str) -> Result<f64, &str> {
        if let TfsDataVector::FloatVec(ref vec) = self.columns[self.column_headers[column]] {
            println!("trying to get element {} of FloatVec(len={})", key, vec.len());
            Ok(vec[key])
        }
        else {
            Err("The column '{}' is not a float column")
        }
    }
     fn cold(&self, column: &str) -> Result<&Vec<f64>, &str> {
        if let TfsDataVector::FloatVec(ref vec) = self.columns[self.column_headers[column]] {
            Ok(vec)
        }
        else {
            Err("The column '{}' is not a float column")
        }
    }
    fn locs(&self, key: usize, column: &str) -> Result<&String, &str> {
        if let TfsDataVector::StringVec(ref vec) = self.columns[self.column_headers[column]] {
            Ok(&vec[key])
        }
        else {
            Err("The column '{}' is not a string column")
        }
    }
     fn cols(&self, column: &str) -> Result<&Vec<String>, &str> {
        if let TfsDataVector::StringVec(ref vec) = self.columns[self.column_headers[column]] {
            Ok(vec)
        }
        else {
            Err("The column '{}' is not a String column")
        }
    }
}

impl Index<&str> for TfsDataFrame {
    type Output = TfsDataVector;

    /// Returns the column `col` as `TfsDataVector`, i.e. regardless of type.
    fn index(&self, col: &str) -> &Self::Output {
        &self.columns[self.column_headers[col]] 
    }
}

impl Display for TfsDataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "\x1B[1mTfsDataFrame, columns = {}\n\nHeader\x1B[0m:\n",
            self.column_headers.len())?;

        for (key, value) in self.properties.iter() {
            write!(f, "{:10} {:20}\n", key, value)?;
        }
        

        // only print 5 columns and 5 rows for now
        write!(f, "\n\x1B[1mData:\x1B[0m\n")?;
        for col in self.colnames.iter().take(5) {
            write!(f, "{:>15}", col)?;
        }
        write!(f, "     ... \n")?;
        for i in 0..5 {
            for col in 0..5 {
                write!(f, "{:>15.7}", self.columns[col].at(i))?;
            }
            write!(f, "     ...\n")?;
        }
        for i in 0..5 {
            write!(f, "{:>15}", "...")?;
        }
        write!(f, "     ...\n\n")
    }
}

impl Debug for TfsDataVector {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TfsDataVector::FloatVec(vec) => 
                write!(f, "FloatVec(len={})", vec.len()),
            TfsDataVector::StringVec(vec) =>
                write!(f, "StringVec(len={})", vec.len()),
        }
    }
}

impl Display for TfsProperty {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TfsProperty::Text(t) => write!(f, "%s {}", t),
            TfsProperty::Value(v) => write!(f, "%le {}", v),
        }
    }
}

impl Display for TfsValue<'_> {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TfsValue::Text(t) => Display::fmt(t, f),
            TfsValue::Float(v) => Display::fmt(v, f),
        }
    }
}

impl<'a> TfsDataVector {
    /// Returns the value at position `key` in type agnostic way.
    pub fn at(&'a self, key: usize) -> TfsValue<'a> {
        match self {
            TfsDataVector::FloatVec(vec) => TfsValue::Float(&vec[key]),
            TfsDataVector::StringVec(vec) => TfsValue::Text(&vec[key]),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
