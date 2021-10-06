use crate::dataframe::{DataValue, DataVector, DataView, Indexer};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use std::fmt;

/// The main struct of the crate. `TfsDataFrame` contains all the information of the loaded TFS file.
///
/// # TFS file structure
///
/// ## Properties
///
/// The first lines are a set of properties of the form
///
/// `
/// @ PROPERTY_NAME <type-id> value
/// `
///
/// where `<type-id>` is one of
///
/// | type id | type of data |
/// | --- |--- |
/// | `%le` | float |
/// |  `%s`| string |    
/// |  `%d`| int      |
///
///
/// ## Header
///
/// The column header contains of two lines, the first giving the names of the columns and the second
/// one the data types.
///
/// The column name row begins with an asterisk (`*`):
///
/// `* NAME POSITION col3 ...`
///
/// The data type row begins with a dollar symbol (`$`):
///
/// `$ %s %le ...`
///
/// ## Data
///
/// And finally, data rows just contain a whitespace separated list of the values for the respective columns.
///
/// ` ELEMENT1 0.0 ...`
pub struct TfsDataFrame<T: std::str::FromStr> {
    columns: Vec<DataVector<T>>,
    column_headers: HashMap<String, usize>,
    pub properties: HashMap<String, DataValue<T>>,
    index_str: HashMap<String, usize>,
    colnames: Vec<String>,
    coltypes: Vec<String>,
}

impl<T: std::str::FromStr> TfsDataFrame<T> {
    /// Opens a tfs file and stores the conten in a TfsDataFrame. Will panic! if opening fails rather
    /// than return a `Result<>`.~
    pub fn open_expect<P>(path: P) -> TfsDataFrame<T>
    where
        P: AsRef<Path>,
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        TfsDataFrame::open(path).expect("couldn't open the TFS file")
    }

    /// Opens a tfs file and stores the content in a TfsDataFrame.
    pub fn open<P>(path: P) -> Result<TfsDataFrame<T>, std::io::Error>
    where
        P: AsRef<Path>,
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        let mut reader = BufReader::new(File::open(path.as_ref())?).lines();

        let mut df = TfsDataFrame {
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
                        "%le" => df.properties.insert(
                            name,
                            DataValue::Real(line_it.next().unwrap().parse().unwrap()),
                        ),
                        _ => df
                            .properties
                            .insert(name, DataValue::Text(line_it.collect())),
                    };
                }
                _ => {}
            }
            if df.colnames.len() > 0 && df.coltypes.len() > 0 {
                break; // we have parsed the header, pass on to reading the data lines
            }
        }

        // setup columns
        for (ia, ib) in df.colnames.iter().zip(df.coltypes.iter()) {
            df.column_headers.insert(String::from(ia), df.columns.len());
            match ib.as_ref() {
                "%le" => df.columns.push(DataVector::RealVector(Vec::new())),
                _ => df.columns.push(DataVector::TextVector(Vec::new())),
            };
        }

        for line in reader {
            if let Ok(l) = line {
                let line_it = l.split_whitespace();
                for (idata, icolumn) in line_it.into_iter().zip(df.columns.iter_mut()) {
                    match icolumn {
                        DataVector::RealVector(ref mut vec) => vec.push((*idata).parse().unwrap()),
                        DataVector::TextVector(ref mut vec) => {
                            vec.push(String::from(idata).trim_matches('\"').to_owned())
                        }
                    }
                }
            }
        }

        Ok(df)
    }

    /// Sets the index to the specified column. The column has to be a String column. `f64` does
    /// not implement `Eq` so it cannot be used as hash key.
    ///
    /// The index has to be set for [`loc`](#method.locd) to work.
    ///
    /// ```
    /// use tfs::{TfsDataFrame};
    ///
    /// let mut df: TfsDataFrame<f32> = TfsDataFrame::open("test/test.tfs").expect("unable to open file");
    ///
    /// // only after a first invocation of `set_index` access by the index will be possible
    /// df.set_index("NAME");
    ///
    /// // works always, indexing using an integer, will acces the nth value according to the order
    /// // in the original file
    /// let betx1 = df.loc_real(0, "BETX").unwrap();
    /// let betx2 = df.loc_real("BPM1", "BETX").unwrap();
    ///
    /// assert_eq!(betx1, betx2);
    /// ```
    pub fn set_index(&mut self, column: &str) -> &mut Self {
        self.index_str.clear();
        if let DataVector::TextVector(vec) = &self.columns[self.column_headers[column]] {
            for i in 0..vec.len() {
                self.index_str.insert(vec[i].clone(), i);
            }
        } else {
            panic!("column not in the df");
        }
        self
    }

    pub fn len(&self) -> usize {
        match &self.columns[0] {
            DataVector::TextVector(v) => v.len(),
            DataVector::RealVector(v) => v.len(),
        }
    }

    /// Returns the property `key` from the header if it is a data value, otherwise it panics.
    pub fn propd(&self, key: &str) -> &T {
        if let DataValue::Real(ref v) = self.properties[key] {
            return v;
        }
        panic!(
            "the key '{}' is present in the header but it isn't a data value",
            key
        );
    }

    /// Returns the property `key` from the header if it is a string, otherwise it panics.
    pub fn props(&self, key: &str) -> &String {
        if let DataValue::Text(ref t) = self.properties[key] {
            return t;
        }
        panic!(
            "the key '{}' is present in the header but it isn't a string",
            key
        );
    }

    pub fn column_count(&self) -> usize {
        self.column_headers.len()
    }
}

impl<T> TfsDataFrame<T>
where T: std::str::FromStr {
    pub fn col(&self, column: &str) -> &DataVector<T> {
        &self.columns[self
            .column_headers
            .get(column)
            .expect(&format!("column {} not in dataframe", column))
            .clone()]
    }
    pub fn move_col(&mut self, column: &str) -> DataVector<T> {
        self.columns.remove(self
            .column_headers
            .get(column)
            .expect(&format!("column {} not in dataframe", column))
            .clone())
    }

    pub fn loc<'a, Key>(&self, key: Key, column: &'a str) -> DataView<T>
    where
        Key: Into<Indexer<'a>>,
    {
        use DataVector::*;
        let col = self.col(column);
        let idx = match key.into() {
            Indexer::Index(i) => i,
            Indexer::Key(k) => self.index_str[k],
        };

        match col {
            RealVector(v) => DataView::Real(&v[idx]),
            TextVector(v) => DataView::Text(&v[idx]),
        }
    }

    pub fn loc_real<'a, Key>(&self, key: Key, column: &'a str) -> Option<&T>
    where Key: Into<Indexer<'a>> {
        match self.loc(key, column) {
            DataView::Real(r) => Some(r),
            DataView::Text(t) => None,
        }
    }
    pub fn loc_text<'a, Key>(&self, key: Key, column: &'a str) -> Option<&str>
    where Key: Into<Indexer<'a>> {
        match self.loc(key, column) {
            DataView::Real(r) => None,
            DataView::Text(t) => Some(t),
        }
    }
}

impl<T: fmt::Debug + std::str::FromStr> fmt::Debug for TfsDataFrame<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("TfsDataFrame [{} rows]{{\n", self.len()))?;
        f.write_str("Header: \n")?;
        f.debug_map().entries(&self.properties).finish()?;
        f.write_str("\nColumns:\n")?;
        f.debug_map().entries(&self.column_headers).finish()?;
        f.write_str("\n}")
    }
}

impl<T: fmt::Display + std::str::FromStr> fmt::Display for TfsDataFrame<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("TfsDataFrame [{} rows] {{\n", self.len()))?;
        write!(f, "Header [{}]: \n", self.properties.len())?;
        for k in &self.properties {
            writeln!(f, "  {:32}: {:24}", k.0, k.1)?;
        }
        write!(f, "Columns [{}]:\n", self.columns.len())?;
        for c in &self.column_headers {
            writeln!(f, "  {1:5} {0:12}", c.0, self.coltypes[*c.1])?;
        }
        f.write_str("}\n")
    }
}