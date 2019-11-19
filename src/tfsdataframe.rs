use crate::dataframe::{DataFrame, DataValue, DataVector, DataView, Indexer};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct TfsDataFrame {
    columns: Vec<DataVector>,
    column_headers: HashMap<String, usize>,
    properties: HashMap<String, DataValue>,
    index_str: HashMap<String, usize>,
    colnames: Vec<String>,
    coltypes: Vec<String>,
}

impl TfsDataFrame {
    /// Opens a tfs file and stores the conten in a TfsDataFrame. Will panic! if opening fails rather
    /// than return a `Result<>`.
    pub fn open_expect<P>(path: P) -> TfsDataFrame
    where
        P: AsRef<Path>,
    {
        TfsDataFrame::open(path).expect("couldn't open the TFS file")
    }

    /// Opens a tfs file and stores the content in a TfsDataFrame.
    pub fn open<P>(path: P) -> Result<TfsDataFrame, std::io::Error>
    where
        P: AsRef<Path>,
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
    /// use tfs::{DataFrame, TfsDataFrame};
    ///
    /// let mut df = TfsDataFrame::open("test/test.tfs").expect("unable to open file");
    ///
    /// // works always, indexing using an integer, will acces the nth value according to the order
    /// // in the original file
    /// let betx1 = df.loc_real(0, "BETX").clone();
    ///
    /// // only after a first invocation of `set_index` access by the index will be possible
    /// df.set_index("NAME");
    /// let betx2 = df.loc_real("BPM1", "BETX").clone();
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
    pub fn propd(&self, key: &str) -> &f64 {
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
}

impl DataFrame for TfsDataFrame {
    fn col<'a>(&self, column: &str) -> &DataVector {
        &self.columns[self
            .column_headers
            .get(column)
            .expect(&format!("column {} not in dataframe", column))
            .clone()]
    }

    fn loc<'a, Key>(&self, key: Key, column: &str) -> DataView
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
}
