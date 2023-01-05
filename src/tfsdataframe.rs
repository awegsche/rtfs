use polars::prelude::{DataFrame, NamedFrom, NumericNative, PolarsError};
use polars::series::Series;

use crate::dataframe::{DataValue, DataVector, DataView, Indexer};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use std::fmt;

/// `TfsDataFrame` is a wrapper around `polars::DataFrame` that supports the `TFS` format.
/// A TFS file consists of a list of properties (key - value pairs) followed by a chunk of data
/// in tabular format.
///
/// The following example loads a temporary tfs file into memory and prints its data:
///
pub struct TfsDataFrame<T: std::str::FromStr + polars::prelude::NumericNative> {
    pub properties: HashMap<String, DataValue<T>>,
    df: DataFrame,
}

impl<T: std::str::FromStr + NumericNative> TfsDataFrame<T> {
    /// Opens a tfs file and stores the content in a TfsDataFrame. Will panic! if opening fails rather
    /// than return a `Result<>`.~
    pub fn open_expect<P>(path: P) -> TfsDataFrame<T>
    where
        P: AsRef<Path>,
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        TfsDataFrame::open(path).expect("couldn't open the TFS file")
    }

    /// Opens a tfs file and stores the content in a TfsDataFrame.
    pub fn open<P>(path: P) -> Result<TfsDataFrame<T>, PolarsError>
    where
        P: AsRef<Path>,
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        let mut reader = BufReader::new(File::open(path.as_ref())?).lines();

        let mut properties = HashMap::new();
        let mut colnames = vec![];
        let mut coltypes = vec![];

        loop {
            let line = reader.next().unwrap().unwrap();
            let mut line_it = line.split_whitespace();

            match line_it.next().unwrap() {
                "*" => colnames.extend(line_it.map(|x| String::from(x))),
                "$" => coltypes.extend(line_it.map(|x| String::from(x))),
                "@" => {
                    let name = String::from(line_it.next().unwrap());
                    match line_it.next().unwrap() {
                        "%le" => properties.insert(
                            name,
                            DataValue::Real(
                                line_it
                                    .next()
                                    .unwrap()
                                    .parse()
                                    .expect("should be a valid property"),
                            ),
                        ),
                        _ => properties.insert(name, DataValue::Text(line_it.collect())),
                    };
                }
                _ => {}
            }
            if colnames.len() > 0 && coltypes.len() > 0 {
                break; // we have parsed the header, pass on to reading the data lines
            }
        }

        let mut columns: Vec<DataVector<f64>> = vec![];

        // setup columns
        for (ia, ib) in colnames.iter().zip(coltypes.iter()) {
            match ib.as_ref() {
                "%le" => columns.push(DataVector::RealVector(Vec::new())),
                _ => columns.push(DataVector::TextVector(Vec::new())),
            };
        }

        for line in reader {
            if let Ok(l) = line {
                let line_it = l.split_whitespace();
                for (idata, icolumn) in line_it.into_iter().zip(columns.iter_mut()) {
                    match icolumn {
                        DataVector::RealVector(ref mut vec) => {
                            vec.push((*idata).parse().unwrap_or(f64::NAN))
                        }
                        DataVector::TextVector(ref mut vec) => {
                            vec.push(String::from(idata).trim_matches('\"').to_owned())
                        }
                    }
                }
            }
        }

        let mut serieses: Vec<Series> = vec![];

        for (name, column) in colnames.iter().zip(columns) {
            match column {
                DataVector::TextVector(v) => serieses.push(Series::new(name, &v)),
                DataVector::RealVector(v) => serieses.push(Series::new(name, v)),
            };
        }

        Ok(TfsDataFrame {
            properties,
            df: DataFrame::new(serieses)?,
        })
    }

    pub fn len(&self) -> usize {
        self.df.height()
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
        self.df.width()
    }

    pub fn column(&self, name: &str) -> anyhow::Result<&Series> {
        Ok(self.df.column(name)?)
    }

    pub fn df(&self) -> &DataFrame {
        &self.df
    }
}

impl<T: fmt::Debug + std::str::FromStr + NumericNative> fmt::Debug for TfsDataFrame<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("TfsDataFrame [{} rows]{{\n", self.len()))?;
        f.write_str("Header: \n")?;
        f.debug_map().entries(&self.properties).finish()?;
        write!(f, "{:?}", self.df)?;
        f.write_str("\n}")
    }
}

impl<T: fmt::Display + std::str::FromStr + NumericNative> fmt::Display for TfsDataFrame<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("TfsDataFrame [{} rows] {{\n", self.len()))?;
        write!(f, "Header [{}]: \n", self.properties.len())?;
        for k in &self.properties {
            writeln!(f, "  {:32}: {:24}", k.0, k.1)?;
        }
        write!(f, "{}", self.df)
    }
}
