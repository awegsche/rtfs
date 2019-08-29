/// The contents of this file a copied verbatim from
/// https://stackoverflow.com/questions/44510445/borrowed-value-not-living-long-enough-bufreader-lines-to-iterator-of-string
/// thanks to breeden for his answer to the stack overflow thread
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::iter::IntoIterator;
use std::str::SplitWhitespace;

/// Owned String that will be split by whitespaces
pub struct SplitWhitespaceOwned(String);

impl<'a> IntoIterator for &'a SplitWhitespaceOwned {
    type Item = &'a str;
    type IntoIter = SplitWhitespace<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.split_whitespace()
    }
}

// Returns an iterator of an iterator. The use case is a very large file where
// each line is very long. The outer iterator goes over the file's lines.
// The inner iterator returns the words of each line.
pub fn tokens_from_path<P>(path_arg: P) -> Box<Iterator<Item = SplitWhitespaceOwned>>
    where P: AsRef<Path>
{
    let reader = reader_from_path(path_arg);
    let iter = reader
        .lines()
        .filter_map(|result| result.ok())
        .map(|s| SplitWhitespaceOwned(s));
    Box::new(iter)
}

fn reader_from_path<P>(path_arg: P) -> BufReader<File>
    where P: AsRef<Path>
{
    let path = path_arg.as_ref();
    let file = File::open(path).unwrap();
    BufReader::new(file)
}