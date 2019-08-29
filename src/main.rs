mod lib;


use lib::{TfsDataFrame, DataFrame};

fn main() {
    println!("hello world");


    let mut df = TfsDataFrame::open("/media/awegsche/HDD/files/learning/73_rust_lobster/toymodel/twiss.dat");


    println!("{}", df);

    println!("df.loc({},{}) = {}", 5, "BETX", df.locd(5, "BETX").unwrap());
    println!("the column {} is {:?}", "BETX", df["BETX"]);
    
}