mod parser;
mod rewriter;

use std::io::{self, Read};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let input = if args.len() > 1 && args[1] != "-" {
        std::fs::read_to_string(&args[1]).expect("Failed to read file")
    } else {
        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .expect("Failed to read stdin");
        buf
    };

    let output = rewriter::rewrite(&input);
    print!("{}", output);
}
