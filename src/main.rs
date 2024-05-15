
use std::time::Instant;

mod first_attempt;
mod first_attempt_alternative;
mod first_attemp_vec;
mod improved_file_read;
mod multithreaded_rayon;
mod multithreaded_manual;
mod prototyping;
mod chunked_reading;


fn main() {
    let input_file = "C:/Users/Max/Downloads/1brc-main/data/measurements_small.txt";
    // let input_file = "C:/Users/Max/Downloads/1brc-main/data/measurements.txt";
    
    let timer = Instant::now();
    // first_attempt::brc(input_file);
    // first_attempt_alternative::brc(input_file);
    // first_attemp_vec::brc(input_file).unwrap();
    // improved_file_read::brc(input_file);
    // multithreaded_manual::brc(input_file).unwrap();
    // multithreaded_rayon::brc(input_file).unwrap();
    chunked_reading::brc(input_file).unwrap();
    // prototyping::brc(input_file).unwrap();
    println!("\n{:?}", timer.elapsed());
}
