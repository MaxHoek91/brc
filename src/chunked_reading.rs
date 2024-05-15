use std::io::{BufReader, Read, Seek};
use anyhow::Result as Result;
use std::fs::File;
use ahash::RandomState;
use hashbrown::HashMap;

type MeasurementsMap = HashMap<Box<[u8]>, Measurement, RandomState>;

const CHUNK_SIZE: usize = 1000_usize.pow(2);
const NEWLINE: u8 = 10;
const SEMICOLON: u8 = 59;
const MINUS: u8 = 45;
const PERIOD: u8 = 46;

struct Measurement {
    minimum: i32,
    maximum: i32,
    count: i32,
    sum: i32
}

impl Measurement {
    fn new(value: i32) -> Self {
        Self { minimum: value, maximum: value, count: 1, sum: value }
    }

    fn update(&mut self, value: i32) {
        self.minimum = self.minimum.min(value);
        self.maximum = self.maximum.max(value);
        self.count += 1;
        self.sum += value;
    }
}

pub fn brc(file_path: &str) -> Result<()> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::with_capacity(CHUNK_SIZE, file);
    let mut buffer: [u8; CHUNK_SIZE] = [0; CHUNK_SIZE];

    let mut measurements: MeasurementsMap = HashMap::default();

    let mut buffer_size = CHUNK_SIZE;
    while buffer_size == CHUNK_SIZE {
        buffer_size = reader.read(&mut buffer)?;
        let last_newline = scan_ascii_chunk(&buffer, buffer_size, &mut measurements);
        reader.seek_relative((last_newline - buffer_size + 1) as i64)?;
    }

    let mut rest = Vec::new();
    let rest_size = reader.read_to_end(&mut rest)?;
    println!("Rest size: {rest_size}");
    println!("{:?}", String::from_utf8(rest));

    Ok(())
}

fn scan_ascii_chunk(buffer: &[u8], size: usize, measurements: &mut MeasurementsMap) -> usize {

    let mut newline_pos = 0;
    let mut semi_pos = 0;
    
    let mut pos = 0;
    while pos < size {
        match buffer[pos] {
            SEMICOLON => semi_pos = pos,
            NEWLINE => {
                let name = &buffer[(newline_pos + 1)..semi_pos];
                let value = parse_int(&buffer[(semi_pos + 1)..=pos]);

                measurements
                    .entry_ref(name)
                    .and_modify(| measurement | measurement.update(value))
                    .or_insert_with(|| Measurement::new(value));

                newline_pos = pos;
            },
            _ => { }
        }
        pos += 1;
    }
    
    newline_pos
}

fn parse_int(buffer: &[u8]) -> i32 {
    let mut acc: i32 = 0;
    let mut is_neg = false;

    for &val in buffer {
        match val {
            PERIOD => continue,
            MINUS => is_neg = true,
            _ => acc = acc * 10 + val as i32 - 48
        }
    }

    match is_neg {
        true => -acc,
        false => acc,
    }
}
