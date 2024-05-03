use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Write;

use ahash::AHashMap as HashMap;
use anyhow::Result as Result;
use bstr::ByteSlice;
use memmap2::MmapOptions;

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
    fn update(&mut self, value: i32) {
        self.minimum = self.minimum.min(value);
        self.maximum = self.maximum.max(value);
        self.count += 1;
        self.sum += value;
    }

    fn merge(&mut self, other: &Self){
        self.minimum = self.minimum.min(other.minimum);
        self.maximum = self.maximum.max(other.maximum);
        self.count += other.count;
        self.sum += other.sum;
    }
}

impl Default for Measurement {
    fn default() -> Self {
        Self { minimum: i32::MAX, maximum: i32::MIN, count: 0, sum: 0 }
    }
}

impl Display for Measurement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let min = self.minimum as f32 * 0.1;
        let max = self.maximum as f32 * 0.1;
        let avg = self.sum as f32 / self.count as f32 * 0.1;

        write!(f, "{:.1}/{:.1}/{:.1}", min, avg, max)
    }
}

pub fn brc(file_path: &str) -> Result<()> {
    let cores: usize = std::thread::available_parallelism().unwrap().into();

    let file: File = File::open(file_path)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let size: usize = mmap.len();

    let chunk_size: usize = size / cores;
    let mut starts: Vec<usize> = (0..cores)
        .map(| core | core * chunk_size)
        .collect();

    for i in 1..cores {
        starts[i] = find_next_newline(starts[i], &mmap);
    }

    let mut ends: Vec<usize> = vec![0; cores];
    ends[..(cores - 1)].copy_from_slice(&starts[1..cores]);
    ends[cores - 1] = size;
    
    let mut parts = Vec::with_capacity(cores);
    std::thread::scope(
        | scope | {
            let mut handles = Vec::with_capacity(cores);
            for thread in 0..cores {
                let start = starts[thread];
                let end = ends[thread];
                let buffer = &mmap;
                let handle = scope.spawn(
                    move || { scan_ascii_chunk(start, end, buffer) }
                );
                handles.push(handle)
            }
            for handle in handles {
                let chunk = handle.join().unwrap();
                parts.push(chunk);
            }
        }
    );

    let weather_stations: HashMap<&[u8], Measurement> = parts
        .into_iter()
        .fold(
            HashMap::default(),
            |mut a, b| { merge(&mut a, &b); a }
        );

    let mut weather_stations: Vec<(&[u8], Measurement)> = weather_stations.into_iter().collect();
    weather_stations.sort_by_key(| item | item.0);

    let mut weather_iter = weather_stations.into_iter();
    let (first_station, first_weather) = weather_iter.next().unwrap();
    let first_station = std::str::from_utf8(first_station)?;

    let stdout = std::io::stdout();
    let mut lock = stdout.lock();

    write!(lock, "{{")?;
    write!(lock, "{first_station}={first_weather}")?;
    for (station, weather) in  weather_iter {
        let station = std::str::from_utf8(station)?;
        write!(lock, ", {station}={weather}")?;
    }
    writeln!(lock, "}}")?;
    Ok(())
}

fn find_next_newline(start: usize, buffer: &[u8]) -> usize {
    match buffer[start..].find_byte(NEWLINE) {
        Some(position) => start + position + 1,
        None => unreachable!()
    }
}

fn scan_ascii_chunk(start: usize, end: usize, buffer: &[u8]) -> HashMap<&[u8], Measurement> {
    let mut measurements: HashMap<&[u8], Measurement> = HashMap::default();

    let mut line_start = start;
    let mut name_end = start;

    for (position, &character) in buffer[start..end].iter().enumerate() {
        match character {
            SEMICOLON => {
                name_end = start + position;
            },
            NEWLINE => {
                let station = &buffer[line_start..name_end];
                let value = parse_ascii_to_int(
                    &buffer[(name_end + 1)..(position + start)]
                );
                measurements.entry(station).or_default().update(value);
                line_start = start + position + 1;
            },
            _ => continue
        };
    }

    measurements
}

fn parse_ascii_to_int(buffer: &[u8]) -> i32 {
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

fn merge<'a>(
    map_one: &mut HashMap<&'a [u8], Measurement>,
    map_two: &HashMap<&'a [u8], Measurement>
) {
    map_two
        .iter()
        .for_each(
            | (key, value) | map_one.entry(key).or_default().merge(value)
        );
}