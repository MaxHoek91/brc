use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::io::Write;
use std::str;

use ahash::RandomState;
use anyhow::Result as Result;
use hashbrown::HashMap;
use rayon::prelude::*;

type MeasurementsMap = HashMap<Box<[u8]>, Measurement, RandomState>;
type SortedMeasurements = BTreeMap<Box<[u8]>, Measurement>;

const CHUNK_SIZE: usize = 1024 * 1024;
const NEWLINE: u8 = 10;
const SEMICOLON: u8 = 59;
const MINUS: u8 = 45;
const PERIOD: u8 = 46;

#[derive(Debug)]
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

    fn merge(&mut self, other: &Self) {
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
    let threads: usize = std::thread::available_parallelism().unwrap().into();
    let boundaries = find_chunk_boundaries(file_path, threads)?;
    
    let parts: Vec<_> = boundaries
        .par_iter()
        .map(| (start, end) | read_chunk(file_path, *start, *end))
        .collect();

    let measurements: MeasurementsMap = parts
        .into_iter()
        .fold(
            HashMap::default(),
            |mut a, b| { merge(&mut a, &b); a }
        );
    
    let measurements: SortedMeasurements = BTreeMap::from_iter(measurements);
    write_output(measurements)?;
    Ok(())
}

fn merge<'a>(
    map_one: &mut MeasurementsMap,
    map_two: &MeasurementsMap
) {
    map_two
        .iter()
        .for_each(
            | (key, value) | {
                map_one.entry(key.clone()).or_insert(Measurement::default()).merge(value)
            }
        );
}

fn find_chunk_boundaries(file_path: &str, threads: usize) -> Result<Vec<(usize, usize)>> {
    let file: File = File::open(file_path)?;
    let file_size: usize = file.metadata().unwrap().len() as usize;
    let mut reader = BufReader::with_capacity(64, file);

    let chunk_size: usize = file_size / threads;

    let mut starts: Vec<usize> = Vec::with_capacity(threads);
    starts.push(0);
    for i in 1..threads {
        reader.seek_relative(chunk_size as i64)?;
        let mut buffer: Vec<u8> = Vec::with_capacity(64);
        let start = reader.read_until(NEWLINE, &mut buffer)?;
        starts.push(starts[i - 1] + chunk_size + start);
    }

    let mut ends: Vec<usize> = vec![0; threads];
    ends[..(threads - 1)].copy_from_slice(&starts[1..threads]);
    ends[threads - 1] = file_size;

    Ok(Vec::from_iter(starts.into_iter().zip(ends)))
}

fn read_chunk(file_path: &str, start: usize, end: usize) -> MeasurementsMap {
    let file = File::open(file_path).unwrap();
    let mut buffer: [u8; CHUNK_SIZE] = [0; CHUNK_SIZE];

    let mut reader = BufReader::with_capacity(CHUNK_SIZE, file);
    reader.seek(SeekFrom::Start(start as u64)).unwrap();

    let mut measurements: MeasurementsMap = HashMap::default();
    let mut read_total = 0;
    let mut read_size = CHUNK_SIZE;

    while (read_size == CHUNK_SIZE) && (read_total < end) {
        read_size = reader.read(&mut buffer).unwrap();
        let last_line_start = scan_ascii_chunk(&buffer[..read_size], &mut measurements);
        reader.seek_relative(last_line_start as i64 - read_size as i64).unwrap();
        read_total += last_line_start;
    }

    measurements
}

#[inline]
fn scan_ascii_chunk(buffer: &[u8], measurements: &mut MeasurementsMap) -> usize {
    let mut line_start = 0;
    let mut semi_pos = 0;

    for (pos, char) in buffer.iter().enumerate() {
        match *char {
            SEMICOLON => semi_pos = pos,
            NEWLINE => {
                let name = &buffer[(line_start)..semi_pos];
                let value = parse_int(&buffer[(semi_pos + 1)..pos]);

                measurements
                    .entry_ref(name)
                    .and_modify(| measurement | measurement.update(value))
                    .or_insert(Measurement::new(value));

                line_start = pos + 1;
            },
            _ => continue
        }
    }
    line_start
}

#[inline]
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

#[inline]
fn write_output(weather_stations: SortedMeasurements) -> Result<()>
{
    let mut weather_iter = weather_stations.into_iter();
    let (first_station, first_weather) = weather_iter.next().unwrap();
    let first_station = str::from_utf8(&first_station)?;

    let stdout = std::io::stdout();
    let mut lock = stdout.lock();

    write!(lock, "{{")?;
    write!(lock, "{first_station}={first_weather}")?;
    for (station, weather) in  weather_iter {
        let station = str::from_utf8(&station)?;
        write!(lock, ", {station}={weather}")?;
    }
    writeln!(lock, "}}")?;
    Ok(())
}
