use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::io::Write;
use std::str;

use ahash::RandomState;
use hashbrown::HashMap;
use anyhow::Result as Result;

type MeasurementsMap = HashMap<Box<[u8]>, Measurement, RandomState>;

const BUF_SIZE: usize = 1024 * 1024;
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

    let (starts, ends) = find_chunk_boundaries(file_path, cores)?;

    let mut chunks = Vec::with_capacity(cores);
    std::thread::scope(
        | scope | {
            let mut handles = Vec::with_capacity(cores);
            for i in 0..cores {
                let start = starts[i];
                let read_size = ends[i] - start;

                let handle = scope.spawn(
                    move || {
                        let file = File::open(file_path).unwrap();
                        let mut reader = BufReader::with_capacity(BUF_SIZE, file);
                        let mut buffer: [u8; BUF_SIZE] = [0; BUF_SIZE];

                        let mut read_length: usize = 1;
                        let mut read_total: usize = 0;

                        let mut measurements: MeasurementsMap = HashMap::default();

                        reader.seek(SeekFrom::Start(start as u64)).unwrap();
                        while (read_total < read_size) && (read_length != 0) {
                            reader.seek_relative(read_length as i64).unwrap();
                            let buffer_size = reader.read(&mut buffer).unwrap();
                            read_length = scan_ascii_chunk(&buffer, buffer_size, &mut measurements);
                            read_total += read_length;
                        }
                        measurements
                    }
                );
                handles.push(handle)
            }
            for handle in handles {
                let chunk  = handle.join().unwrap();
                chunks.push(chunk);
            }
        }
    );

    let mut weather_stations = BTreeMap::new();
    let mut chunk_iter =  chunks.into_iter();
    weather_stations.extend(chunk_iter.next().unwrap());
    for chunk in chunk_iter {
        for (city, measurement) in chunk.into_iter() {
            weather_stations
                .entry(city)
                .and_modify(| city | city.merge(&measurement))
                .or_insert(measurement);
        }
    }

    write_output(weather_stations)?;
    Ok(())
}

fn find_chunk_boundaries(file_path: &str, threads: usize) -> Result<(Vec<usize>, Vec<usize>)> {
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

    Ok((starts, ends))
}

fn scan_ascii_chunk(buffer: &[u8], end: usize, measurements: &mut MeasurementsMap) -> usize {

    let mut line_start = 0;
    let mut name_end = 0;
    
    let mut position = 0;
    while position < end {
        match buffer[position] {
            SEMICOLON => name_end = position,
            NEWLINE => {
                let station_name = &buffer[line_start..name_end];
                let value = parse_ascii_to_int(&buffer[(name_end + 1)..position]);

                line_start = position + 1;
                
                measurements
                    .entry_ref(station_name)
                    .and_modify(| measurement | measurement.update(value))
                    .or_insert_with(|| Measurement::new(value));

            },
            _ => { }
        };
        position += 1;
    }
    line_start - 1
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

fn write_output(weather_stations: BTreeMap<Box<[u8]>, Measurement>) -> Result<()> {
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


// pub fn brc(file_path: &str) -> Result<()> {
//     let cores: usize = std::thread::available_parallelism().unwrap().into();
//
//     let file = File::open(file_path)?;
//     let fsize: usize = file
//         .metadata()
//         .unwrap()
//         .len()
//         .try_into()?;
//     let splitter = fsize / cores;
//
//     let mut chunks = Vec::with_capacity(cores);
//
//     std::thread::scope(
//         | scope | {
//             let mut handles = Vec::with_capacity(cores);
//             for i in 0..cores {
//                 let handle = scope.spawn(
//                     move || {
//                         let file = File::open(file_path).unwrap();
//                         let mut reader = BufReader::with_capacity(BUF_SIZE, file);
//
//                         let mut output = 0;
//
//                         let mut read_length: usize = 1;
//                         let mut read_total: usize = 0;
//
//                         let offset = (i * splitter) as i64;
//                         reader.seek_relative(offset).unwrap();
//
//                         while (read_total < splitter) && (read_length != 0) {
//                             let buffer = reader.fill_buf().unwrap();
//                             read_length = buffer.len();
//
//                             output = scan_ascii_chunk(buffer);
//
//                             reader.consume(read_length);
//                             read_total += read_length;
//                         }
//                         output
//                     }
//                 );
//                 handles.push(handle)
//             }
//             for handle in handles {
//                 let chunk  = handle.join().unwrap();
//                 chunks.push(chunk);
//             }
//         }
//     );
//
//     println!("{chunks:?}");
//
//     Ok(())
// }
//
// fn scan_ascii_chunk(buffer: &[u8]) -> u8 {
//     0
// }

// let mut chunks = Vec::with_capacity(cores);
// std::thread::scope(
//     | scope | {
//         let mut handles = Vec::with_capacity(cores);
//         for thread in 0..cores {
//             let start = starts[thread];
//             let end = ends[thread];
//             let total = end - start;
//
//             let handle = scope.spawn(
//                 move || {
//                     let mut measurements: HashMap<&[u8], Measurement> = HashMap::default();
//
//                     let file = File::open(file_path).unwrap();
//                     let mut reader = BufReader::with_capacity(BUF_SIZE, file);
//                     reader.seek(SeekFrom::Start(start as u64)).unwrap();
//
//                     let mut read_length = 0;
//                     let mut read_total = 0;
//
//                     while (read_total < total) && (read_length != 0) {
//                         let buffer = reader.fill_buf().unwrap();
//                         read_length = buffer.len();
//
//                         let mut extra_buffer = Vec::new();
//
//                         reader.seek_relative(size as i64).unwrap();
//                         let extra_size = reader.read_until(NEWLINE, &mut extra_buffer).unwrap();
//                         read_length += extra_size;
//
//                         scan_ascii_chunk(start, end, buffer, &extra_buffer, &mut measurements);
//                         reader.consume(read_length);
//                         read_total += read_length;
//                     }
//                 }
//             );
//             handles.push(handle)
//         }
//         for handle in handles {
//             let chunk = handle.join().unwrap();
//             chunks.push(chunk);
//         }
//     }
// );
