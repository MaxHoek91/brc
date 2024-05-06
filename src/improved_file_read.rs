use std::{fs, str};
use std::fmt::{Display, Formatter};
use std::io::Write;

use ahash::AHashMap as HashMap;
use anyhow::Result;

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


pub fn brc(file: &str) -> Result<()> {
    let data = fs::read(file)?;

    let mut weather_stations: HashMap<&[u8], Measurement> = HashMap::new();

    let mut line_start = 0;
    let mut name_end = 0;
    
    data
        .iter()
        .enumerate()
        .for_each(
            | (position, &character) | {
                match character {
                    SEMICOLON => {
                        name_end = position;
                    },
                    NEWLINE => {
                        let station = &data[line_start..name_end];
                        let value = parse_ascii_to_int(
                            &data[(name_end + 1)..position]
                        );
                        weather_stations
                            .entry(station)
                            .or_default()
                            .update(value);
                        line_start = position + 1;
                    },
                    _ => { }
                }
            }
        );

    let mut weather_stations: Vec<(&[u8], Measurement)> = weather_stations.into_iter().collect();
    weather_stations.sort_by_key(| item | item.0);
    write_output(weather_stations)?;
    Ok(())
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

fn write_output(weather_stations: Vec<(&[u8], Measurement)>) -> Result<()> {
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