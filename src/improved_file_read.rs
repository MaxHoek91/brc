use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Write;
use std::str;

use ahash::AHashMap as HashMap;
use anyhow::Result;
use bstr::{BStr, ByteSlice};
use memmap2::MmapOptions;

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
    let file = File::open(file).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

    let mut weather_stations: HashMap<&BStr, Measurement> = HashMap::new();

    mmap.lines()
        .for_each(
            | line | {
                let (location, measurement) = line.split_once_str(&[b';']).unwrap();
                
                let len = measurement.len();
                let mut slice: Vec<u8> = Vec::from(&measurement[0..len - 2]);
                slice.push(measurement[len - 1]);
                
                weather_stations
                    .entry(location.into())
                    .or_default()
                    .update(str::from_utf8(&slice).unwrap().parse::<i32>().unwrap())
            }
        );

    let mut weather_stations: Vec<(&BStr, Measurement)> = weather_stations.into_iter().collect();
    weather_stations.sort_by_key(| item | item.0);

    let mut weather_iter = weather_stations.into_iter();
    let (first_station, first_weather) = weather_iter.next().unwrap();

    let stdout = std::io::stdout();
    let mut lock = stdout.lock();

    write!(lock, "{{")?;
    write!(lock, "{first_station}={first_weather}")?;
    for (station, weather) in  weather_iter {
        write!(lock, ", {station}={weather}")?;
    }
    writeln!(lock, "}}")?;
    Ok(())
}