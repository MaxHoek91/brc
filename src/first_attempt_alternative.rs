use std::fmt::{Display, Formatter};
use std::fs;
use std::io::Write;

use ahash::AHashMap as HashMap;
use anyhow::Result;

struct Measurement {
    minimum: f32,
    maximum: f32,
    count: u32,
    sum: f32
}

impl Measurement {
    fn update(&mut self, value: f32) {
        self.minimum = self.minimum.min(value);
        self.maximum = self.maximum.max(value);
        self.count += 1;
        self.sum += value;
    }
}

impl Default for Measurement {
    fn default() -> Self {
        Self { minimum: f32::MAX, maximum: f32::MIN, count: 0, sum: 0. }
    }
}

impl Display for Measurement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let avg = self.sum / (self.count as f32);
        write!(f, "{:.1}/{:.1}/{:.1}", self.minimum, avg, self.maximum)
    }
}


pub fn brc(file_path: &str) -> Result<()> {
    let data: String = fs::read_to_string(file_path)?;
    
    let mut weather_stations: HashMap<&str, Measurement> = HashMap::new();
    
    data.lines()
        .for_each(
            | line | {
            let (location, measurement) = line.split_once(';').unwrap();
            weather_stations
                .entry(location)
                .or_default()
                .update(measurement.parse::<f32>().unwrap()) 
            }
        );

    let mut weather_stations: Vec<(&str, Measurement)> = weather_stations.into_iter().collect();
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