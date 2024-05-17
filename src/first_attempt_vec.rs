use std::fmt::{Display, Formatter};
use std::fs;
use std::io::Write;

use anyhow::Result;

struct Measurement<'a> {
    name: &'a str,
    minimum: f32,
    maximum: f32,
    count: u32,
    sum: f32
}

impl<'a> Measurement<'a> {
    
    fn new(name: &'a str, value: f32) -> Self {
        Self { name, minimum: value, maximum: value, count: 1, sum: value }
    }
    
    fn update(&mut self, value: f32) {
        self.minimum = self.minimum.min(value);
        self.maximum = self.maximum.max(value);
        self.count += 1;
        self.sum += value;
    }
}

impl<'a> Display for Measurement<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let avg = self.sum / (self.count as f32);
        write!(f, "{}={:.1}/{:.1}/{:.1}", self.name, self.minimum, avg, self.maximum)
    }
}


pub fn brc(file_path: &str) -> Result<()> {
    let data: String = fs::read_to_string(file_path)?;

    let mut weather_stations: Vec<Measurement> = Vec::new();

    data.lines()
        .for_each(
            | line | {
                let (location, value) = line.split_once(';').unwrap();
                let value: f32 = value.parse().unwrap();
                
                match weather_stations
                    .iter_mut()
                    .find(| station | station.name == location) 
                {
                    Some(station) => station.update(value),
                    None => weather_stations.push(Measurement::new(location, value)),
                };
            }
        );

    weather_stations.sort_by_key(| station | station.name);

    let mut weather_iter = weather_stations.into_iter();
    let first_station = weather_iter.next().unwrap();

    let stdout = std::io::stdout();
    let mut lock = stdout.lock();

    write!(lock, "{{")?;
    write!(lock, "{first_station}")?;
    for station in  weather_iter {
        write!(lock, ", {station}")?;
    }
    writeln!(lock, "}}")?;
    Ok(())
}