use std::collections::HashMap;
use std::fs;

#[derive(Debug)]
struct Measurement {
    minimum: f32,
    maximum: f32,
    count: u32,
    sum: f32
}

impl Measurement {
    fn new() -> Self {
        Self { minimum: f32::MAX, maximum: f32::MIN, count: 0, sum: 0. }
    }
    
    fn update(&mut self, value: f32) {
        self.minimum = self.minimum.min(value);
        self.maximum = self.maximum.max(value);
        self.count += 1;
        self.sum += value;
    }
}


pub fn brc(file: &str) {
    let data: String = fs::read_to_string(file).expect("Could not read the file");
    
    let mut weather_stations: HashMap<&str, Measurement> = HashMap::new();
    
    for line in data.lines() {
        let (location, measurement) = match line.split_once(';') {
            Some((loc, val)) => (loc, val),
            None => continue
        };
        
        let measurement_value: f32 = measurement.parse().unwrap();
        weather_stations
            .entry(location)
            .or_insert(Measurement::new())
            .update(measurement_value);
    }
    
    let mut weather_stations: Vec<(&str, Measurement)> = weather_stations.into_iter().collect();
    weather_stations.sort_by_key(| item | item.0);

    
    let mut weather_iter = weather_stations.into_iter();
    
    let (first_station, first_weather) = weather_iter.next().unwrap();
    let avg = first_weather.sum / (first_weather.count as f32);
    
    print!("{{");
    print!("{first_station}={:.1}/{:.1}/{:.1}", first_weather.minimum, avg, first_weather.maximum);
    
    for (station, weather) in  weather_iter {
        let avg = weather.sum / (weather.count as f32);
        print!(", {station}={:.1}/{:.1}/{:.1}", weather.minimum, avg, weather.maximum);
    }
    println!("}}");
}