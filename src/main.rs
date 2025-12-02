use chrono::{DateTime, Datelike, Utc};
use regex::Regex;
use reqwest::blocking::get;
use serde::Deserialize;
use zip::ZipArchive;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct PriceInfo {
    #[serde(rename = "A")]
    pub avg: f64,
    #[serde(rename = "X")]
    pub max: f64,
    #[serde(rename = "N")]
    pub min: f64,
    #[serde(rename = "EC")]
    pub entry_count: u32,
    #[serde(rename = "AC")]
    pub amount_count: u32,
    #[serde(rename = "S")]
    pub suggested_price: Option<f64>,
    #[serde(rename = "SA")]
    pub sale_avg: Option<f64>,
    #[serde(rename = "SE")]
    pub sale_entry_count: Option<u32>,
    #[serde(rename = "SAC")]
    pub sale_amount_count: Option<u32>,
}

#[derive(Debug)]
pub struct ItemEntry {
    pub item_id: String,
    pub quality: String,
    pub level: String,
    pub trait_id: String,
    pub variant: String,
    pub price: PriceInfo,
}

fn download_zip(url: &str, output_path: &str) -> io::Result<()> {
    println!("Downloading from {}...", url);
    let response = get(url).expect("Failed to download file");
    let bytes = response.bytes().expect("Failed to read bytes");

    let mut file = File::create(output_path)?;
    file.write_all(&bytes)?;
    println!("Downloaded ZIP to {}", output_path);
    Ok(())
}

fn extract_lua_from_zip(zip_path: &str, lua_filename: &str, output_path: &str) -> io::Result<()> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file).expect("Failed to read ZIP archive");

    for i in 0..archive.len() {
        let mut file_in_zip = archive.by_index(i).unwrap();
        if file_in_zip.name().ends_with(lua_filename) {
            let mut out_file = File::create(output_path)?;
            io::copy(&mut file_in_zip, &mut out_file)?;
            println!("Extracted {} to {}", lua_filename, output_path);
            return Ok(());
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("{} not found in ZIP archive", lua_filename),
    ))
}

fn extract_price_table(text: &str) -> String {
    let no_comments = Regex::new(r"--.*").unwrap().replace_all(text, "").to_string();
    if let Some(pos) = no_comments.find("self.PriceTable") {
        if let Some(open_brace_pos) = no_comments[pos..].find('{') {
            let mut idx = pos + open_brace_pos;
            let mut depth: i32 = 0;
            let chars: Vec<char> = no_comments.chars().collect();
            let mut in_string: Option<char> = None;
            let mut escaped = false;
            let mut start = None;
            let mut end = None;
            while idx < chars.len() {
                let c = chars[idx];
                if in_string.is_some() {
                    if escaped {
                        escaped = false;
                    } else if c == '\\' {
                        escaped = true;
                    } else if Some(c) == in_string {
                        in_string = None;
                    }
                } else {
                    if c == '\'' || c == '"' {
                        in_string = Some(c);
                    } else if c == '{' {
                        if depth == 0 {
                            start = Some(idx + 1);
                        }
                        depth += 1;
                        } else if c == '}' {
                            depth -= 1;
                        if depth == 0 {
                            end = Some(idx);
                        break;
                        }
                    }
                }
                idx += 1;
            }

            if let (Some(s), Some(e)) = (start, end) {
                return chars[s..e].iter().collect::<String>().trim().to_string();
            }
        }
    }

    panic!("Could not locate self.PriceTable={{...}} block");
}

fn extract_item_lookup_table(text: &str) -> Option<String> {
    let re = Regex::new(r"self\.ItemLookUpTable\s*=\s*\{(?s)(?P<body>.*?)\}\s*end").unwrap();
    if let Some(caps) = re.captures(text) {
        return Some(caps["body"].to_string());
    }
    None
}

fn extract_timestamp_from_block(block: &str) -> Option<i64> {
    let re = Regex::new(r#"\[?\s*['\"]?TimeStamp['\"]?\s*\]?\s*=\s*(\d+)"#).unwrap();
        if let Some(caps) = re.captures(block) {
            if let Some(m) = caps.get(1) {
                if let Ok(ts) = m.as_str().parse::<i64>() {
                    return Some(ts);
            }
        }
    }
    None
}

fn lua_to_json(lua: &str) -> String {
    let mut s = lua.to_string();
    s = s.replace("\r\n", "\n");
    s = Regex::new(r"'([^'\\]*(?:\\.[^'\\]*)*)'")
        .unwrap()
        .replace_all(&s, r#""$1""#)
        .to_string();
    s = Regex::new(r#"\[\s*"([^"]*)"\s*\]\s*="#)
        .unwrap()
        .replace_all(&s, r#""$1":"#)
        .to_string();
    s = Regex::new(r#"\[\s*'([^']*)'\s*\]\s*="#)
        .unwrap()
        .replace_all(&s, r#""$1":"#)
        .to_string();
    s = Regex::new(r#"\[\s*(-?\d+(?:\.\d+)?)\s*\]\s*="#)
        .unwrap()
        .replace_all(&s, r#""$1":"#)
        .to_string();
    s = Regex::new(r#"(?P<prefix>(?:\{|,|\[|\s))(?P<key>[A-Za-z_]\w*)\s*="#)
        .unwrap()
        .replace_all(&s, "${prefix}\"${key}\":")
        .to_string();
    s = Regex::new(r"\bnil\b").unwrap().replace_all(&s, "null").to_string();
    s = Regex::new(r"\btrue\b").unwrap().replace_all(&s, "true").to_string();
    s = Regex::new(r"\bfalse\b").unwrap().replace_all(&s, "false").to_string();

    s
}

fn remove_trailing_commas_recursive(mut text: String) -> String {
    let re = Regex::new(r",\s*(?P<close>[\}\]])").unwrap();
    loop {
        let new_text = re.replace_all(&text, "$close").to_string();
        if new_text == text {
            break;
        }
        text = new_text;
    }

    text = Regex::new(r",\s*$").unwrap().replace_all(&text, "").to_string();

    text
}

fn traverse_value(value: &serde_json::Value, path: &mut Vec<String>, results: &mut Vec<ItemEntry>) {
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            path.push(k.clone());

            if let serde_json::Value::Object(inner) = v {
                if inner.contains_key("A") && inner.contains_key("X") {
                    let price: PriceInfo =
                        serde_json::from_value(serde_json::Value::Object(inner.clone()))
                            .expect("Failed to parse PriceInfo");
                    
                    let mut p = path.clone();
                    while p.len() < 5 { p.push("".to_string()); }
                    results.push(ItemEntry {
                        item_id: p[0].clone(),
                        quality: p[1].clone(),
                        level: p[2].clone(),
                        trait_id: p[3].clone(),
                        variant: p[4].clone(),
                        price,
                    });
                } else {
                    traverse_value(v, path, results);
                }
            }
            path.pop();
        }
    }
}



fn parse_ttc_lua(lua_text: &str) -> (Vec<ItemEntry>, Option<i64>) {
    let extracted = extract_price_table(&lua_text);
    let timestamp = extract_timestamp_from_block(&extracted);

    let jsonish = lua_to_json(&extracted);
    let cleaned = remove_trailing_commas_recursive(jsonish);
    let wrapped = format!("{{{}}}", cleaned);
    let parsed: serde_json::Value =
    serde_json::from_str(&wrapped).expect("Failed to parse JSON from TTC Lua file");
    let mut results = Vec::new();
    let mut path = Vec::new();
    traverse_value(&parsed["Data"], &mut path, &mut results);

    (results, timestamp)
}

fn parse_item_lookup(lua_text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Some(body) = extract_item_lookup_table(lua_text) {
        let re = Regex::new(r#"\[\s*\"([^\"]+)\"\s*\]\s*=\s*\{\s*\[\s*\d+\s*\]\s*=\s*(\d+)\s*,?\s*\}"#).unwrap();
        for cap in re.captures_iter(&body) {
            let name = cap[1].to_string();
            let id = cap[2].to_string();
            map.insert(id, name);
        }
    }
    map
}

fn write_entries_to_csv_manual(entries: &[ItemEntry], path: &str) -> std::io::Result<()> {
    if let Some(parent) = Path::new(path).parent() { fs::create_dir_all(parent)?; }
    let mut file = File::create(path)?;
    let header = ["item_id", "quality", "level", "trait", "variant", "avg", "max", "min", "entry_count", "amount_count", "suggested_price", "sale_avg", "sale_entry_count", "sale_amount_count"].join(",");
    writeln!(file, "{}", header)?;
    for e in entries {
        let parts = vec![ e.item_id.clone(), e.quality.clone(), e.level.clone(), e.trait_id.clone(), e.variant.clone(), e.price.avg.to_string(), e.price.max.to_string(), e.price.min.to_string(), e.price.entry_count.to_string(), e.price.amount_count.to_string(), e.price.suggested_price.map_or("".to_string(), |v| v.to_string()), e.price.sale_avg.map_or("".to_string(), |v| v.to_string()), e.price.sale_entry_count.map_or("".to_string(), |v| v.to_string()), e.price.sale_amount_count.map_or("".to_string(), |v| v.to_string()), ];
        let row = parts.iter().map(|s| s.as_str()).collect::<Vec<&str>>().join(",");
        writeln!(file, "{}", row)?;
    }
    Ok(())
}

fn write_lookup_table(lookup_map: &BTreeMap<String, String>, path: &str) -> std::io::Result<()> {
    if let Some(parent) = Path::new(path).parent() { fs::create_dir_all(parent)?; }
    let mut file = File::create(path)?;
    let header = ["item_id", "item_name"].join(",");
    writeln!(file, "{}", header)?;
    for (id, name) in lookup_map.iter() {
        let quoted_name = format!("\"{}\"", name);
        let parts = vec![ id, &quoted_name];
        let row = parts.iter().map(|s| s.as_str()).collect::<Vec<&str>>().join(",");
        writeln!(file, "{}", row)?;
    }
    Ok(())
}

fn process_server(region: &str, latest_csv: &str) -> io::Result<()> {
    let (url, zip_path, lua_filename, lookup_filename, csv_prefix) = match region {
        "NA" => (
            "https://us.tamrieltradecentre.com/download/PriceTable",
            "PriceTableNA.zip",
            "PriceTableNA.lua",
            "ItemLookUpTable_EN.lua",
            "na",
        ),
        "EU" => (
            "https://eu.tamrieltradecentre.com/download/PriceTable",
            "PriceTableEU.zip",
            "PriceTableEU.lua",
            "ItemLookUpTable_EN.lua",
            "eu",
        ),
        _ => panic!("Unknown region: {}", region),
    };

    let lua_output = lua_filename;
    let lookup_output = lookup_filename;
    download_zip(url, zip_path)?;
    extract_lua_from_zip(zip_path, lua_filename, lua_output)?;

    let mut lookup_map: BTreeMap<String, String> = BTreeMap::new();

    if let Ok(()) = extract_lua_from_zip(zip_path, lookup_filename, lookup_output) {
        let lookup_text = fs::read_to_string(lookup_output).expect("Could not read lookup Lua file");
        lookup_map = parse_item_lookup(&lookup_text);
        if Path::new(lookup_output).exists() { 
            fs::remove_file(lookup_output)?; 
        }
    } else {
        println!("Warning: {} not found in ZIP archive; item names will be empty", lookup_filename);
    }
    let lua_text = fs::read_to_string(lua_output).expect("Could not read Lua file");
    let (entries, timestamp_opt) = parse_ttc_lua(&lua_text);
    println!("Parsed {} price entries for {}.", entries.len(), region);

    let timestamp = timestamp_opt.unwrap_or_else(|| Utc::now().timestamp()); 
    let ndt = DateTime::from_timestamp(timestamp, 0).unwrap();
    let folder = format!("{:04}/{:02}/{:02}", ndt.year(), ndt.month(), ndt.day());
    fs::create_dir_all(&folder)?;

    let csv_path = format!("{}/{}.csv", folder, csv_prefix);
    write_entries_to_csv_manual(&entries, &csv_path)?;
    write_entries_to_csv_manual(&entries, latest_csv)?;
    let lookup_path = format!("{}/lookup.csv", folder);
    let latest_lookup_path = format!("latest/lookup.csv");
    write_lookup_table(&lookup_map, &lookup_path)?;
    write_lookup_table(&lookup_map, &latest_lookup_path)?;

    println!("CSV written to {} and latest CSV updated at {}", csv_path, latest_csv);

    if Path::new(zip_path).exists() { fs::remove_file(zip_path)?; }
    if Path::new(lua_output).exists() { fs::remove_file(lua_output)?; }

    Ok(())
}

fn main() -> io::Result<()> {
    process_server("NA", "latest/na.csv")?;
    process_server("EU", "latest/eu.csv")?;
    Ok(())
}