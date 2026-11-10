use chrono::{DateTime, Datelike, Utc};
use csv::ReaderBuilder;
use num_format::{Locale, ToFormattedString};
use regex::Regex;
use reqwest::blocking::get;
use serde::Deserialize;
use zip::ZipArchive;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
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

fn write_entries_to_csv_zst(entries: &[ItemEntry], path: &str) -> io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut encoder = zstd::Encoder::new(BufWriter::new(file), 3)?;

    let header = [
        "item_id", "quality", "level", "trait", "variant",
        "avg", "max", "min", "entry_count", "amount_count",
        "suggested_price", "sale_avg", "sale_entry_count", "sale_amount_count",
    ]
    .join(",");
    writeln!(encoder, "{}", header)?;

    for e in entries {
        let parts = vec![
            e.item_id.clone(),
            e.quality.clone(),
            e.level.clone(),
            e.trait_id.clone(),
            e.variant.clone(),
            e.price.avg.to_string(),
            e.price.max.to_string(),
            e.price.min.to_string(),
            e.price.entry_count.to_string(),
            e.price.amount_count.to_string(),
            e.price.suggested_price.map_or("".to_string(), |v| v.to_string()),
            e.price.sale_avg.map_or("".to_string(), |v| v.to_string()),
            e.price.sale_entry_count.map_or("".to_string(), |v| v.to_string()),
            e.price.sale_amount_count.map_or("".to_string(), |v| v.to_string()),
        ];
        writeln!(encoder, "{}", parts.join(","))?;
    }

    encoder.finish()?;
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

fn write_lookup_table_zst(lookup_map: &BTreeMap<String, String>, path: &str) -> io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut encoder = zstd::Encoder::new(BufWriter::new(file), 3)?;

    writeln!(encoder, "item_id,item_name")?;
    for (id, name) in lookup_map.iter() {
        writeln!(encoder, "{},\"{}\"", id, name)?;
    }

    encoder.finish()?;
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

    let csv_path = format!("{}/{}.csv.zst", folder, csv_prefix);
    write_entries_to_csv_zst(&entries, &csv_path)?;
    write_entries_to_csv_manual(&entries, latest_csv)?;

    update_lookup_master(&lookup_map, &folder)?;

    if Path::new(zip_path).exists() { fs::remove_file(zip_path)?; }
    if Path::new(lua_output).exists() { fs::remove_file(lua_output)?; }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct CsvRow {
    item_id: String,
    quality: String,
    level: String,
    #[serde(rename = "trait")]
    trait_id: String,
    variant: String,

    avg: f64,
    max: f64,
    min: f64,
    entry_count: u32,
    amount_count: u32,

    suggested_price: Option<f64>,
    sale_avg: Option<f64>,
    sale_entry_count: Option<u32>,
    sale_amount_count: Option<u32>,
}

pub fn parse_items_from_csv_file(path: &str) -> Result<Vec<ItemEntry>, Box<dyn Error>> {
    let file = File::open(path)?;
    let mut reader = csv::Reader::from_reader(file);

    let mut entries = Vec::new();

    for result in reader.deserialize::<CsvRow>() {
        let row = result?;

        entries.push(ItemEntry {
            item_id: row.item_id,
            quality: row.quality,
            level: row.level,
            trait_id: row.trait_id,
            variant: row.variant,
            price: PriceInfo {
                avg: row.avg,
                max: row.max,
                min: row.min,
                entry_count: row.entry_count,
                amount_count: row.amount_count,
                suggested_price: row.suggested_price,
                sale_avg: row.sale_avg,
                sale_entry_count: row.sale_entry_count,
                sale_amount_count: row.sale_amount_count,
            },
        });
    }

    Ok(entries)
}

fn entry_gold_turnover(entry: &ItemEntry) -> Option<(f64, u64)> {
    let sold = entry.price.sale_amount_count.filter(|&n| n > 0)?;
    let avg = entry.price.sale_avg?;
    Some((avg * sold as f64, sold as u64))
}

pub fn total_gold_turnover(entries: &[ItemEntry]) -> u64 {
    entries
        .iter()
        .filter_map(entry_gold_turnover)
        .map(|(gold, _)| gold)
        .sum::<f64>()
        .round() as u64
}

#[derive(Debug, Deserialize)]
struct ItemNameRow {
    item_id: u32,
    item_name: String,
}

pub fn load_item_names(path: &str) -> Result<HashMap<u32, String>, Box<dyn Error>> {
    let file = File::open(path)?;
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let mut names = HashMap::new();

    for result in reader.deserialize::<ItemNameRow>() {
        let row = result?;
        names.insert(row.item_id, row.item_name);
    }

    Ok(names)
}

pub fn print_top_items_by_gold_turnover(entries: &[ItemEntry], item_names: &HashMap<u32, String>) {
    let mut totals: HashMap<u32, (f64, u64)> = HashMap::new();
    for entry in entries {
        let Some((gold, units)) = entry_gold_turnover(entry) else { continue };
        if let Ok(id) = entry.item_id.parse::<u32>() {
            let t = totals.entry(id).or_insert((0.0, 0));
            t.0 += gold;
            t.1 += units;
        }
    }

    let mut ranked: Vec<(u32, (f64, u64))> = totals.into_iter().collect();
    ranked.sort_by(|a, b| b.1 .0.total_cmp(&a.1 .0).then(a.0.cmp(&b.0)));

    println!("{:<6} {:<18} {:<12} {}", "Rank", "Gold Turnover", "Units Sold", "Item");
    for (rank, (item_id, (gold, units))) in ranked.into_iter().take(50).enumerate() {
        let name = item_names.get(&item_id).map(String::as_str).unwrap_or("UNKNOWN");
        println!(
            "{:<6} {:<18} {:<12} {}",
            rank + 1,
            (gold.round() as u64).to_formatted_string(&Locale::en),
            units.to_formatted_string(&Locale::en),
            name
        );
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    process_server("NA", "latest/na.csv")?;
    process_server("EU", "latest/eu.csv")?;
    let na = parse_items_from_csv_file("latest/na.csv")?;
    let eu = parse_items_from_csv_file("latest/eu.csv")?;
    let item_lookup = load_item_names(MASTER_LOOKUP_PATH)?;
    let na_gold = total_gold_turnover(&na);
    let eu_gold = total_gold_turnover(&eu);
    println!("\nPC NA Gold Turnover: {} (based on available TTC sales data)", na_gold.to_formatted_string(&Locale::en));
    print_top_items_by_gold_turnover(&na, &item_lookup);
    println!("\nPC EU Gold Turnover: {} (based on available TTC sales data)", eu_gold.to_formatted_string(&Locale::en));
    print_top_items_by_gold_turnover(&eu, &item_lookup);
    Ok(())
}

const MASTER_LOOKUP_PATH: &str = "latest/lookup.csv";
const NAME_HISTORY_PATH: &str = "latest/lookup_name_history.csv";
const MAX_RENAMES_LOGGED: usize = 20;

type NameHistory = BTreeMap<(String, String), (String, String)>;

fn load_lookup_map(path: &str) -> Result<BTreeMap<String, String>, Box<dyn Error>> {
    let mut map = BTreeMap::new();
    if !Path::new(path).exists() {
        return Ok(map);
    }
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(path)?;
    for rec in reader.records() {
        let rec = rec?;
        let (Some(id), Some(name)) = (rec.get(0), rec.get(1)) else {
            return Err(format!("{}: row with fewer than 2 fields", path).into());
        };
        map.insert(id.trim().to_string(), name.to_string());
    }
    Ok(map)
}

fn load_name_history(path: &str) -> Result<NameHistory, Box<dyn Error>> {
    let mut history = NameHistory::new();
    if !Path::new(path).exists() {
        return Ok(history);
    }
    let mut reader = ReaderBuilder::new().has_headers(true).from_path(path)?;
    for rec in reader.records() {
        let rec = rec?;
        if rec.len() != 4 {
            return Err(format!("{}: expected 4 fields, got {}", path, rec.len()).into());
        }
        history.insert(
            (rec[0].to_string(), rec[1].to_string()),
            (rec[2].to_string(), rec[3].to_string()),
        );
    }
    Ok(history)
}

fn write_name_history(history: &NameHistory, path: &str) -> Result<(), Box<dyn Error>> {
    let mut w = csv::Writer::from_path(path)?;
    w.write_record(["item_id", "item_name", "first_seen", "last_seen"])?;
    for ((id, name), (first, last)) in history {
        w.write_record([id.as_str(), name.as_str(), first.as_str(), last.as_str()])?;
    }
    w.flush()?;
    Ok(())
}

fn update_lookup_master(new_map: &BTreeMap<String, String>, folder: &str) -> io::Result<()> {
    let as_io = |e: Box<dyn Error>| io::Error::new(io::ErrorKind::Other, e.to_string());

    if new_map.is_empty() {
        println!("Warning: source lookup is empty; master lookup left untouched");
        return Ok(());
    }
    let date = folder.replace('/', "-");

    let mut master = load_lookup_map(MASTER_LOOKUP_PATH).map_err(as_io)?;
    let mut history = load_name_history(NAME_HISTORY_PATH).map_err(as_io)?;

    for (id, name) in new_map {
        let e = history
            .entry((id.clone(), name.clone()))
            .or_insert((date.clone(), date.clone()));
        if date < e.0 {
            e.0 = date.clone();
        }
        if date > e.1 {
            e.1 = date.clone();
        }
    }

    let mut added: Vec<String> = Vec::new();
    let mut renamed: Vec<(String, String, String)> = Vec::new();
    for (id, name) in new_map {
        match master.get(id) {
            None => added.push(id.clone()),
            Some(old) if old != name => renamed.push((id.clone(), old.clone(), name.clone())),
            _ => {}
        }
    }
    let retained = master.keys().filter(|k| !new_map.contains_key(*k)).count();

    for id in &added {
        master.insert(id.clone(), new_map[id].clone());
    }
    for (id, _, new_name) in &renamed {
        master.insert(id.clone(), new_name.clone());
    }

    if !added.is_empty() || !renamed.is_empty() {
        println!(
            "Lookup master: +{} new id(s), {} renamed (snapshot {})",
            added.len(),
            renamed.len(),
            date
        );
        for (id, old, new_name) in renamed.iter().take(MAX_RENAMES_LOGGED) {
            println!("  RENAMED {}: {:?} -> {:?}", id, old, new_name);
        }
        if renamed.len() > MAX_RENAMES_LOGGED {
            println!("  ... {} more renames (see {})", renamed.len() - MAX_RENAMES_LOGGED, NAME_HISTORY_PATH);
        }

        let mut by_name: BTreeMap<String, Vec<&String>> = BTreeMap::new();
        for (id, name) in &master {
            by_name.entry(name.trim().to_lowercase()).or_default().push(id);
        }
        let changed: Vec<&String> = added.iter().chain(renamed.iter().map(|r| &r.0)).collect();
        for id in changed {
            if let Some(ids) = by_name.get(&master[id].trim().to_lowercase()) {
                if ids.len() > 1 {
                    let list: Vec<&str> = ids.iter().map(|i| i.as_str()).collect();
                    println!(
                        "  WARNING: name {:?} is now shared by ids {} - possible id reuse",
                        master[id],
                        list.join(", ")
                    );
                }
            }
        }

        write_lookup_table(&master, MASTER_LOOKUP_PATH)?;
        write_lookup_table_zst(&master, &format!("{}/lookup.csv", folder))?;
    }
    if retained > 0 {
        println!("Lookup master: {} id(s) no longer in the source lookup (kept)", retained);
    }

    write_name_history(&history, NAME_HISTORY_PATH).map_err(as_io)?;
    Ok(())
}