// Load WCA export zip: persons, competitions, results (filtered to a chosen
// event), and result_attempts (only for the kept result ids).

use ahash::{AHashMap, AHashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

pub struct Person {
    pub wca_id: String,
    pub sub_id: u16,
    pub name: String,
    pub country_id: String,
    pub gender: String,
}

pub struct Competition {
    pub id: String,
    pub year: i32,
}

pub struct Result333 {
    pub id: i64,
    pub pos: i32,
    pub best: i32,
    pub average: i32,
    pub comp_key: u32,
    pub round_type_id: String,
    pub person_key: u32, // points into persons (sub_id==1 entry)
    pub event_id: u16,
}

pub struct Attempt {
    pub result_id: i64,
    pub attempt_number: u8,
    pub value: i32,
}

pub struct WcaData {
    pub persons: Vec<Person>,              // all persons (all sub_ids)
    pub person_idx_by_wca_id: AHashMap<String, u32>, // wca_id -> index of sub_id==1 entry
    pub competitions: Vec<Competition>,
    pub comp_idx_by_id: AHashMap<String, u32>,
    pub events: Vec<String>,
    pub event_idx: AHashMap<String, u16>,
    pub results: Vec<Result333>,
    pub attempts_by_result: AHashMap<i64, Vec<Attempt>>, // sorted by attempt_number
}

impl WcaData {
    pub fn event_id(&self, name: &str) -> Option<u16> {
        self.event_idx.get(name).copied()
    }
    pub fn person_key(&self, wca_id: &str) -> Option<u32> {
        self.person_idx_by_wca_id.get(wca_id).copied()
    }

    pub fn event_years(&self, event_id: u16) -> Vec<i32> {
        let mut comp_ids: AHashSet<u32> = AHashSet::new();
        for r in &self.results {
            if r.event_id == event_id {
                comp_ids.insert(r.comp_key);
            }
        }
        let (mut lo, mut hi) = (i32::MAX, i32::MIN);
        for cid in &comp_ids {
            let y = self.competitions[*cid as usize].year;
            if y < lo { lo = y; }
            if y > hi { hi = y; }
        }
        (lo..=hi).collect()
    }

    pub fn person_event_years(&self, person_key: u32, event_id: u16) -> Vec<i32> {
        let mut all_comp: AHashSet<u32> = AHashSet::new();
        let mut person_comp: AHashSet<u32> = AHashSet::new();
        for r in &self.results {
            if r.event_id != event_id {
                continue;
            }
            all_comp.insert(r.comp_key);
            if r.person_key == person_key {
                person_comp.insert(r.comp_key);
            }
        }
        let mut lo = i32::MAX;
        for c in &person_comp {
            lo = lo.min(self.competitions[*c as usize].year);
        }
        let mut hi = i32::MIN;
        for c in &all_comp {
            hi = hi.max(self.competitions[*c as usize].year);
        }
        (lo..=hi).collect()
    }
}

pub fn load_wca(zip_path: &Path) -> anyhow::Result<WcaData> {
    let name = zip_path.file_name().unwrap().to_string_lossy().to_string();
    anyhow::ensure!(
        name.contains("WCA_export_v2_"),
        "not a WCA v2 export zip"
    );
    let f = File::open(zip_path)?;
    let mut zip = zip::ZipArchive::new(f)?;

    // persons
    let mut persons = Vec::new();
    let mut person_idx: AHashMap<String, u32> = AHashMap::new();
    {
        let file = zip.by_name("WCA_export_persons.tsv")?;
        let mut rdr = make_reader(file);
        let mut rec = csv::ByteRecord::new();
        // header row consumed by csv reader
        while rdr.read_byte_record(&mut rec)? {
            let name = field(&rec, 0);
            let gender = field(&rec, 1);
            let wca_id = field(&rec, 2);
            let sub_id: u16 = field(&rec, 3).parse().unwrap_or(1);
            let country = field(&rec, 4);
            if sub_id == 1 {
                person_idx.insert(wca_id.to_string(), persons.len() as u32);
            }
            persons.push(Person {
                name: name.to_string(),
                gender: gender.to_string(),
                wca_id: wca_id.to_string(),
                sub_id,
                country_id: country.to_string(),
            });
        }
    }

    // competitions (we only need id + year). Competitions tsv has many fields
    // and some free-text ones, but row boundaries are line breaks and fields
    // are plain tab-separated; columns we want are fixed-position by header.
    let mut competitions = Vec::new();
    let mut comp_idx: AHashMap<String, u32> = AHashMap::new();
    {
        let file = zip.by_name("WCA_export_competitions.tsv")?;
        let mut rdr = BufReader::with_capacity(1 << 20, file);
        let mut header = String::new();
        rdr.read_line(&mut header)?;
        let header_fields: Vec<&str> = header.trim_end_matches('\n').split('\t').collect();
        let id_col = header_fields.iter().position(|x| *x == "id").unwrap();
        let year_col = header_fields.iter().position(|x| *x == "year").unwrap();
        let mut line = String::new();
        loop {
            line.clear();
            let n = rdr.read_line(&mut line)?;
            if n == 0 { break; }
            let line_t = line.trim_end_matches('\n');
            let fs: Vec<&str> = line_t.split('\t').collect();
            if fs.len() <= year_col { continue; }
            let id = fs[id_col].to_string();
            let year: i32 = fs[year_col].parse().unwrap_or(0);
            comp_idx.insert(id.clone(), competitions.len() as u32);
            competitions.push(Competition { id, year });
        }
    }

    // events: build from results as they are loaded (unique event ids).
    let mut events: Vec<String> = Vec::new();
    let mut event_idx: AHashMap<String, u16> = AHashMap::new();

    // results
    let mut results: Vec<Result333> = Vec::new();
    {
        let file = zip.by_name("WCA_export_results.tsv")?;
        let mut rdr = make_reader(file);
        let mut rec = csv::ByteRecord::new();
        // id, pos, best, average, competition_id, round_type_id, event_id,
        // person_name, person_id, format_id, regional_single_record,
        // regional_average_record, person_country_id
        while rdr.read_byte_record(&mut rec)? {
            let id: i64 = field(&rec, 0).parse()?;
            let pos: i32 = field(&rec, 1).parse().unwrap_or(0);
            let best: i32 = field(&rec, 2).parse().unwrap_or(0);
            let average: i32 = field(&rec, 3).parse().unwrap_or(0);
            let comp_id = field(&rec, 4);
            let round_type = field(&rec, 5).to_string();
            let ev = field(&rec, 6);
            let person_id = field(&rec, 8);
            let comp_key = match comp_idx.get(comp_id) {
                Some(k) => *k,
                None => continue,
            };
            let person_key = match person_idx.get(person_id) {
                Some(k) => *k,
                None => continue,
            };
            let event_id = match event_idx.get(ev) {
                Some(&k) => k,
                None => {
                    let k = events.len() as u16;
                    events.push(ev.to_string());
                    event_idx.insert(ev.to_string(), k);
                    k
                }
            };
            results.push(Result333 {
                id,
                pos,
                best,
                average,
                comp_key,
                round_type_id: round_type,
                person_key,
                event_id,
            });
        }
    }

    // result_attempts: only keep attempts whose result_id is in the set of
    // result ids for events we care about. For generality (we support any
    // event via calc), keep all attempts whose result_id is in our results.
    let wanted: AHashSet<i64> = results.iter().map(|r| r.id).collect();
    let mut attempts_by_result: AHashMap<i64, Vec<Attempt>> = AHashMap::new();
    {
        let file = zip.by_name("WCA_export_result_attempts.tsv")?;
        let mut rdr = make_reader(file);
        let mut rec = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut rec)? {
            let value: i32 = match field(&rec, 0).parse() { Ok(v) => v, Err(_) => continue };
            let attempt_number: u8 = match field(&rec, 1).parse() { Ok(v) => v, Err(_) => continue };
            let result_id: i64 = match field(&rec, 2).parse() { Ok(v) => v, Err(_) => continue };
            if !wanted.contains(&result_id) { continue; }
            attempts_by_result
                .entry(result_id)
                .or_default()
                .push(Attempt { result_id, attempt_number, value });
        }
    }
    for v in attempts_by_result.values_mut() {
        v.sort_by_key(|a| a.attempt_number);
    }

    Ok(WcaData {
        persons,
        person_idx_by_wca_id: person_idx,
        competitions,
        comp_idx_by_id: comp_idx,
        events,
        event_idx,
        results,
        attempts_by_result,
    })
}

fn make_reader<R: Read>(r: R) -> csv::Reader<BufReader<R>> {
    csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .quoting(false)
        .has_headers(true)
        .flexible(true)
        .from_reader(BufReader::with_capacity(1 << 20, r))
}

fn field<'a>(rec: &'a csv::ByteRecord, i: usize) -> &'a str {
    std::str::from_utf8(rec.get(i).unwrap_or(b"")).unwrap_or("")
}
