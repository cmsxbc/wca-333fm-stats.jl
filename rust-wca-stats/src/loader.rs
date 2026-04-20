// Load WCA export zip: persons, competitions, results (filtered to a chosen
// event), and result_attempts (only for the kept result ids).

use ahash::{AHashMap, AHashSet};
use std::fs::File;
use std::io::Read;
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
        let mut file = zip.by_name("WCA_export_persons.tsv")?;
        let mut buf = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buf)?;
        for_each_data_line(&buf, |line| {
            let mut it = TsvFields::new(line);
            let name = it.next_str();
            let gender = it.next_str();
            let wca_id = it.next_str();
            let sub_id: u16 = parse_int_bytes(it.next_bytes()).unwrap_or(1);
            let country = it.next_str();
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
        });
    }

    // competitions (we only need id + year). Competitions tsv has many fields
    // and some free-text ones, but row boundaries are line breaks and fields
    // are plain tab-separated; columns we want are fixed-position by header.
    let mut competitions = Vec::new();
    let mut comp_idx: AHashMap<String, u32> = AHashMap::new();
    {
        let mut file = zip.by_name("WCA_export_competitions.tsv")?;
        let mut buf = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buf)?;
        let (hdr, body) = split_header(&buf);
        let id_col = header_col(hdr, b"id").expect("no id column");
        let year_col = header_col(hdr, b"year").expect("no year column");
        let max_col = id_col.max(year_col);
        for_each_line(body, |line| {
            let mut fields: Vec<&[u8]> = Vec::with_capacity(max_col + 1);
            let mut start = 0usize;
            for (i, b) in line.iter().enumerate() {
                if *b == b'\t' {
                    fields.push(&line[start..i]);
                    start = i + 1;
                    if fields.len() > max_col { break; }
                }
            }
            if fields.len() <= max_col {
                fields.push(&line[start..]);
            }
            if fields.len() <= max_col { return; }
            let id = std::str::from_utf8(fields[id_col]).unwrap_or("").to_string();
            let year: i32 = parse_int_bytes(fields[year_col]).unwrap_or(0);
            comp_idx.insert(id.clone(), competitions.len() as u32);
            competitions.push(Competition { id, year });
        });
    }

    // events: build from results as they are loaded (unique event ids).
    let mut events: Vec<String> = Vec::new();
    let mut event_idx: AHashMap<String, u16> = AHashMap::new();

    // results
    let mut results: Vec<Result333> = Vec::new();
    {
        let mut file = zip.by_name("WCA_export_results.tsv")?;
        let mut buf = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buf)?;
        for_each_data_line(&buf, |line| {
            let mut it = TsvFields::new(line);
            let id: i64 = match parse_int_bytes(it.next_bytes()) { Some(v) => v, None => return };
            let pos: i32 = parse_int_bytes(it.next_bytes()).unwrap_or(0);
            let best: i32 = parse_int_bytes(it.next_bytes()).unwrap_or(0);
            let average: i32 = parse_int_bytes(it.next_bytes()).unwrap_or(0);
            let comp_id = it.next_str();
            let round_type = it.next_str();
            let ev = it.next_str();
            it.skip();                    // person_name
            let person_id = it.next_str();

            let comp_key = match comp_idx.get(comp_id) {
                Some(k) => *k,
                None => return,
            };
            let person_key = match person_idx.get(person_id) {
                Some(k) => *k,
                None => return,
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
                round_type_id: round_type.to_string(),
                person_key,
                event_id,
            });
        });
    }

    // result_attempts: only keep attempts whose result_id is in the set of
    // result ids for events we care about. For generality (we support any
    // event via calc), keep all attempts whose result_id is in our results.
    let wanted: AHashSet<i64> = results.iter().map(|r| r.id).collect();
    let mut attempts_by_result: AHashMap<i64, Vec<Attempt>> = AHashMap::new();
    {
        let mut file = zip.by_name("WCA_export_result_attempts.tsv")?;
        let mut buf = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buf)?;
        for_each_data_line(&buf, |line| {
            let mut it = TsvFields::new(line);
            let value: i32 = match parse_int_bytes(it.next_bytes()) { Some(v) => v, None => return };
            let attempt_number: u8 = match parse_int_bytes::<u32>(it.next_bytes()) {
                Some(v) => v as u8, None => return };
            let result_id: i64 = match parse_int_bytes(it.next_bytes()) { Some(v) => v, None => return };
            if !wanted.contains(&result_id) { return; }
            attempts_by_result
                .entry(result_id)
                .or_default()
                .push(Attempt { result_id, attempt_number, value });
        });
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

// -------- fast TSV helpers (no UTF-8 checks on numeric fields) --------

fn split_header(buf: &[u8]) -> (&[u8], &[u8]) {
    match memchr::memchr(b'\n', buf) {
        Some(p) => {
            let hdr_end = if p > 0 && buf[p - 1] == b'\r' { p - 1 } else { p };
            (&buf[..hdr_end], &buf[p + 1..])
        }
        None => (buf, &buf[..0]),
    }
}

fn header_col(hdr: &[u8], name: &[u8]) -> Option<usize> {
    let mut i = 0;
    let mut col = 0;
    loop {
        let next = memchr::memchr(b'\t', &hdr[i..]).map(|p| i + p);
        let end = next.unwrap_or(hdr.len());
        if &hdr[i..end] == name { return Some(col); }
        match next {
            Some(p) => { i = p + 1; col += 1; }
            None => return None,
        }
    }
}

fn for_each_line<F: FnMut(&[u8])>(mut buf: &[u8], mut f: F) {
    while !buf.is_empty() {
        let (line, rest) = match memchr::memchr(b'\n', buf) {
            Some(p) => (&buf[..p], &buf[p + 1..]),
            None => (buf, &buf[..0]),
        };
        let line = if !line.is_empty() && line[line.len() - 1] == b'\r' {
            &line[..line.len() - 1]
        } else { line };
        if !line.is_empty() { f(line); }
        buf = rest;
    }
}

fn for_each_data_line<F: FnMut(&[u8])>(buf: &[u8], mut f: F) {
    // Skip the header row.
    let rest = match memchr::memchr(b'\n', buf) {
        Some(p) => &buf[p + 1..],
        None => return,
    };
    for_each_line(rest, |line| f(line));
}

struct TsvFields<'a> {
    rest: &'a [u8],
}

impl<'a> TsvFields<'a> {
    fn new(line: &'a [u8]) -> Self { Self { rest: line } }
    fn next_bytes(&mut self) -> &'a [u8] {
        match memchr::memchr(b'\t', self.rest) {
            Some(p) => {
                let out = &self.rest[..p];
                self.rest = &self.rest[p + 1..];
                out
            }
            None => {
                let out = self.rest;
                self.rest = &self.rest[..0];
                out
            }
        }
    }
    fn next_str(&mut self) -> &'a str {
        // All text fields in the WCA export are valid UTF-8.  Avoid the
        // validation cost on the hot path.
        unsafe { std::str::from_utf8_unchecked(self.next_bytes()) }
    }
    fn skip(&mut self) { self.next_bytes(); }
}

trait FromDecimal: Sized {
    fn from_dec(bytes: &[u8]) -> Option<Self>;
}
macro_rules! impl_from_dec {
    ($t:ty, $neg:expr) => {
        impl FromDecimal for $t {
            #[inline]
            fn from_dec(mut bytes: &[u8]) -> Option<Self> {
                if bytes.is_empty() { return None; }
                let neg = bytes[0] == b'-';
                if neg || bytes[0] == b'+' { bytes = &bytes[1..]; }
                if bytes.is_empty() { return None; }
                let mut n: Self = 0;
                for &b in bytes {
                    if !(b'0'..=b'9').contains(&b) { return None; }
                    n = n.checked_mul(10)?.checked_add((b - b'0') as Self)?;
                }
                if neg {
                    if !$neg { return None; }
                    Some(0 as Self - n)
                } else {
                    Some(n)
                }
            }
        }
    };
}
impl_from_dec!(i64, true);
impl_from_dec!(i32, true);
impl_from_dec!(u32, false);
impl_from_dec!(u16, false);

fn parse_int_bytes<T: FromDecimal>(b: &[u8]) -> Option<T> { T::from_dec(b) }
