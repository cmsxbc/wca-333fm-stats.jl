// Per-year calculation of 3x3 FM statistics. Mirrors WCAStats.jl:calc().

use ahash::{AHashMap, AHashSet};

use crate::loader::{Attempt, WcaData};
use crate::stats;

#[derive(Clone, Copy)]
pub enum YearFilter {
    Eq(i32),
    Le(i32),
}

impl YearFilter {
    fn matches(self, year: i32) -> bool {
        match self {
            YearFilter::Eq(y) => year == y,
            YearFilter::Le(y) => year <= y,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ColKind { Int, Float }

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ColDir { Asc, Desc }

#[derive(Clone)]
pub enum Cell {
    Missing,
    Int(i64),
    Float(f64),
}

impl Cell {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Cell::Missing => None,
            Cell::Int(v) => Some(*v as f64),
            Cell::Float(v) => Some(*v),
        }
    }
    pub fn is_missing(&self) -> bool {
        matches!(self, Cell::Missing)
    }
}

// Ordered value-column schema, matching Julia output.
// (name, kind, direction for ranking)
pub const COLS: &[(&str, ColKind, ColDir)] = &[
    ("competitions",                ColKind::Int,   ColDir::Desc),
    ("rounds",                      ColKind::Int,   ColDir::Desc),
    ("best",                        ColKind::Int,   ColDir::Asc),
    ("best_max",                    ColKind::Int,   ColDir::Asc),
    ("best_count",                  ColKind::Int,   ColDir::Desc),
    ("best_nunique",                ColKind::Int,   ColDir::Desc),
    ("best_mean",                   ColKind::Float, ColDir::Asc),
    ("best_std",                    ColKind::Float, ColDir::Asc),
    ("best_avg",                    ColKind::Float, ColDir::Asc),
    ("best_median",                 ColKind::Float, ColDir::Asc),
    ("best_mode",                   ColKind::Int,   ColDir::Asc),
    ("best_mode_count",             ColKind::Int,   ColDir::Desc),
    ("best_consecutive",            ColKind::Int,   ColDir::Desc),
    ("best_consecutive_start",      ColKind::Int,   ColDir::Asc),
    ("best_consecutive_end",        ColKind::Int,   ColDir::Asc),
    ("average_attempts",            ColKind::Int,   ColDir::Desc),
    ("average",                     ColKind::Float, ColDir::Asc),
    ("average_max",                 ColKind::Float, ColDir::Asc),
    ("average_count",               ColKind::Int,   ColDir::Desc),
    ("average_nunique",             ColKind::Int,   ColDir::Desc),
    ("average_mean",                ColKind::Float, ColDir::Asc),
    ("average_std",                 ColKind::Float, ColDir::Asc),
    ("average_avg",                 ColKind::Float, ColDir::Asc),
    ("average_median",              ColKind::Float, ColDir::Asc),
    ("average_mode",                ColKind::Float, ColDir::Asc),
    ("average_mode_count",          ColKind::Int,   ColDir::Desc),
    ("average_consecutive",         ColKind::Int,   ColDir::Desc),
    ("average_consecutive_start",   ColKind::Float, ColDir::Asc),
    ("average_consecutive_end",     ColKind::Float, ColDir::Asc),
    ("gold",                        ColKind::Int,   ColDir::Desc),
    ("silver",                      ColKind::Int,   ColDir::Desc),
    ("bronze",                      ColKind::Int,   ColDir::Desc),
    ("chances",                     ColKind::Int,   ColDir::Desc),
    ("attempts",                    ColKind::Int,   ColDir::Desc),
    ("solved_count",                ColKind::Int,   ColDir::Desc),
    ("solved_nunique",              ColKind::Int,   ColDir::Desc),
    ("solved_mean",                 ColKind::Float, ColDir::Asc),
    ("solved_std",                  ColKind::Float, ColDir::Asc),
    ("solved_avg",                  ColKind::Float, ColDir::Asc),
    ("solved_median",               ColKind::Float, ColDir::Asc),
    ("solved_mode",                 ColKind::Int,   ColDir::Asc),
    ("solved_mode_count",           ColKind::Int,   ColDir::Desc),
    ("solved_min",                  ColKind::Int,   ColDir::Asc),
    ("solved_max",                  ColKind::Int,   ColDir::Asc),
    ("solved_consecutive",          ColKind::Int,   ColDir::Desc),
    ("solved_consecutive_start",    ColKind::Int,   ColDir::Asc),
    ("solved_consecutive_end",      ColKind::Int,   ColDir::Asc),
    ("solved_mo3_last",             ColKind::Float, ColDir::Asc),
    ("solved_mo3_best",             ColKind::Float, ColDir::Asc),
    ("solved_mo5_last",             ColKind::Float, ColDir::Asc),
    ("solved_mo5_best",             ColKind::Float, ColDir::Asc),
    ("solved_mo12_last",            ColKind::Float, ColDir::Asc),
    ("solved_mo12_best",            ColKind::Float, ColDir::Asc),
    ("solved_mo50_last",            ColKind::Float, ColDir::Asc),
    ("solved_mo50_best",            ColKind::Float, ColDir::Asc),
    ("solved_mo100_last",           ColKind::Float, ColDir::Asc),
    ("solved_mo100_best",           ColKind::Float, ColDir::Asc),
    ("solved_ao5_last",             ColKind::Float, ColDir::Asc),
    ("solved_ao5_best",             ColKind::Float, ColDir::Asc),
    ("solved_ao12_last",            ColKind::Float, ColDir::Asc),
    ("solved_ao12_best",            ColKind::Float, ColDir::Asc),
    ("solved_ao50_last",            ColKind::Float, ColDir::Asc),
    ("solved_ao50_best",            ColKind::Float, ColDir::Asc),
    ("solved_ao100_last",           ColKind::Float, ColDir::Asc),
    ("solved_ao100_best",           ColKind::Float, ColDir::Asc),
    ("avg_item_3rd_min",            ColKind::Int,   ColDir::Asc),
    ("avg_item_3rd_max",            ColKind::Int,   ColDir::Asc),
    ("avg_item_2nd_min",            ColKind::Int,   ColDir::Asc),
    ("avg_item_2nd_max",            ColKind::Int,   ColDir::Asc),
];

pub fn col_idx(name: &str) -> usize {
    COLS.iter().position(|(n, _, _)| *n == name).unwrap_or_else(|| {
        panic!("unknown column: {}", name)
    })
}

// Order in which Julia emits rank/nr columns: all asc cols (in COLS order),
// then desc cols (in the literal order specified in Julia's `desc_cols`).
pub const DESC_ORDER: &[&str] = &[
    "competitions", "rounds", "chances", "attempts",
    "solved_count", "solved_nunique", "solved_mode_count", "solved_consecutive",
    "best_count", "best_nunique", "best_mode_count", "best_consecutive",
    "average_attempts", "average_count", "average_nunique", "average_mode_count", "average_consecutive",
    "gold", "silver", "bronze",
];

pub fn rank_col_order() -> Vec<usize> {
    let mut out: Vec<usize> = Vec::new();
    for (i, (_, _, dir)) in COLS.iter().enumerate() {
        if *dir == ColDir::Asc { out.push(i); }
    }
    for name in DESC_ORDER {
        out.push(col_idx(name));
    }
    out
}

#[derive(Clone)]
pub struct Row {
    pub person_key: u32,
    pub person_id: String,
    pub person_name: String,
    pub country_id: String,
    pub gender: String,
    pub vals: Vec<Cell>,       // len == COLS.len()
    pub ranks: Vec<Cell>,      // len == COLS.len(); Missing if vals missing
    pub nrs: Vec<Cell>,        // len == COLS.len()
    pub year: Option<i64>,
    pub category: Option<String>,
}

impl Row {
    fn new(
        person_key: u32,
        person_id: String,
        person_name: String,
        country_id: String,
        gender: String,
    ) -> Self {
        Row {
            person_key,
            person_id,
            person_name,
            country_id,
            gender,
            vals: vec![Cell::Missing; COLS.len()],
            ranks: vec![Cell::Missing; COLS.len()],
            nrs: vec![Cell::Missing; COLS.len()],
            year: None,
            category: None,
        }
    }

    pub fn set_meta(&mut self, key: &str, v: Cell) {
        match key {
            "year" => self.year = match v { Cell::Int(n) => Some(n), _ => None },
            "category" => self.category = match v {
                Cell::Int(n) => Some(n.to_string()),
                Cell::Float(f) => Some(f.to_string()),
                Cell::Missing => None,
            },
            _ => {}
        }
    }

    pub fn year_cell_str(&self) -> String {
        match self.year {
            Some(y) => y.to_string(),
            None => String::new(),
        }
    }
    pub fn category_cell_str(&self) -> String {
        self.category.clone().unwrap_or_default()
    }

    /// Element-wise numeric subtraction (self - other) for all val/rank/nr
    /// columns. Meta (person info) is carried from `self` (the later row).
    /// Category is renamed to "{cat}-year-detla" (Julia typo preserved).
    pub fn delta(&self, other: &Row, _meta_cols: &'static [&'static str]) -> Row {
        fn sub(a: &Cell, b: &Cell, kind: ColKind) -> Cell {
            match (a, b) {
                (Cell::Missing, _) | (_, Cell::Missing) => Cell::Missing,
                _ => {
                    let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) else { return Cell::Missing; };
                    let d = x - y;
                    if kind == ColKind::Int { Cell::Int(d as i64) } else { Cell::Float(d) }
                }
            }
        }
        let mut row = self.clone();
        for i in 0..COLS.len() {
            let kind = COLS[i].1;
            row.vals[i] = sub(&self.vals[i], &other.vals[i], kind);
            row.ranks[i] = sub(&self.ranks[i], &other.ranks[i], ColKind::Int);
            row.nrs[i] = sub(&self.nrs[i], &other.nrs[i], ColKind::Int);
        }
        // category suffix
        row.category = self.category.as_ref().map(|c| {
            if let Some(base) = c.strip_suffix("-year") {
                format!("{}-year-detla", base)
            } else {
                format!("{}-detla", c)
            }
        });
        row
    }
}


pub struct Frame {
    pub rows: Vec<Row>,
    pub year_filter: YearFilter,
}

impl Frame {
    pub fn row_for_person(&self, person_key: u32) -> Option<Row> {
        self.rows.iter().find(|r| r.person_key == person_key).cloned()
    }

    pub fn empty_row(&self, person_key: u32) -> Row {
        // personName/countryId/gender unknown; caller should fill from data
        // if needed.
        Row::new(person_key, String::new(), String::new(), String::new(), String::new())
    }

    pub fn header_for_summary(&self) -> Vec<String> {
        let mut h = vec!["personId".to_string(), "personName".to_string(),
            "countryId".to_string(), "gender".to_string()];
        for (n, _, _) in COLS { h.push((*n).to_string()); }
        for (n, _, _) in COLS { h.push(format!("{}_rank", n)); }
        for (n, _, _) in COLS { h.push(format!("{}_nr", n)); }
        h.push("year".to_string());
        h.push("category".to_string());
        h
    }

    pub fn person_meta_cols(&self) -> &'static [&'static str] {
        &["personId", "personName", "countryId", "gender", "year", "category"]
    }
}

pub fn calc(data: &WcaData, event_id: u16, yf: YearFilter) -> Frame {
    // Step 1: filter results to event and year.
    let mut kept: Vec<usize> = Vec::new(); // indices into data.results
    for (i, r) in data.results.iter().enumerate() {
        if r.event_id != event_id { continue; }
        let y = data.competitions[r.comp_key as usize].year;
        if !yf.matches(y) { continue; }
        kept.push(i);
    }

    // Step 2: group by person_key -> list of result indices.
    let mut by_person: AHashMap<u32, Vec<usize>> = AHashMap::new();
    for &i in &kept {
        by_person.entry(data.results[i].person_key).or_default().push(i);
    }
    // Julia's DataFrames join pipeline produces rows sorted by personId string,
    // except persons not found in the sub_id==1 persons table are appended last
    // (unmatched rightjoin rows) — also alphabetically among themselves.
    let mut person_order: Vec<u32> = by_person.keys().copied().collect();
    person_order.sort_by(|&a, &b| {
        let pa = &data.persons[a as usize];
        let pb = &data.persons[b as usize];
        let ma = data.person_idx_by_wca_id.contains_key(&pa.wca_id);
        let mb = data.person_idx_by_wca_id.contains_key(&pb.wca_id);
        mb.cmp(&ma).then_with(|| pa.wca_id.cmp(&pb.wca_id))
    });

    let mut rows: Vec<Row> = Vec::with_capacity(person_order.len());
    for pk in person_order {
        let p = &data.persons[pk as usize];
        let mut row = Row::new(
            pk,
            p.wca_id.clone(),
            p.name.clone(),
            p.country_id.clone(),
            p.gender.clone(),
        );
        compute_row(&mut row, data, pk, &by_person[&pk]);
        rows.push(row);
    }

    // Step 3: compute ranks.
    compute_ranks(&mut rows);

    Frame { rows, year_filter: yf }
}

fn compute_row(row: &mut Row, data: &WcaData, _person_key: u32, idxs: &[usize]) {
    let set = |row: &mut Row, name: &str, c: Cell| {
        row.vals[col_idx(name)] = c;
    };

    // --- Round-level stats ---
    let rs: Vec<&crate::loader::Result333> = idxs.iter().map(|&i| &data.results[i]).collect();

    // competitions (unique comp_keys)
    let mut comp_set: AHashSet<u32> = AHashSet::new();
    for r in &rs { comp_set.insert(r.comp_key); }
    set(row, "competitions", Cell::Int(comp_set.len() as i64));
    set(row, "rounds", Cell::Int(rs.len() as i64));

    // best stats on best > 0
    let bests: Vec<i64> = rs.iter().filter(|r| r.best > 0).map(|r| r.best as i64).collect();
    if !bests.is_empty() {
        let (mn, mx) = extrema(&bests);
        set(row, "best", Cell::Int(mn));
        set(row, "best_max", Cell::Int(mx));
        set(row, "best_count", Cell::Int(bests.len() as i64));
        let mut uniq = bests.clone(); uniq.sort(); uniq.dedup();
        set(row, "best_nunique", Cell::Int(uniq.len() as i64));
        set(row, "best_mean", Cell::Float(stats::mean_i(&bests)));
        set(row, "best_std", Cell::Float(stats::std_i(&bests)));
        match stats::trim_avg_i(&bests) {
            Some(v) => set(row, "best_avg", Cell::Float(v)),
            None => {}
        }
        set(row, "best_median", Cell::Float(stats::median_f_from_i(&bests)));
        let (mode, mc) = stats::mode_count_i(&bests);
        set(row, "best_mode", Cell::Int(mode));
        set(row, "best_mode_count", Cell::Int(mc));
        let (cc, cs, ce) = stats::calc_consecutive(&bests, &[1]);
        set(row, "best_consecutive", Cell::Int(cc));
        set(row, "best_consecutive_start", Cell::Int(cs));
        set(row, "best_consecutive_end", Cell::Int(ce));
    }

    // average_attempts on average != 0
    let avg_attempts = rs.iter().filter(|r| r.average != 0).count();
    if avg_attempts > 0 {
        set(row, "average_attempts", Cell::Int(avg_attempts as i64));
    }

    // average stats on average > 0
    let avgs_i: Vec<i64> = rs.iter().filter(|r| r.average > 0).map(|r| r.average as i64).collect();
    if !avgs_i.is_empty() {
        let avgs_real: Vec<f64> = avgs_i.iter().map(|&v| v as f64 / 100.0).collect();
        let (mn, mx) = extrema_f(&avgs_real);
        set(row, "average", Cell::Float(mn));
        set(row, "average_max", Cell::Float(mx));
        set(row, "average_count", Cell::Int(avgs_real.len() as i64));
        let mut uniq = avgs_i.clone(); uniq.sort(); uniq.dedup();
        set(row, "average_nunique", Cell::Int(uniq.len() as i64));
        set(row, "average_mean", Cell::Float(stats::mean_f(&avgs_real)));
        set(row, "average_std", Cell::Float(stats::std_f(&avgs_real)));
        if let Some(v) = stats::trim_avg_f(&avgs_real) {
            set(row, "average_avg", Cell::Float(v));
        }
        // median of Float vector: sort then pick. Use f64 sort.
        let mut sorted = avgs_real.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        let med = if n % 2 == 1 { sorted[n/2] } else { (sorted[n/2-1]+sorted[n/2])/2.0 };
        set(row, "average_median", Cell::Float(med));
        // mode is computed on integer average, then divided by 100.
        let (mode_i, mc) = stats::mode_count_i(&avgs_i);
        set(row, "average_mode", Cell::Float(mode_i as f64 / 100.0));
        set(row, "average_mode_count", Cell::Int(mc));
        let (cc, cs, ce) = stats::calc_consecutive(&avgs_i, &[33, 34]);
        set(row, "average_consecutive", Cell::Int(cc));
        set(row, "average_consecutive_start", Cell::Float(cs as f64 / 100.0));
        set(row, "average_consecutive_end", Cell::Float(ce as f64 / 100.0));
    }

    // medals: final rounds (f or c) with best > 0; count pos==1,2,3
    let (mut g, mut s, mut b) = (0i64, 0i64, 0i64);
    for r in &rs {
        if r.best > 0 && (r.round_type_id == "f" || r.round_type_id == "c") {
            match r.pos { 1 => g+=1, 2 => s+=1, 3 => b+=1, _ => {} }
        }
    }
    // Julia uses rightjoin semantics, but medals are produced only if any
    // row matched the filter. With no matches, gold/silver/bronze stay
    // missing. (In Julia, they become missing for persons with no finals.)
    // All persons in our rows have at least one result; those without any
    // final-with-success get missing medals (matches Julia).
    let any_final_best = rs.iter().any(|r| r.best > 0 && (r.round_type_id == "f" || r.round_type_id == "c"));
    if any_final_best {
        set(row, "gold", Cell::Int(g));
        set(row, "silver", Cell::Int(s));
        set(row, "bronze", Cell::Int(b));
    }

    // --- Single attempts (per-attempt level) ---
    // Build the single_res_df equivalent: sorted by (result_id, attempt_number).
    // We iterate results in sorted id order; attempts are already per-result
    // sorted by attempt_number in loader.
    let mut sorted_rs: Vec<&crate::loader::Result333> = rs.clone();
    sorted_rs.sort_by_key(|r| r.id);

    // Collect all attempts, preserving order (missing attempts => leftjoin
    // keeps one row per result even with no attempts).
    struct SingleRow<'a> {
        result_id: i64,
        attempt_number: Option<u8>,
        value: Option<i32>,
        result: &'a crate::loader::Result333,
    }
    let mut single: Vec<SingleRow> = Vec::new();
    for r in &sorted_rs {
        match data.attempts_by_result.get(&r.id) {
            Some(atts) if !atts.is_empty() => {
                for a in atts {
                    single.push(SingleRow {
                        result_id: r.id,
                        attempt_number: Some(a.attempt_number),
                        value: Some(a.value),
                        result: r,
                    });
                }
            }
            _ => {
                single.push(SingleRow {
                    result_id: r.id,
                    attempt_number: None,
                    value: None,
                    result: r,
                });
            }
        }
    }

    // chances = total rows in single
    set(row, "chances", Cell::Int(single.len() as i64));
    // attempts = rows with value > -2 (i.e., not DNS; DNF=-1 kept; positive values kept)
    let attempts_count: i64 = single.iter().filter(|s| s.value.map(|v| v > -2).unwrap_or(false)).count() as i64;
    set(row, "attempts", Cell::Int(attempts_count));

    // solved = value > 0 (in order of single iteration, which is sorted by (id, attempt_number))
    let solved: Vec<i64> = single
        .iter()
        .filter_map(|s| s.value.filter(|v| *v > 0).map(|v| v as i64))
        .collect();

    if !solved.is_empty() {
        set(row, "solved_count", Cell::Int(solved.len() as i64));
        let mut uniq = solved.clone(); uniq.sort(); uniq.dedup();
        set(row, "solved_nunique", Cell::Int(uniq.len() as i64));
        set(row, "solved_mean", Cell::Float(stats::mean_i(&solved)));
        set(row, "solved_std", Cell::Float(stats::std_i(&solved)));
        if let Some(v) = stats::trim_avg_i(&solved) {
            set(row, "solved_avg", Cell::Float(v));
        }
        set(row, "solved_median", Cell::Float(stats::median_f_from_i(&solved)));
        let (mode, mc) = stats::mode_count_i(&solved);
        set(row, "solved_mode", Cell::Int(mode));
        set(row, "solved_mode_count", Cell::Int(mc));
        let (mn, mx) = extrema(&solved);
        set(row, "solved_min", Cell::Int(mn));
        set(row, "solved_max", Cell::Int(mx));
        let (cc, cs, ce) = stats::calc_consecutive(&solved, &[1]);
        set(row, "solved_consecutive", Cell::Int(cc));
        set(row, "solved_consecutive_start", Cell::Int(cs));
        set(row, "solved_consecutive_end", Cell::Int(ce));

        for n in [3usize, 5, 12, 50, 100] {
            if let Some((last, best)) = stats::rolling_mean(&solved, n) {
                set(row, &format!("solved_mo{}_last", n), Cell::Float(last));
                set(row, &format!("solved_mo{}_best", n), Cell::Float(best));
            }
        }
        for n in [5usize, 12, 50, 100] {
            if let Some((last, best)) = stats::rolling_trim_avg(&solved, n) {
                set(row, &format!("solved_ao{}_last", n), Cell::Float(last));
                set(row, &format!("solved_ao{}_best", n), Cell::Float(best));
            }
        }
    }

    // --- avg_item_3rd/2nd (per round average, then per person extrema) ---
    // For each result with average > 0, compute max and median (as i64) of
    // its attempt values. Note: attempts here are the raw values from
    // result_attempts (can include -1/-2 for DNF/DNS). Julia operates on
    // the :value column of single_df after filter(:average => >(0)).
    let mut worsts: Vec<i64> = Vec::new();
    let mut medians: Vec<i64> = Vec::new();
    for r in &sorted_rs {
        if r.average <= 0 { continue; }
        if let Some(atts) = data.attempts_by_result.get(&r.id) {
            let vs: Vec<i64> = atts.iter().map(|a| a.value as i64).collect();
            if vs.is_empty() { continue; }
            worsts.push(*vs.iter().max().unwrap());
            medians.push(stats::median_i(&vs));
        }
    }
    if !worsts.is_empty() {
        let (mn, mx) = extrema(&worsts);
        set(row, "avg_item_3rd_min", Cell::Int(mn));
        set(row, "avg_item_3rd_max", Cell::Int(mx));
        let (mn, mx) = extrema(&medians);
        set(row, "avg_item_2nd_min", Cell::Int(mn));
        set(row, "avg_item_2nd_max", Cell::Int(mx));
    }
}

fn extrema(xs: &[i64]) -> (i64, i64) {
    let mut mn = xs[0]; let mut mx = xs[0];
    for &v in &xs[1..] {
        if v < mn { mn = v; }
        if v > mx { mx = v; }
    }
    (mn, mx)
}
fn extrema_f(xs: &[f64]) -> (f64, f64) {
    let mut mn = xs[0]; let mut mx = xs[0];
    for &v in &xs[1..] {
        if v < mn { mn = v; }
        if v > mx { mx = v; }
    }
    (mn, mx)
}

fn compute_ranks(rows: &mut [Row]) {
    for ci in 0..COLS.len() {
        let dir = COLS[ci].2;
        // world rank across all rows
        let ranks = competerank_col(rows, ci, dir, None);
        for (i, r) in ranks.into_iter().enumerate() {
            rows[i].ranks[ci] = r;
        }
    }
    // national ranks: group by countryId
    let mut by_country: AHashMap<String, Vec<usize>> = AHashMap::new();
    for (i, r) in rows.iter().enumerate() {
        by_country.entry(r.country_id.clone()).or_default().push(i);
    }
    for ci in 0..COLS.len() {
        let dir = COLS[ci].2;
        for (_country, idxs) in &by_country {
            let ranks = competerank_col(rows, ci, dir, Some(idxs));
            for (j, r) in ranks.into_iter().enumerate() {
                rows[idxs[j]].nrs[ci] = r;
            }
        }
    }
}

fn competerank_col(
    rows: &[Row],
    ci: usize,
    dir: ColDir,
    subset: Option<&Vec<usize>>,
) -> Vec<Cell> {
    // Build (index, value) pairs, skipping missing (keep None marker).
    let n = subset.map(|s| s.len()).unwrap_or(rows.len());
    let get_row = |k: usize| -> &Row {
        match subset {
            Some(s) => &rows[s[k]],
            None => &rows[k],
        }
    };
    let mut out: Vec<Cell> = vec![Cell::Missing; n];
    let mut present: Vec<(usize, f64)> = Vec::with_capacity(n);
    for k in 0..n {
        let v = &get_row(k).vals[ci];
        if let Some(f) = v.as_f64() {
            present.push((k, f));
        }
    }
    if present.is_empty() { return out; }
    // Sort: Julia isless orders NaN after everything, so treat NaN as
    // greater than any non-NaN.
    let cmp_f = |a: f64, b: f64| -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        let an = a.is_nan();
        let bn = b.is_nan();
        match (an, bn) {
            (true, true) => Equal,
            (true, false) => Greater,
            (false, true) => Less,
            _ => a.partial_cmp(&b).unwrap(),
        }
    };
    present.sort_by(|x, y| {
        let o = cmp_f(x.1, y.1);
        if dir == ColDir::Desc { o.reverse() } else { o }
    });
    let mut rank: i64 = 1;
    let mut prev: Option<f64> = None;
    for (pos, (k, v)) in present.iter().enumerate() {
        // Julia: NaN != NaN, so each NaN is distinct (no tie grouping).
        let eq = match prev {
            Some(p) => p == *v,
            None => false,
        };
        if !eq {
            rank = pos as i64 + 1;
        }
        out[*k] = Cell::Int(rank);
        prev = Some(*v);
    }
    out
}
