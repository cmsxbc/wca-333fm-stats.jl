// Per-year calculation of 3x3 FM statistics. Mirrors WCAStats.jl:calc().

use ahash::{AHashMap, AHashSet};

use crate::loader::{WcaData};
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

// Precomputed column indices, resolved once instead of linearly scanning
// `COLS` on every `set()` call in the hot path of `compute_row`.
struct ColIdx {
    competitions: usize,
    rounds: usize,
    best: usize,
    best_max: usize,
    best_count: usize,
    best_nunique: usize,
    best_mean: usize,
    best_std: usize,
    best_avg: usize,
    best_median: usize,
    best_mode: usize,
    best_mode_count: usize,
    best_consecutive: usize,
    best_consecutive_start: usize,
    best_consecutive_end: usize,
    average_attempts: usize,
    average: usize,
    average_max: usize,
    average_count: usize,
    average_nunique: usize,
    average_mean: usize,
    average_std: usize,
    average_avg: usize,
    average_median: usize,
    average_mode: usize,
    average_mode_count: usize,
    average_consecutive: usize,
    average_consecutive_start: usize,
    average_consecutive_end: usize,
    gold: usize,
    silver: usize,
    bronze: usize,
    chances: usize,
    attempts: usize,
    solved_count: usize,
    solved_nunique: usize,
    solved_mean: usize,
    solved_std: usize,
    solved_avg: usize,
    solved_median: usize,
    solved_mode: usize,
    solved_mode_count: usize,
    solved_min: usize,
    solved_max: usize,
    solved_consecutive: usize,
    solved_consecutive_start: usize,
    solved_consecutive_end: usize,
    solved_mo: [(usize, usize, usize); 5],
    solved_ao: [(usize, usize, usize); 4],
    avg_item_3rd_min: usize,
    avg_item_3rd_max: usize,
    avg_item_2nd_min: usize,
    avg_item_2nd_max: usize,
}

impl ColIdx {
    fn new() -> Self {
        ColIdx {
            competitions: col_idx("competitions"),
            rounds: col_idx("rounds"),
            best: col_idx("best"),
            best_max: col_idx("best_max"),
            best_count: col_idx("best_count"),
            best_nunique: col_idx("best_nunique"),
            best_mean: col_idx("best_mean"),
            best_std: col_idx("best_std"),
            best_avg: col_idx("best_avg"),
            best_median: col_idx("best_median"),
            best_mode: col_idx("best_mode"),
            best_mode_count: col_idx("best_mode_count"),
            best_consecutive: col_idx("best_consecutive"),
            best_consecutive_start: col_idx("best_consecutive_start"),
            best_consecutive_end: col_idx("best_consecutive_end"),
            average_attempts: col_idx("average_attempts"),
            average: col_idx("average"),
            average_max: col_idx("average_max"),
            average_count: col_idx("average_count"),
            average_nunique: col_idx("average_nunique"),
            average_mean: col_idx("average_mean"),
            average_std: col_idx("average_std"),
            average_avg: col_idx("average_avg"),
            average_median: col_idx("average_median"),
            average_mode: col_idx("average_mode"),
            average_mode_count: col_idx("average_mode_count"),
            average_consecutive: col_idx("average_consecutive"),
            average_consecutive_start: col_idx("average_consecutive_start"),
            average_consecutive_end: col_idx("average_consecutive_end"),
            gold: col_idx("gold"),
            silver: col_idx("silver"),
            bronze: col_idx("bronze"),
            chances: col_idx("chances"),
            attempts: col_idx("attempts"),
            solved_count: col_idx("solved_count"),
            solved_nunique: col_idx("solved_nunique"),
            solved_mean: col_idx("solved_mean"),
            solved_std: col_idx("solved_std"),
            solved_avg: col_idx("solved_avg"),
            solved_median: col_idx("solved_median"),
            solved_mode: col_idx("solved_mode"),
            solved_mode_count: col_idx("solved_mode_count"),
            solved_min: col_idx("solved_min"),
            solved_max: col_idx("solved_max"),
            solved_consecutive: col_idx("solved_consecutive"),
            solved_consecutive_start: col_idx("solved_consecutive_start"),
            solved_consecutive_end: col_idx("solved_consecutive_end"),
            solved_mo: [
                (3,   col_idx("solved_mo3_last"),   col_idx("solved_mo3_best")),
                (5,   col_idx("solved_mo5_last"),   col_idx("solved_mo5_best")),
                (12,  col_idx("solved_mo12_last"),  col_idx("solved_mo12_best")),
                (50,  col_idx("solved_mo50_last"),  col_idx("solved_mo50_best")),
                (100, col_idx("solved_mo100_last"), col_idx("solved_mo100_best")),
            ],
            solved_ao: [
                (5,   col_idx("solved_ao5_last"),   col_idx("solved_ao5_best")),
                (12,  col_idx("solved_ao12_last"),  col_idx("solved_ao12_best")),
                (50,  col_idx("solved_ao50_last"),  col_idx("solved_ao50_best")),
                (100, col_idx("solved_ao100_last"), col_idx("solved_ao100_best")),
            ],
            avg_item_3rd_min: col_idx("avg_item_3rd_min"),
            avg_item_3rd_max: col_idx("avg_item_3rd_max"),
            avg_item_2nd_min: col_idx("avg_item_2nd_min"),
            avg_item_2nd_max: col_idx("avg_item_2nd_max"),
        }
    }
}

static COL_IDX: std::sync::OnceLock<ColIdx> = std::sync::OnceLock::new();
fn ci() -> &'static ColIdx { COL_IDX.get_or_init(ColIdx::new) }

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

// Scratch buffers reused across compute_row calls within a single calc().
// Keeping capacity between persons avoids ~10 small Vec allocations per
// person and saves ~1–2s over a full run.
#[derive(Default)]
struct Scratch {
    bests: Vec<i64>,
    avgs_i: Vec<i64>,
    avgs_real: Vec<f64>,
    avgs_sorted: Vec<f64>,
    uniq: Vec<i64>,
    solved: Vec<i64>,
    worsts: Vec<i64>,
    medians: Vec<i64>,
    att_vs: Vec<i64>,
    // Only the attempt `value` field (or None for missing-attempt result rows)
    // is actually read downstream, so we can skip the richer SingleRow struct.
    single_values: Vec<Option<i32>>,
}

impl Scratch {
    fn clear_all(&mut self) {
        self.bests.clear();
        self.avgs_i.clear();
        self.avgs_real.clear();
        self.avgs_sorted.clear();
        self.uniq.clear();
        self.solved.clear();
        self.worsts.clear();
        self.medians.clear();
        self.att_vs.clear();
        self.single_values.clear();
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
    let mut scratch = Scratch::default();
    for pk in person_order {
        let p = &data.persons[pk as usize];
        let mut row = Row::new(
            pk,
            p.wca_id.clone(),
            p.name.clone(),
            p.country_id.clone(),
            p.gender.clone(),
        );
        compute_row(&mut row, data, pk, &by_person[&pk], &mut scratch);
        rows.push(row);
    }

    // Step 3: compute ranks.
    compute_ranks(&mut rows);

    Frame { rows, year_filter: yf }
}

fn compute_row(row: &mut Row, data: &WcaData, _person_key: u32, idxs: &[usize], sc: &mut Scratch) {
    let ci = ci();
    sc.clear_all();

    // --- Round-level stats ---
    let rs: Vec<&crate::loader::Result333> = idxs.iter().map(|&i| &data.results[i]).collect();

    // competitions (unique comp_keys)
    let mut comp_set: AHashSet<u32> = AHashSet::new();
    for r in &rs { comp_set.insert(r.comp_key); }
    row.vals[ci.competitions] = Cell::Int(comp_set.len() as i64);
    row.vals[ci.rounds] = Cell::Int(rs.len() as i64);

    // best stats on best > 0
    for r in &rs { if r.best > 0 { sc.bests.push(r.best as i64); } }
    if !sc.bests.is_empty() {
        let bests = &sc.bests;
        let (mn, mx) = extrema(bests);
        row.vals[ci.best] = Cell::Int(mn);
        row.vals[ci.best_max] = Cell::Int(mx);
        row.vals[ci.best_count] = Cell::Int(bests.len() as i64);
        sc.uniq.clear();
        sc.uniq.extend_from_slice(bests);
        sc.uniq.sort_unstable();
        sc.uniq.dedup();
        row.vals[ci.best_nunique] = Cell::Int(sc.uniq.len() as i64);
        row.vals[ci.best_mean] = Cell::Float(stats::mean_i(bests));
        row.vals[ci.best_std] = Cell::Float(stats::std_i(bests));
        if let Some(v) = stats::trim_avg_i(bests) {
            row.vals[ci.best_avg] = Cell::Float(v);
        }
        row.vals[ci.best_median] = Cell::Float(stats::median_f_from_i(bests));
        let (mode, mc) = stats::mode_count_i(bests);
        row.vals[ci.best_mode] = Cell::Int(mode);
        row.vals[ci.best_mode_count] = Cell::Int(mc);
        let (cc, cs, ce) = stats::calc_consecutive(bests, &[1]);
        row.vals[ci.best_consecutive] = Cell::Int(cc);
        row.vals[ci.best_consecutive_start] = Cell::Int(cs);
        row.vals[ci.best_consecutive_end] = Cell::Int(ce);
    }

    // average_attempts on average != 0
    let avg_attempts = rs.iter().filter(|r| r.average != 0).count();
    if avg_attempts > 0 {
        row.vals[ci.average_attempts] = Cell::Int(avg_attempts as i64);
    }

    // average stats on average > 0
    for r in &rs { if r.average > 0 { sc.avgs_i.push(r.average as i64); } }
    if !sc.avgs_i.is_empty() {
        sc.avgs_real.extend(sc.avgs_i.iter().map(|&v| v as f64 / 100.0));
        let avgs_i = &sc.avgs_i;
        let avgs_real = &sc.avgs_real;
        let (mn, mx) = extrema_f(avgs_real);
        row.vals[ci.average] = Cell::Float(mn);
        row.vals[ci.average_max] = Cell::Float(mx);
        row.vals[ci.average_count] = Cell::Int(avgs_real.len() as i64);
        sc.uniq.clear();
        sc.uniq.extend_from_slice(avgs_i);
        sc.uniq.sort_unstable();
        sc.uniq.dedup();
        row.vals[ci.average_nunique] = Cell::Int(sc.uniq.len() as i64);
        row.vals[ci.average_mean] = Cell::Float(stats::mean_f(avgs_real));
        row.vals[ci.average_std] = Cell::Float(stats::std_f(avgs_real));
        if let Some(v) = stats::trim_avg_f(avgs_real) {
            row.vals[ci.average_avg] = Cell::Float(v);
        }
        sc.avgs_sorted.extend_from_slice(avgs_real);
        sc.avgs_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sc.avgs_sorted.len();
        let med = if n % 2 == 1 { sc.avgs_sorted[n/2] } else { (sc.avgs_sorted[n/2-1]+sc.avgs_sorted[n/2])/2.0 };
        row.vals[ci.average_median] = Cell::Float(med);
        let (mode_i, mc) = stats::mode_count_i(avgs_i);
        row.vals[ci.average_mode] = Cell::Float(mode_i as f64 / 100.0);
        row.vals[ci.average_mode_count] = Cell::Int(mc);
        let (cc, cs, ce) = stats::calc_consecutive(avgs_i, &[33, 34]);
        row.vals[ci.average_consecutive] = Cell::Int(cc);
        row.vals[ci.average_consecutive_start] = Cell::Float(cs as f64 / 100.0);
        row.vals[ci.average_consecutive_end] = Cell::Float(ce as f64 / 100.0);
    }

    // medals: final rounds (f or c) with best > 0; count pos==1,2,3
    let (mut g, mut s, mut b) = (0i64, 0i64, 0i64);
    let mut any_final_best = false;
    for r in &rs {
        if r.best > 0 && (r.round_type_id == "f" || r.round_type_id == "c") {
            any_final_best = true;
            match r.pos { 1 => g+=1, 2 => s+=1, 3 => b+=1, _ => {} }
        }
    }
    if any_final_best {
        row.vals[ci.gold] = Cell::Int(g);
        row.vals[ci.silver] = Cell::Int(s);
        row.vals[ci.bronze] = Cell::Int(b);
    }

    // --- Single attempts ---
    // Sort rs in place by id for per-attempt iteration (attempts are already
    // sorted by attempt_number per result).
    let mut sorted_rs = rs;
    sorted_rs.sort_by_key(|r| r.id);

    // Build single_values: one Option<i32> per (result_id, attempt_number)
    // row — None represents a result with no attempts (leftjoin pad).
    for r in &sorted_rs {
        match data.attempts_by_result.get(&r.id) {
            Some(atts) if !atts.is_empty() => {
                for a in atts { sc.single_values.push(Some(a.value)); }
            }
            _ => { sc.single_values.push(None); }
        }
    }

    row.vals[ci.chances] = Cell::Int(sc.single_values.len() as i64);
    let attempts_count: i64 = sc.single_values.iter()
        .filter(|v| v.map(|v| v > -2).unwrap_or(false)).count() as i64;
    row.vals[ci.attempts] = Cell::Int(attempts_count);

    for v in &sc.single_values {
        if let Some(v) = v { if *v > 0 { sc.solved.push(*v as i64); } }
    }

    if !sc.solved.is_empty() {
        let solved = &sc.solved;
        row.vals[ci.solved_count] = Cell::Int(solved.len() as i64);
        sc.uniq.clear();
        sc.uniq.extend_from_slice(solved);
        sc.uniq.sort_unstable();
        sc.uniq.dedup();
        row.vals[ci.solved_nunique] = Cell::Int(sc.uniq.len() as i64);
        row.vals[ci.solved_mean] = Cell::Float(stats::mean_i(solved));
        row.vals[ci.solved_std] = Cell::Float(stats::std_i(solved));
        if let Some(v) = stats::trim_avg_i(solved) {
            row.vals[ci.solved_avg] = Cell::Float(v);
        }
        row.vals[ci.solved_median] = Cell::Float(stats::median_f_from_i(solved));
        let (mode, mc) = stats::mode_count_i(solved);
        row.vals[ci.solved_mode] = Cell::Int(mode);
        row.vals[ci.solved_mode_count] = Cell::Int(mc);
        let (mn, mx) = extrema(solved);
        row.vals[ci.solved_min] = Cell::Int(mn);
        row.vals[ci.solved_max] = Cell::Int(mx);
        let (cc, cs, ce) = stats::calc_consecutive(solved, &[1]);
        row.vals[ci.solved_consecutive] = Cell::Int(cc);
        row.vals[ci.solved_consecutive_start] = Cell::Int(cs);
        row.vals[ci.solved_consecutive_end] = Cell::Int(ce);

        for &(n, last_i, best_i) in &ci.solved_mo {
            if let Some((last, best)) = stats::rolling_mean(solved, n) {
                row.vals[last_i] = Cell::Float(last);
                row.vals[best_i] = Cell::Float(best);
            }
        }
        for &(n, last_i, best_i) in &ci.solved_ao {
            if let Some((last, best)) = stats::rolling_trim_avg(solved, n) {
                row.vals[last_i] = Cell::Float(last);
                row.vals[best_i] = Cell::Float(best);
            }
        }
    }

    // --- avg_item_3rd/2nd ---
    for r in &sorted_rs {
        if r.average <= 0 { continue; }
        if let Some(atts) = data.attempts_by_result.get(&r.id) {
            if atts.is_empty() { continue; }
            sc.att_vs.clear();
            for a in atts { sc.att_vs.push(a.value as i64); }
            sc.worsts.push(*sc.att_vs.iter().max().unwrap());
            sc.medians.push(stats::median_i(&sc.att_vs));
        }
    }
    if !sc.worsts.is_empty() {
        let (mn, mx) = extrema(&sc.worsts);
        row.vals[ci.avg_item_3rd_min] = Cell::Int(mn);
        row.vals[ci.avg_item_3rd_max] = Cell::Int(mx);
        let (mn, mx) = extrema(&sc.medians);
        row.vals[ci.avg_item_2nd_min] = Cell::Int(mn);
        row.vals[ci.avg_item_2nd_max] = Cell::Int(mx);
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
