// Rust port of WCAStats.jl. Computes per-person 3x3 Fewest Moves statistics
// from a WCA export zip and writes matching results.in<Y>.csv / results.to<Y>.csv.

use ahash::AHashMap;
use clap::{Parser, Subcommand};
use std::fs;
use std::path::{Path, PathBuf};

mod loader;
mod stats;
mod calc;
mod output;

#[derive(Parser, Debug)]
#[command(name = "wca-stats")]
struct Cli {
    /// Path to WCA export zip (name must contain "WCA_export_v2_")
    source: PathBuf,

    /// Restrict to single year
    #[arg(long)]
    year: Option<i32>,

    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Print specific persons from the latest year's results.
    #[command(visible_alias = "P")]
    Person {
        /// WCA person ids
        ids: Vec<String>,
    },
    /// Print top-K by a column from the latest year's results.
    #[command(visible_alias = "K")]
    Topk {
        /// Column name (without _rank/_nr suffix)
        col: String,
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Restrict to country (use national rank instead of world rank)
        #[arg(long)]
        country: Option<String>,
    },
    /// Summarize one person across all years.
    #[command(visible_alias = "S")]
    Summary {
        /// WCA person id
        id: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let src = cli.source.to_string_lossy().to_string();
    if !src.contains("WCA_export_v2_") {
        eprintln!("cannot load data");
        return Ok(());
    }
    let t_load = std::time::Instant::now();
    let data = loader::load_wca(&cli.source)?;
    eprintln!("Load Data done ({:.2}s)", t_load.elapsed().as_secs_f64());

    let result_dir = Path::new("results");
    fs::create_dir_all(result_dir)?;

    let event_id = data.event_id("333fm").expect("333fm event missing in data");

    let is_summary = matches!(cli.command, Some(Cmd::Summary { .. }));
    let summary_person_id: Option<u32> = match &cli.command {
        Some(Cmd::Summary { id }) => Some(data.person_key(id).unwrap_or_else(|| {
            panic!("unknown person id {}", id)
        })),
        _ => None,
    };

    let mut years: Vec<i32> = if is_summary {
        let pid = summary_person_id.unwrap();
        data.person_event_years(pid, event_id)
    } else {
        data.event_years(event_id)
    };
    years.sort();
    if let Some(y) = cli.year {
        if !is_summary {
            years.retain(|yy| *yy == y);
        }
    }

    // Summary bookkeeping
    let mut summary_rows: Vec<calc::Row> = Vec::new();
    let mut last_rows: AHashMap<&'static str, calc::Row> = AHashMap::new();
    let mut summary_header: Option<Vec<String>> = None;

    let n_years = years.len();
    for (yi, &year) in years.iter().enumerate() {
        eprintln!("dealing {} ...", year);
        for (category, filter) in
            [("in", calc::YearFilter::Eq(year)), ("to", calc::YearFilter::Le(year))]
        {
            let df = calc::calc(&data, event_id, filter);
            let fname = result_dir.join(format!("results.{}{}.csv", category, year));
            let t_io = std::time::Instant::now();
            output::write_csv(&fname, &df)?;
            eprintln!(
                "saved: {} ({:.2}s)",
                fname.display(),
                t_io.elapsed().as_secs_f64()
            );

            if is_summary {
                let pid = summary_person_id.unwrap();
                let mut row = df.row_for_person(pid).unwrap_or_else(|| df.empty_row(pid));
                // Populate meta fields (Julia's behavior carries name/country/gender through).
                let p = &data.persons[pid as usize];
                row.person_id = p.wca_id.clone();
                row.person_name = p.name.clone();
                row.country_id = p.country_id.clone();
                row.gender = p.gender.clone();
                row.year = Some(year as i64);
                row.category = Some(format!("{}-year", category));
                if summary_header.is_none() {
                    summary_header = Some(df.header_for_summary());
                }
                summary_rows.push(row.clone());
                if let Some(last) = last_rows.get(category) {
                    let mut delta = row.delta(last, &df.person_meta_cols());
                    delta.year = Some(year as i64);
                    delta.category = Some(format!("{}-year-detla", category));
                    summary_rows.push(delta);
                }
                last_rows.insert(category, row);
            }

            let is_last_year = yi + 1 == n_years;
            if is_last_year && category == "to" {
                match &cli.command {
                    Some(Cmd::Topk { col, k, country }) => {
                        output::print_topk(&df, col, *k, country.as_deref());
                    }
                    Some(Cmd::Person { ids }) => {
                        output::print_some_persons(&df, ids);
                    }
                    _ => {}
                }
            }
        }
    }

    if is_summary {
        let pid_str = match &cli.command {
            Some(Cmd::Summary { id }) => id.clone(),
            _ => unreachable!(),
        };
        let header = summary_header.unwrap();
        let path = result_dir.join(format!("{}.csv", pid_str));
        output::write_summary_csv(&path, &header, &summary_rows)?;
    }

    Ok(())
}
