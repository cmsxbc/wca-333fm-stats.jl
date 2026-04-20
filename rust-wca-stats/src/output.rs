// CSV writing, top-k/person/summary helpers.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::calc::{Cell, ColKind, Frame, Row, COLS, col_idx, rank_col_order};

pub type OutRow = Row;

pub fn write_csv(path: &Path, frame: &Frame) -> std::io::Result<()> {
    let f = File::create(path)?;
    let mut w = BufWriter::with_capacity(1 << 20, f);

    // Header
    w.write_all(b"personId,personName,countryId,gender")?;
    for (n, _, _) in COLS { write!(w, ",{}", n)?; }
    let rank_order = rank_col_order();
    for &i in &rank_order { write!(w, ",{}_rank", COLS[i].0)?; }
    for &i in &rank_order { write!(w, ",{}_nr", COLS[i].0)?; }
    w.write_all(b"\n")?;

    let mut fbuf = ryu::Buffer::new();
    let mut ibuf = itoa::Buffer::new();

    for row in &frame.rows {
        write_str_csv(&mut w, &row.person_id)?;
        w.write_all(b",")?;
        write_str_csv(&mut w, &row.person_name)?;
        w.write_all(b",")?;
        write_str_csv(&mut w, &row.country_id)?;
        w.write_all(b",")?;
        write_str_csv(&mut w, &row.gender)?;

        // Value cells
        for (i, (_n, kind, _)) in COLS.iter().enumerate() {
            w.write_all(b",")?;
            write_cell(&mut w, &row.vals[i], *kind, &mut fbuf, &mut ibuf)?;
        }
        // Rank cells (always integer or missing), asc-then-desc order
        for &i in &rank_order {
            w.write_all(b",")?;
            write_cell(&mut w, &row.ranks[i], ColKind::Int, &mut fbuf, &mut ibuf)?;
        }
        // NR cells
        for &i in &rank_order {
            w.write_all(b",")?;
            write_cell(&mut w, &row.nrs[i], ColKind::Int, &mut fbuf, &mut ibuf)?;
        }
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

fn write_cell<W: Write>(
    w: &mut W,
    c: &Cell,
    kind: ColKind,
    fbuf: &mut ryu::Buffer,
    ibuf: &mut itoa::Buffer,
) -> std::io::Result<()> {
    match c {
        Cell::Missing => Ok(()),
        Cell::Int(v) => {
            w.write_all(ibuf.format(*v).as_bytes())
        }
        Cell::Float(v) => {
            if v.is_nan() {
                w.write_all(b"NaN")
            } else if v.is_infinite() {
                w.write_all(if *v > 0.0 { b"Inf" } else { b"-Inf" })
            } else {
                // For columns declared as Int, we should never arrive here
                // with a Float. Format shortest-roundtrip like Julia's show.
                let s = fbuf.format(*v);
                // ryu produces "1.0" for 1.0, "0.3333333333333333" for 1/3,
                // and e-notation only for extreme values. Julia's Base.show
                // agrees for typical values but uses e.g. "1.0e8" for 1e8
                // while ryu uses "1e8". Our values are small so ok.
                let _ = kind; // kind unused
                w.write_all(s.as_bytes())
            }
        }
    }
}

fn write_str_csv<W: Write>(w: &mut W, s: &str) -> std::io::Result<()> {
    // Julia's CSV.write quotes strings containing comma, quote, newline.
    let needs_quote = s.contains(',') || s.contains('"') || s.contains('\n');
    if needs_quote {
        w.write_all(b"\"")?;
        for ch in s.chars() {
            if ch == '"' { w.write_all(b"\"\"")?; } else { write!(w, "{}", ch)?; }
        }
        w.write_all(b"\"")?;
    } else {
        w.write_all(s.as_bytes())?;
    }
    Ok(())
}

pub fn print_some_persons(frame: &Frame, ids: &[String]) {
    use std::collections::HashSet;
    let ids: HashSet<&str> = ids.iter().map(|s| s.as_str()).collect();
    let rows: Vec<&Row> = frame.rows.iter().filter(|r| ids.contains(r.person_id.as_str())).collect();
    if rows.is_empty() { return; }
    let name_lens: Vec<usize> = rows.iter().map(|r| r.person_name.chars().count().max(1)).collect();

    // Column name column width
    let mut col_names: Vec<String> = vec![
        "personId".into(), "personName".into(), "countryId".into(), "gender".into(),
    ];
    for (n, _, _) in COLS { col_names.push((*n).to_string()); }
    for (n, _, _) in COLS { col_names.push(format!("{}_rank", n)); }
    for (n, _, _) in COLS { col_names.push(format!("{}_nr", n)); }
    let col_name_len: usize = col_names.iter().map(|s| s.chars().count()).max().unwrap_or(0);

    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());

    // Print person meta rows.
    for &(name, getter) in &[
        ("personId", 0u8),
        ("personName", 1),
        ("countryId", 2),
        ("gender", 3),
    ] {
        write!(out, "{:>width$}", name, width = col_name_len).unwrap();
        for (i, r) in rows.iter().enumerate() {
            let v: &str = match getter {
                0 => &r.person_id, 1 => &r.person_name,
                2 => &r.country_id, _ => &r.gender,
            };
            write!(out, "    {:>w$}", v, w = name_lens[i]).unwrap();
        }
        writeln!(out).unwrap();
    }

    // Print value rows
    let write_numeric =
        |out: &mut std::io::BufWriter<std::io::StdoutLock>, name: &str, cells: Vec<&Cell>| {
            write!(out, "{:>width$}", name, width = col_name_len).unwrap();
            for (i, cell) in cells.iter().enumerate() {
                match cell {
                    Cell::Missing => { write!(out, "    {:>w$}", "", w = name_lens[i]).unwrap(); }
                    Cell::Int(v) => { write!(out, "    {:>w$}", v, w = name_lens[i]).unwrap(); }
                    Cell::Float(v) => { write!(out, "    {:>w$.2}", v, w = name_lens[i]).unwrap(); }
                }
            }
            writeln!(out).unwrap();
        };

    for (i, (n, _, _)) in COLS.iter().enumerate() {
        let cells: Vec<&Cell> = rows.iter().map(|r| &r.vals[i]).collect();
        write_numeric(&mut out, n, cells);
    }
    for (i, (n, _, _)) in COLS.iter().enumerate() {
        let cells: Vec<&Cell> = rows.iter().map(|r| &r.ranks[i]).collect();
        write_numeric(&mut out, &format!("{}_rank", n), cells);
    }
    for (i, (n, _, _)) in COLS.iter().enumerate() {
        let cells: Vec<&Cell> = rows.iter().map(|r| &r.nrs[i]).collect();
        write_numeric(&mut out, &format!("{}_nr", n), cells);
    }
}

// (legacy helper removed)

pub fn print_topk(frame: &Frame, col: &str, k: usize, country: Option<&str>) {
    let ci = col_idx(col);
    let (use_nr, filtered): (bool, Vec<&Row>) = match country {
        Some(c) => (true, frame.rows.iter().filter(|r| r.country_id == c).collect()),
        None => (false, frame.rows.iter().collect()),
    };
    let mut withrank: Vec<(i64, &Row)> = filtered
        .into_iter()
        .filter_map(|r| {
            let cell = if use_nr { &r.nrs[ci] } else { &r.ranks[ci] };
            match cell {
                Cell::Int(v) if (*v as usize) <= k => Some((*v, r)),
                _ => None,
            }
        })
        .collect();
    withrank.sort_by_key(|(r, _)| *r);

    // Columns: personName, countryId, col, col_nr, col_rank
    println!("{:>20} {:>20} {:>20} {:>10} {:>10}",
        "personName", "countryId", col,
        format!("{}_nr", col), format!("{}_rank", col));
    for (_rank, r) in withrank {
        let vstr = match &r.vals[ci] {
            Cell::Missing => "".to_string(),
            Cell::Int(v) => v.to_string(),
            Cell::Float(v) => ryu::Buffer::new().format(*v).to_string(),
        };
        let nrs = match &r.nrs[ci] { Cell::Int(v) => v.to_string(), _ => "".into() };
        let rks = match &r.ranks[ci] { Cell::Int(v) => v.to_string(), _ => "".into() };
        println!("{:>20} {:>20} {:>20} {:>10} {:>10}",
            r.person_name, r.country_id, vstr, nrs, rks);
    }
}

/// Summary support: for the summary command we accumulate per-year rows
/// plus delta rows. These are written to a separate CSV.
pub fn write_summary_csv(
    path: &Path,
    _header: &[String],
    rows: &[Row],
) -> std::io::Result<()> {
    let f = File::create(path)?;
    let mut w = BufWriter::with_capacity(1 << 16, f);

    w.write_all(b"personId,personName,countryId,gender")?;
    for (n, _, _) in COLS { write!(w, ",{}", n)?; }
    let rank_order = rank_col_order();
    for &i in &rank_order { write!(w, ",{}_rank", COLS[i].0)?; }
    for &i in &rank_order { write!(w, ",{}_nr", COLS[i].0)?; }
    w.write_all(b",year,category\n")?;

    let mut fbuf = ryu::Buffer::new();
    let mut ibuf = itoa::Buffer::new();
    for row in rows {
        write_str_csv(&mut w, &row.person_id)?;
        w.write_all(b",")?;
        write_str_csv(&mut w, &row.person_name)?;
        w.write_all(b",")?;
        write_str_csv(&mut w, &row.country_id)?;
        w.write_all(b",")?;
        write_str_csv(&mut w, &row.gender)?;
        for (i, (_, kind, _)) in COLS.iter().enumerate() {
            w.write_all(b",")?;
            write_cell(&mut w, &row.vals[i], *kind, &mut fbuf, &mut ibuf)?;
        }
        for &i in &rank_order {
            w.write_all(b",")?;
            write_cell(&mut w, &row.ranks[i], ColKind::Int, &mut fbuf, &mut ibuf)?;
        }
        for &i in &rank_order {
            w.write_all(b",")?;
            write_cell(&mut w, &row.nrs[i], ColKind::Int, &mut fbuf, &mut ibuf)?;
        }
        write!(w, ",{},{}", row.year_cell_str(), row.category_cell_str())?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

// Minimal set_meta / delta support on Row via extension trait in calc.rs.
// (We add the fields there and the helpers below.)
