use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, RwLock};

use rayon::prelude::*;
use warp::Filter;

use crate::filters::View;
use crate::process;
use crate::readers;

type RecordCache = Arc<RwLock<HashMap<String, Arc<CachedDataSet>>>>;

#[derive(Clone)]
struct CachedDataSet {
    records: Vec<crate::Record>,
    iso_dates: Vec<Option<String>>,
    empty_ts: usize,
}

#[derive(Clone)]
struct AppState {
    default_file: Option<String>,
    cache: RecordCache,
}

fn load_records(path: &str) -> Result<CachedDataSet, warp::Rejection> {
    let reader = readers::detect_reader(path).map_err(|_| warp::reject::not_found())?;
    let view = view_for_file(path);
    let mut records: Vec<crate::Record> = process(reader, view).filter_map(|r| r.ok()).collect();
    records.reverse();
    let mut with_ts: Vec<(crate::Record, Option<String>)> = Vec::with_capacity(records.len());
    let mut empty_ts = 0usize;
    for rec in records {
        let ds = rec.variables.get("timestamp").or_else(|| rec.variables.get("time")).cloned().unwrap_or_default();
        if ds.is_empty() {
            empty_ts += 1;
            with_ts.push((rec, None));
        } else {
            with_ts.push((rec, date_str_to_iso(&ds)));
        }
    }
    // Partition: timestamped records first, then empty-timestamp records
    let mut i = 0;
    for j in 0..with_ts.len() {
        if with_ts[j].1.is_some() {
            if i != j {
                with_ts.swap(i, j);
            }
            i += 1;
        }
    }
    let iso_dates: Vec<Option<String>> = with_ts.iter().map(|(_, iso)| iso.clone()).collect();
    let records: Vec<crate::Record> = with_ts.into_iter().map(|(r, _)| r).collect();
    Ok(CachedDataSet { records, iso_dates, empty_ts })
}

fn cached_records(state: &AppState, path: &str, refresh: bool) -> Result<Arc<CachedDataSet>, warp::Rejection> {
    if refresh {
        state.cache.write().unwrap().remove(path);
    } else if let Some(data) = state.cache.read().unwrap().get(path) {
        return Ok(Arc::clone(data));
    }
    let data = Arc::new(load_records(path)?);
    state.cache.write().unwrap().insert(path.to_owned(), Arc::clone(&data));
    Ok(data)
}

fn view_for_file(path: &str) -> View {
    let fname = std::path::Path::new(path)
        .file_name().map(|s| s.to_string_lossy()).unwrap_or_default();
    let view_file = if fname.to_lowercase().contains("reu") {
        "view_reu.json"
    } else if fname.ends_with(".OUT") || fname.ends_with(".out") {
        "view_core.json"
    } else {
        ""
    };
    if view_file.is_empty() {
        return View { operations: vec![] };
    }
    // Try: next to the log file
    let view_path = std::path::Path::new(path)
        .parent().unwrap_or(std::path::Path::new("."))
        .join(view_file);
    if let Ok(file) = File::open(&view_path) {
        if let Ok(v) = serde_json::from_reader(file) {
            return v;
        }
    }
    // Try: relative to the executable
    if let Ok(exe) = std::env::current_exe() {
        let exe_dir = exe.parent().unwrap_or(std::path::Path::new("."));
        let view_path = exe_dir.join(view_file);
        if let Ok(file) = File::open(&view_path) {
            if let Ok(v) = serde_json::from_reader(file) { return v; }
        }
        // Try: Resources/ next to the executable (macOS .app bundle)
        let view_path = exe_dir.join("../Resources").join(view_file);
        if let Ok(file) = File::open(&view_path) {
            if let Ok(v) = serde_json::from_reader(file) { return v; }
        }
    }
    // Try: current working directory
    if let Ok(file) = File::open(view_file) {
        if let Ok(v) = serde_json::from_reader(file) { return v; }
    }
    View { operations: vec![] }
}

pub async fn serve(
    host: std::net::IpAddr,
    port: u16,
    default_file: Option<String>,
) {
    let state = Arc::new(AppState {
        default_file,
        cache: Arc::new(RwLock::new(HashMap::new())),
    });

    let state_filter = warp::any().map(move || state.clone());

    let frontend = warp::path::end()
        .and(warp::get())
        .and(state_filter.clone())
        .map(|_state: Arc<AppState>| {
            warp::reply::html(FRONTEND_HTML)
        });

    let query = warp::path("api")
        .and(warp::path("query"))
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query::<HashMap<String, String>>())
        .and(state_filter.clone())
        .and_then(handle_query)
        .boxed();

    let upload = warp::path("api")
        .and(warp::path("upload"))
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::query::<HashMap<String, String>>())
        .and(warp::body::content_length_limit(1024 * 1024 * 1024))
        .and(warp::body::bytes())
        .and(state_filter)
        .and_then(handle_upload)
        .boxed();

    let routes = frontend.or(query).or(upload);

    eprintln!("Starting server on {}:{}", host, port);
    warp::serve(routes).run((host, port)).await;
}

async fn handle_upload(
    params: HashMap<String, String>,
    body: bytes::Bytes,
    _state: Arc<AppState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let name = params.get("name").cloned().unwrap_or_else(|| "upload.log".to_string());
    let temp_dir = std::env::temp_dir().join("logviewer");
    std::fs::create_dir_all(&temp_dir).ok();
    let file_path = temp_dir.join(format!("{}_{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos(), name));
    if std::fs::write(&file_path, &body[..]).is_err() {
        return Err(warp::reject::not_found());
    }
    let fp = file_path.clone();
    tokio::spawn(async move {
        tokio::time::delay_for(std::time::Duration::from_secs(3600)).await;
        std::fs::remove_file(fp).ok();
    });
    Ok(warp::reply::json(&serde_json::json!({
        "path": file_path.to_string_lossy(),
    })))
}

fn detect_op_class(msg: &str) -> &'static str {
    let first = msg.trim_start().split(|c: char| c.is_whitespace() || c == '(').next().unwrap_or("");
    match first.to_uppercase().as_str() {
        "SELECT" | "WITH" => "select",
        "INSERT" | "MERGE" => "insert",
        "UPDATE" => "update",
        "DELETE" | "DROP" | "TRUNCATE" => "delete",
        "CREATE" | "ALTER" | "BEGIN" => "create",
        "EXEC" | "EXECUTE" | "CALL" | "DECLARE" => "exec",
        "COMMIT" => "commit",
        _ => "",
    }
}

#[allow(non_snake_case)]
async fn handle_query(
    params: HashMap<String, String>,
    state: Arc<AppState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let path = params.get("file").cloned().or_else(|| state.default_file.clone());
    let path = match path { Some(p) => p, None => { return Ok(warp::reply::json(&serde_json::json!({"r": [], "t": 0}))); } };
    let refresh = params.get("refresh").map(|v| v == "true").unwrap_or(false);
    let data = cached_records(&state, &path, refresh)?;
    let all_records = &data.records;
    let iso_dates = &data.iso_dates;
    let empty_ts = data.empty_ts;
    let skip: usize = params.get("skip").and_then(|v| v.parse().ok()).unwrap_or(0);
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(5000);
    let f_level = params.get("level").map(String::as_str).unwrap_or("");
    let f_comp = params.get("comp").map(String::as_str).unwrap_or("");
    let f_proc = params.get("proc").map(String::as_str).unwrap_or("");
    let f_thread = params.get("thread").map(String::as_str).unwrap_or("");
    let f_user = params.get("user").map(String::as_str).unwrap_or("");
    let f_msg = params.get("msg").map(String::as_str).unwrap_or("");
    let f_source = params.get("source").map(String::as_str).unwrap_or("");
    let f_line = params.get("line").map(String::as_str).unwrap_or("");
    let f_sourceC = params.get("sourceC").map(String::as_str).unwrap_or("");
    let f_lineC = params.get("lineC").map(String::as_str).unwrap_or("");
    let f_dateFrom = params.get("dateFrom").map(String::as_str).unwrap_or("");
    let f_dateTo = params.get("dateTo").map(String::as_str).unwrap_or("");
    let f_date_to_adj: String;
    let f_date_to_ref = if !f_dateTo.is_empty() && !f_dateTo.contains(':') {
        f_date_to_adj = format!("{}T23:59:59", f_dateTo);
        f_date_to_adj.as_str()
    } else if !f_dateTo.is_empty() && f_dateTo.len() <= 16 {
        f_date_to_adj = format!("{}:59", f_dateTo);
        f_date_to_adj.as_str()
    } else {
        f_dateTo
    };
    let f_hideTriggers = params.get("hideTriggers").map(|v| v == "true").unwrap_or(false);
    let search_q = params.get("q").map(String::as_str).unwrap_or("");

    // Pre-compute dateFrom boundary for early break (records newest-first)
    let has_date_from = !f_dateFrom.is_empty();
    let has_date_to = !f_dateTo.is_empty();
    let main_end = all_records.len() - empty_ts;
    let date_break = if has_date_from {
        let mut e = main_end;
        for i in 0..main_end {
            if let Some(ref iso) = iso_dates[i] {
                if iso.as_str() < f_dateFrom { e = i; break; }
            }
        }
        e
    } else {
        main_end
    };

    let matching: Vec<usize> = all_records[..date_break].par_iter()
        .enumerate()
        .filter(|(i, rec)| {
            let level = rec.variables.get("level").map_or("", |s| s);
            let comp = rec.variables.get("component").map_or("", |s| s);
            let proc = rec.variables.get("proc").map_or("", |s| s);
            let thread = rec.variables.get("thread").map_or("", |s| s);
            let user = rec.variables.get("user").map_or("", |s| s);
            let msg = rec.variables.get("message").map_or(&rec.text, |s| s);
            let source = rec.variables.get("source").map_or("", |s| s);
            let line = rec.variables.get("line").map_or("", |s| s);
            let sourceC = rec.variables.get("sourceC").map_or("", |s| s);
            let lineC = rec.variables.get("lineC").map_or("", |s| s);

            if !f_level.is_empty() && !level.to_lowercase().contains(&f_level.to_lowercase()) { return false; }
            if !f_comp.is_empty() && !comp.to_lowercase().contains(&f_comp.to_lowercase()) { return false; }
            if !f_proc.is_empty() && !proc.to_lowercase().contains(&f_proc.to_lowercase()) { return false; }
            if !f_thread.is_empty() && !thread.to_lowercase().contains(&f_thread.to_lowercase()) { return false; }
            if !f_user.is_empty() && user.to_lowercase() != f_user.to_lowercase() { return false; }
            if !f_msg.is_empty() && !msg.to_lowercase().contains(&f_msg.to_lowercase()) { return false; }
            if !f_source.is_empty() && !source.to_lowercase().contains(&f_source.to_lowercase()) { return false; }
            if !f_line.is_empty() && !line.to_lowercase().contains(&f_line.to_lowercase()) { return false; }
            if !f_sourceC.is_empty() && !sourceC.to_lowercase().contains(&f_sourceC.to_lowercase()) { return false; }
            if !f_lineC.is_empty() && !lineC.to_lowercase().contains(&f_lineC.to_lowercase()) { return false; }
            if f_hideTriggers && comp.to_uppercase().starts_with("TRIGGER") { return false; }
            if has_date_to && *i < main_end {
                if let Some(ref iso) = iso_dates[*i] {
                    if iso.as_str() > f_date_to_ref { return false; }
                }
            }
            if !search_q.is_empty() {
                let haystack = format!("{} {} {}", rec.text, msg, comp).to_lowercase();
                if !search_q.split_whitespace().all(|w| haystack.contains(w)) { return false; }
            }
            true
        })
        .map(|(i, _)| i)
        .chain(
            // Also include empty-timestamp records (non-date filters only)
            (main_end..all_records.len()).into_par_iter().filter(|&i| {
                let rec = &all_records[i];
                let level = rec.variables.get("level").map_or("", |s| s);
                let comp = rec.variables.get("component").map_or("", |s| s);
                let proc = rec.variables.get("proc").map_or("", |s| s);
                let thread = rec.variables.get("thread").map_or("", |s| s);
                let user = rec.variables.get("user").map_or("", |s| s);
                let msg = rec.variables.get("message").map_or(&rec.text, |s| s);
                let source = rec.variables.get("source").map_or("", |s| s);
                let line = rec.variables.get("line").map_or("", |s| s);
                let sourceC = rec.variables.get("sourceC").map_or("", |s| s);
                let lineC = rec.variables.get("lineC").map_or("", |s| s);
                if !f_level.is_empty() && !level.to_lowercase().contains(&f_level.to_lowercase()) { return false; }
                if !f_comp.is_empty() && !comp.to_lowercase().contains(&f_comp.to_lowercase()) { return false; }
                if !f_proc.is_empty() && !proc.to_lowercase().contains(&f_proc.to_lowercase()) { return false; }
                if !f_thread.is_empty() && !thread.to_lowercase().contains(&f_thread.to_lowercase()) { return false; }
                if !f_user.is_empty() && user.to_lowercase() != f_user.to_lowercase() { return false; }
                if !f_msg.is_empty() && !msg.to_lowercase().contains(&f_msg.to_lowercase()) { return false; }
                if !f_source.is_empty() && !source.to_lowercase().contains(&f_source.to_lowercase()) { return false; }
                if !f_line.is_empty() && !line.to_lowercase().contains(&f_line.to_lowercase()) { return false; }
                if !f_sourceC.is_empty() && !sourceC.to_lowercase().contains(&f_sourceC.to_lowercase()) { return false; }
                if !f_lineC.is_empty() && !lineC.to_lowercase().contains(&f_lineC.to_lowercase()) { return false; }
                if f_hideTriggers && comp.to_uppercase().starts_with("TRIGGER") { return false; }
                if !search_q.is_empty() {
                    let haystack = format!("{} {} {}", rec.text, msg, comp).to_lowercase();
                    if !search_q.split_whitespace().all(|w| haystack.contains(w)) { return false; }
                }
                true
            })
        )
        .collect();

    let total = matching.len();
    let out_records: Vec<serde_json::Value> = matching.iter()
        .skip(skip)
        .take(limit)
        .map(|&i| {
            let rec = &all_records[i];
            let comp = rec.variables.get("component").map_or("", |s| s);
            let msg = rec.variables.get("message").map_or(&rec.text, |s| s);
            let is_trigger = comp.to_uppercase().starts_with("TRIGGER");
            let op_class = detect_op_class(msg);
            serde_json::json!({"v": rec.variables, "c": rec.color, "op": op_class, "tr": is_trigger})
        })
        .collect();

    Ok(warp::reply::json(&serde_json::json!({
        "r": out_records,
        "t": total,
    })))
}

fn date_str_to_iso(s: &str) -> Option<String> {
    let parts: Vec<&str> = s.splitn(2, ' ').collect();
    let date_part = parts[0];
    let time_part = parts.get(1).map(|t| t.split(',').next().unwrap_or(t)).unwrap_or("00:00:00");
    let dp: Vec<&str> = date_part.split('/').collect();
    if dp.len() != 3 { return None; }
    let v0 = dp[0].parse::<u32>().ok()?;
    let v1 = dp[1].parse::<u32>().ok()?;
    let v2 = dp[2].parse::<u32>().ok()?;
    // CORE.OUT: dd/MM/yy HH:mm:ss,fff  (has comma in original time)
    // reu.out:  yy/MM/dd HH:mm:ss      (no comma)
    let (d, m, y) = if s.contains(',') {
        (v0, v1, v2)
    } else {
        (v2, v1, v0)
    };
    let y = if y < 100 { y + 2000 } else { y };
    Some(format!("{:04}-{:02}-{:02}T{}", y, m, d, time_part))
}

const FRONTEND_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LogViewer</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:ital,wght@0,400;0,500;0,600;1,400&family=Plus+Jakarta+Sans:wght@500;600;700&display=swap" rel="stylesheet">
<style>
  :root, [data-theme="dark"] {
    --bg-root: #080c10;
    --bg-surface: #0e1419;
    --bg-elevated: #151e26;
    --bg-hover: #1a2530;
    --border-subtle: #1a2632;
    --border: #24323f;
    --text-primary: #e2e8f0;
    --text-secondary: #94a3b8;
    --text-muted: #475569;
    --accent: #f59e0b;
    --accent-glow: rgba(245, 158, 11, 0.1);
    --scrollbar-bg: #0e1419;
    --scrollbar-thumb: #1e2d3d;
    --scrollbar-hover: #2d4055;
    --theme-icon: '\263E';
    --btn-bg: var(--bg-elevated);
  }
  [data-theme="light"] {
    --bg-root: #f5f7fa;
    --bg-surface: #ffffff;
    --bg-elevated: #edf0f5;
    --bg-hover: #e2e6ed;
    --border-subtle: #d1d6de;
    --border: #c4cad4;
    --text-primary: #1e293b;
    --text-secondary: #475569;
    --text-muted: #94a3b8;
    --accent: #d97706;
    --accent-glow: rgba(217, 119, 6, 0.1);
    --scrollbar-bg: #e2e6ed;
    --scrollbar-thumb: #c4cad4;
    --scrollbar-hover: #a8b0c0;
    --theme-icon: '\2600';
    --btn-bg: #ffffff;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    background: var(--bg-root);
    color: var(--text-primary);
    font-family: 'JetBrains Mono', 'Fira Code', ui-monospace, monospace;
    font-size: 12.5px;
    line-height: 1.5;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    -webkit-font-smoothing: antialiased;
  }
  ::selection { background: var(--accent-glow); color: var(--accent); }
  ::-webkit-scrollbar { width: 8px; height: 8px; }
  ::-webkit-scrollbar-track { background: var(--scrollbar-bg); }
  ::-webkit-scrollbar-thumb { background: var(--scrollbar-thumb); border-radius: 4px; border: 2px solid var(--scrollbar-bg); }
  ::-webkit-scrollbar-thumb:hover { background: var(--scrollbar-hover); }

  header {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 20px;
    background: var(--bg-surface);
    border-bottom: 1px solid var(--border-subtle);
    flex-shrink: 0;
    user-select: none;
    flex-wrap: wrap;
    position: relative;
    z-index: 10;
  }
  .brand {
    display: flex;
    align-items: center;
    gap: 9px;
    white-space: nowrap;
  }
  .brand-icon {
    width: 20px; height: 20px;
    border-radius: 4px;
    background: linear-gradient(135deg, var(--accent), #d97706);
    display: flex;
    align-items: center; justify-content: center;
    font-size: 11px; font-weight: 700; color: #000;
    flex-shrink: 0;
  }
  h1 {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 13px; font-weight: 700;
    color: var(--text-primary); letter-spacing: 0.3px;
  }
  .search-wrap {
    flex: 1; position: relative; max-width: 360px;
  }
  .search-wrap svg {
    position: absolute; left: 9px; top: 50%;
    transform: translateY(-50%);
    width: 12px; height: 12px; color: var(--text-muted);
    pointer-events: none;
  }
  .search-wrap input {
    width: 100%;
    padding: 5px 9px 5px 28px;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 5px;
    color: var(--text-primary);
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; outline: none;
    transition: border-color 0.15s, box-shadow 0.15s;
  }
  .search-wrap input:focus { border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-glow); }
  .search-wrap input::placeholder { color: var(--text-muted); }
  .upload-btn {
    position: relative;
    flex-shrink: 0;
  }
  .upload-btn input[type="file"] { display: none; }
  .upload-btn .upload-label {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 4px 10px;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 5px;
    color: var(--text-muted);
    font-size: 11px;
    cursor: pointer;
    transition: border-color 0.15s, color 0.15s;
    white-space: nowrap;
    font-family: 'JetBrains Mono', monospace;
  }
  .upload-btn .upload-label:hover { border-color: var(--text-muted); color: var(--text-secondary); }
  .upload-btn input[type="file"]:focus + .upload-label { border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-glow); }
  .stats {
    font-size: 10.5px; color: var(--text-muted);
    white-space: nowrap; text-align: right; margin-left: auto;
    font-variant-numeric: tabular-nums;
  }
  .stats span { color: var(--text-secondary); font-weight: 500; }

  .table-wrap {
    flex: 1;
    overflow: auto;
    position: relative;
  }
  .table-wrap:empty::after {
    content: "No entries found";
    display: block; text-align: center;
    padding: 64px 24px; color: var(--text-muted); font-size: 13px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
  }
  thead {
    position: sticky;
    top: 0;
    z-index: 5;
  }
  .date-range { display: flex; flex-direction: column; gap: 2px; width: 100%; }
  .date-range input[type="datetime-local"] { background: transparent; border: 1px solid var(--border); color: var(--text-secondary); padding: 1px 4px; border-radius: 3px; font-size: 10px; font-family: 'JetBrains Mono', monospace; width: 100%; box-sizing: border-box; color-scheme: dark; }
  .date-range input[type="datetime-local"]:focus { border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-glow); outline: none; }
  .date-range input[type="datetime-local"]::-webkit-calendar-picker-indicator { filter: invert(0.6); scale: 0.7; cursor: pointer; }
  thead .h-row th {
    background: var(--bg-surface);
    padding: 6px 10px;
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px;
    font-weight: 600;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    text-align: left;
    border-bottom: 1px solid var(--border-subtle);
    white-space: nowrap;
    user-select: none;
  }
  thead .h-row th { position: relative; overflow: visible; }
  .resize-handle {
    position: absolute;
    top: 0; right: -3px;
    width: 7px; height: 100%;
    cursor: col-resize;
    z-index: 10;
    background: transparent;
  }
  .resize-handle::after {
    content: '';
    position: absolute;
    top: 25%; right: 3px;
    width: 1px; height: 50%;
    background: var(--border);
    opacity: 0;
    transition: opacity 0.15s;
  }
  th:hover .resize-handle::after,
  .resize-handle:hover::after,
  .resize-handle.active::after { opacity: 1; }
  .resize-handle:hover,
  .resize-handle.active { background: rgba(245,158,11,0.08); }

  thead .f-row th {
    background: var(--bg-surface);
    padding: 4px 8px;
    border-bottom: 1px solid var(--border-subtle);
    vertical-align: middle;
  }
  thead .f-row th:first-child { padding-left: 20px; }
  .col-filter {
    width: 100%;
    padding: 3px 6px;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text-primary);
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    outline: none;
    transition: border-color 0.12s;
  }
  .col-filter:focus { border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-glow); }
  .col-filter::placeholder { color: var(--text-muted); font-size: 9px; }

  .user-dropdown { width: 100%; display: inline-block; box-sizing: border-box; }
  .user-dropbtn { width: 100%; background: var(--bg-elevated); border: 1px solid var(--border); color: var(--text-secondary); padding: 3px 6px; border-radius: 4px; cursor: pointer; font-size: 10px; font-family: 'Plus Jakarta Sans', sans-serif; text-align: left; white-space: nowrap; box-sizing: border-box; }
  .user-dropdown-content { display: none; position: fixed; z-index: 999; background: var(--bg-elevated); border: 1px solid var(--border); border-radius: 4px; min-width: 180px; max-height: 280px; overflow-y: auto; }
  .user-dropdown.open .user-dropdown-content { display: block; }
  .user-opt { display: block; padding: 4px 10px; font-size: 11px; cursor: pointer; font-family: 'Plus Jakarta Sans', sans-serif; white-space: nowrap; text-align: left; }
  .user-opt:hover { background: var(--bg-hover); }
  .user-opt input { margin-right: 6px; vertical-align: middle; }

  tbody tr {
    transition: background 0.1s;
    cursor: pointer;
  }
  tbody tr:hover { background: var(--bg-hover); }
  tbody tr td {
    padding: 5px 10px;
    border-bottom: 1px solid var(--border-subtle);
    vertical-align: top;
    line-height: 1.5;
    font-size: 11.5px;
  }
  tbody tr td:first-child { padding-left: 20px; width: 38px; }
  tbody tr td:nth-child(2) { color: var(--text-muted); white-space: nowrap; font-size: 11px; }
  tbody tr td:nth-child(3) { color: var(--text-secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  tbody tr td:nth-child(4) { color: var(--text-secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  tbody tr td:nth-child(5) { color: var(--text-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  tbody tr td:nth-child(8) { color: var(--text-primary); word-break: break-all; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; }
  tbody tr td:nth-child(8):hover { text-decoration: underline; text-decoration-color: var(--text-muted); }
  tbody tr.expanded td:nth-child(6) { white-space: normal; word-break: break-all; }

  .bar {
    display: inline-block;
    width: 3px;
    height: 22px;
    border-radius: 2px;
    vertical-align: middle;
    transition: height 0.15s;
  }
  tbody tr:hover .bar { height: 26px; }
  .badge {
    display: inline-block;
    font-size: 8.5px;
    font-weight: 700;
    padding: 0 6px;
    border-radius: 3px;
    line-height: 16px;
    letter-spacing: 0.3px;
    text-transform: uppercase;
    font-family: 'Plus Jakarta Sans', sans-serif;
  }

  @media (max-width: 900px) {
    thead .h-row th:nth-child(4), tbody tr td:nth-child(4),
    thead .h-row th:nth-child(5), tbody tr td:nth-child(5) { display: none; }
    thead .f-row th:nth-child(4),
    thead .f-row th:nth-child(5) { display: none; }
  }
  @media (max-width: 640px) {
    header { padding: 8px 12px; gap: 6px; }
    .search-wrap { max-width: none; order: 10; width: 100%; }
    .stats { margin-left: 0; }
    thead .h-row th:first-child, tbody tr td:first-child,
    thead .f-row th:first-child { padding-left: 12px; }
  thead .f-row .col-filter { width: 100%; box-sizing: border-box; }
  }
  .pagin { display: flex; align-items: center; justify-content: center; gap: 12px; padding: 16px 24px; font-size: 12px; color: var(--text-muted); }
  .pagin button { background: var(--bg-elevated); border: 1px solid var(--border); color: var(--text-secondary); padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 11px; font-family: 'Plus Jakarta Sans', sans-serif; }
  .pagin button:hover:not(:disabled) { background: var(--bg-hover); color: var(--text-primary); }
  .pagin button:disabled { opacity: 0.35; cursor: default; }
  .pagin .pgInfo { min-width: 160px; text-align: center; }
  .theme-toggle { background: var(--btn-bg); border: 1px solid var(--border); color: var(--text-secondary); width: 28px; height: 28px; border-radius: 5px; cursor: pointer; font-size: 16px; display: flex; align-items: center; justify-content: center; transition: background 0.15s; flex-shrink: 0; line-height: 1; }
  .theme-toggle:hover { background: var(--bg-hover); border-color: var(--text-muted); }
  .trigger-toggle { display: flex; align-items: center; gap: 4px; font-size: 10px; color: var(--text-muted); cursor: pointer; font-family: 'Plus Jakarta Sans', sans-serif; white-space: nowrap; flex-shrink: 0; padding: 2px 6px; border: 1px solid var(--border); border-radius: 4px; background: var(--btn-bg); transition: background 0.15s; height: 28px; box-sizing: border-box; }
  .trigger-toggle:hover { background: var(--bg-hover); border-color: var(--text-muted); }
  .trigger-toggle input { margin: 0; accent-color: var(--accent); }
  .refresh-btn { background: var(--btn-bg); border: 1px solid var(--border); color: var(--text-secondary); width: 28px; height: 28px; border-radius: 5px; cursor: pointer; font-size: 18px; display: flex; align-items: center; justify-content: center; transition: background 0.15s; flex-shrink: 0; padding: 0; line-height: 1; }
  .refresh-btn:hover { background: var(--bg-hover); border-color: var(--text-muted); }
  .refresh-btn.spin { animation: rspin 0.6s linear; }
  @keyframes rspin { to { transform: rotate(360deg); } }

  .spinner { position: fixed; inset: 0; z-index: 9998; display: flex; align-items: center; justify-content: center; background: rgba(0,0,0,0.3); }
  .spinner-ring { width: 40px; height: 40px; border: 3px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: sp 0.7s linear infinite; }
  @keyframes sp { to { transform: rotate(360deg); } }
  .sql-modal { display: none; position: fixed; inset: 0; z-index: 9999; background: rgba(0,0,0,0.6); align-items: center; justify-content: center; }
  .sql-modal.open { display: flex; }
  .sql-modal .modal-box { background: var(--bg-surface); border: 1px solid var(--border); border-radius: 8px; max-width: 90vw; max-height: 85vh; width: 900px; display: flex; flex-direction: column; }
  .sql-modal .modal-header { display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; border-bottom: 1px solid var(--border); }
  .sql-modal .modal-header h3 { margin: 0; font-size: 13px; font-weight: 600; color: var(--text-primary); font-family: 'Plus Jakarta Sans', sans-serif; }
  .sql-modal .modal-close { background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: 18px; padding: 0 4px; line-height: 1; }
  .sql-modal .modal-close:hover { color: var(--text-primary); }
  .sql-modal .modal-body { padding: 16px; overflow: auto; font-family: 'JetBrains Mono', monospace; font-size: 12px; line-height: 1.6; color: var(--text-primary); white-space: pre-wrap; word-break: break-word; }
  .sql-modal .modal-body .kw { color: #569cd6; font-weight: 600; }
  .sql-modal .modal-body .fn { color: #dcdcaa; }
  .sql-modal .modal-body .str { color: #ce9178; }
  .sql-modal .modal-body .num { color: #b5cea8; }
  .sql-modal .modal-body .cmt { color: #6a9955; font-style: italic; }
  .sql-modal .modal-body .tag { color: #569cd6; }
  .sql-modal .modal-body .attr { color: #9cdcfe; }
  .sql-modal .modal-body .pi { color: #6a9955; font-style: italic; }
  .sql-modal .modal-body.op-select,
  .sql-modal .modal-body.op-with   { border-left: 3px solid #569cd6; }
  .sql-modal .modal-body.op-insert { border-left: 3px solid #4ec9b0; }
  .sql-modal .modal-body.op-update { border-left: 3px solid #dcdcaa; }
  .sql-modal .modal-body.op-delete { border-left: 3px solid #f44747; }
  .sql-modal .modal-body.op-create { border-left: 3px solid #c586c0; }
  .sql-modal .modal-body.op-alter  { border-left: 3px solid #d7ba7d; }
  .sql-modal .modal-body.op-drop   { border-left: 3px solid #f44747; }
  .sql-modal .modal-body.op-merge  { border-left: 3px solid #4ec9b0; }
  .sql-modal .modal-body.op-exec   { border-left: 3px solid #9cdcfe; }
  .sql-modal .modal-body.op-begin  { border-left: 3px solid #c586c0; }
  .sql-modal .modal-body.op-commit { border-left: 3px solid #4ec9b0; }
  .sql-modal .modal-body.op-truncate { border-left: 3px solid #f44747; }
  .sql-modal .modal-body .kw-select { color: #569cd6 !important; }
  .sql-modal .modal-body .kw-with   { color: #569cd6 !important; }
  .sql-modal .modal-body .kw-insert { color: #4ec9b0 !important; }
  .sql-modal .modal-body .kw-insert { color: #4ec9b0 !important; }
  .sql-modal .modal-body .kw-update { color: #dcdcaa !important; }
  .sql-modal .modal-body .kw-delete { color: #f44747 !important; }
  .sql-modal .modal-body .kw-create { color: #c586c0 !important; }
  .sql-modal .modal-body .kw-alter  { color: #d7ba7d !important; }
  .sql-modal .modal-body .kw-drop   { color: #f44747 !important; }
  .sql-modal .modal-body .kw-merge  { color: #4ec9b0 !important; }
  .sql-modal .modal-body .kw-exec   { color: #9cdcfe !important; }
  .sql-modal .modal-body .kw-begin  { color: #c586c0 !important; }
  .sql-modal .modal-body .kw-commit { color: #4ec9b0 !important; }
  td.msg-cell .op-bar {
    display: inline-block;
    width: 3px;
    height: 22px;
    border-radius: 2px;
    vertical-align: middle;
    margin-right: 6px;
    flex-shrink: 0;
  }
</style>
</head>
<body>
<header>
  <div class="brand">
    <div class="brand-icon">L</div>
    <h1>LogViewer</h1>
  </div>
  <div class="search-wrap">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>
    <input type="text" id="search" placeholder="Search all..." autofocus>
  </div>
  <div class="upload-btn">
    <input type="file" id="file-input" hidden>
    <button id="uploadBtn" class="upload-label" title="Open file">📂 Open file…</button>
  </div>
  <button class="refresh-btn" id="refreshBtn" title="Refresh data">↻</button>
  <label class="trigger-toggle"><input type="checkbox" id="fhideTriggers"> triggers</label>
  <div class="stats"><span id="count">0</span> / <span id="total">0</span></div>
  <button class="theme-toggle" id="themeToggle" title="Toggle theme"></button>
</header>
<div id="log-container" class="table-wrap"></div>
<div id="paginator"></div>
<div id="spinner" class="spinner" style="display:none"><div class="spinner-ring"></div></div>
<div class="sql-modal" id="sqlModal">
  <div class="modal-box">
    <div class="modal-header">
      <h3 id="sqlModalTitle">SQL Query</h3>
      <button class="modal-close" id="sqlModalClose">&times;</button>
    </div>
    <div class="modal-body" id="sqlModalBody"></div>
  </div>
</div>
<script>
const COLORS = [
  '#f59e0b','#3b82f6','#10b981','#8b5cf6','#ec4899','#06b6d4',
  '#84cc16','#f43f5e','#14b8a6','#a855f7','#eab308','#6366f1',
  '#fb923c','#2dd4bf','#818cf8','#f472b6','#22d3ee','#d946ef',
  '#34d399','#fb7185','#a78bfa','#fbbf24','#60a5fa','#4ade80',
  '#c084fc','#f87171','#facc15','#38bdf8','#a3e635','#e879f9',
  '#6ee7b7','#7dd3fc','#fca5a5','#5eead4','#fdba74','#c4b5fd',
  '#86efac','#67e8f9','#fda4af','#d8b4fe'
];
function colorFor(value) {
  if (!value) return '#475569';
  let h = 0;
  for (let i = 0; i < value.length; i++) { h = ((h << 5) - h) + value.charCodeAt(i); h |= 0; }
  return COLORS[Math.abs(h) % COLORS.length];
}

const OP_COLORS = {
  select: '#569cd6', with: '#569cd6', insert: '#4ec9b0', update: '#dcdcaa',
  delete: '#f44747', create: '#c586c0', alter: '#d7ba7d', drop: '#f44747',
  merge: '#4ec9b0', exec: '#9cdcfe', begin: '#c586c0', commit: '#4ec9b0', truncate: '#f44747',
};

let allRecords = [], currentFile = '', allTotal = 0, pageSkip = 0, pageLimit = 2000;
const container = document.getElementById('log-container');
const searchInput = document.getElementById('search');
const countEl = document.getElementById('count');
const totalEl = document.getElementById('total');

const colFilters = { level: '', dateFrom: '', dateTo: '', comp: '', proc: '', thread: '', user: new Set(), msg: '', source: '', line: '', sourceC: '', lineC: '', hideTriggers: false };

const STORAGE_KEY = 'lv_colwidths';
const COL_KEYS = ['bar', 'level', 'ts', 'comp', 'proc', 'thread', 'user', 'msg', 'source', 'line', 'sourceC', 'lineC'];
const DEFAULT_WIDTHS = { bar: 38, level: 70, ts: 155, comp: 180, proc: 170, thread: 60, user: 130, msg: null, source: 160, line: 55, sourceC: 120, lineC: 55 };
let colWidths = {};

function loadWidths() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
    COL_KEYS.forEach(k => { colWidths[k] = saved[k] || DEFAULT_WIDTHS[k]; });
  } catch (_) { COL_KEYS.forEach(k => { colWidths[k] = DEFAULT_WIDTHS[k]; }); }
}
function saveWidths() {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(colWidths)); } catch (_) {}
}
loadWidths();

const resizeState = { col: null, startX: 0, startW: 0 };

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function buildUserDropdown() {
  const users = new Set();
  for (const r of allRecords) {
    const u = ((r.v||{}).user||'').trim();
    if (u && u !== 'n/a') users.add(u);
  }
  const sorted = [...users].sort();
  const selectedCount = colFilters.user.size;
  const label = selectedCount === 0 ? 'All users' : selectedCount + ' user' + (selectedCount > 1 ? 's' : '');
  const c = selectedCount === 0 ? '' : ' style="color:' + colorFor([...colFilters.user][0]) + '"';
  let html = '<div class="user-dropdown" id="user-filter">';
  html += '<button class="user-dropbtn"' + c + '>' + esc(label) + ' &#9662;</button>';
  html += '<div class="user-dropdown-content">';
  html += '<label class="user-opt' + (selectedCount === 0 ? ' active' : '') + '"><input type="checkbox" data-user="__all__"' + (selectedCount === 0 ? ' checked' : '') + '> All</label>';
  for (const u of sorted) {
    const checked = colFilters.user.has(u);
    html += '<label class="user-opt' + (checked ? ' active' : '') + '" style="color:' + colorFor(u) + '"><input type="checkbox" data-user="' + esc(u) + '"' + (checked ? ' checked' : '') + '> ' + esc(u) + '</label>';
  }
  html += '</div></div>';
  return html;
}

function simplifyLevel(lv) {
  if (!lv) return '';
  const m = lv.match(/^(\d+|[a-zA-Z]+)/);
  return m ? m[1] : lv;
}

function formatSql(sql) {
  const keywords = 'SELECT|FROM|WHERE|AND|OR|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|FULL|ON|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TABLE|INDEX|INTO|VALUES|SET|GROUP\\s+BY|ORDER\\s+BY|HAVING|LIMIT|OFFSET|AS|DISTINCT|UNION|ALL|BETWEEN|IN|NOT|NULL|IS|LIKE|EXISTS|CASE|WHEN|THEN|ELSE|END|BEGIN|COMMIT|ROLLBACK|TRUNCATE|MERGE|USING|WITH|RECURSIVE|FETCH|NEXT|ROWS|ONLY|ASC|DESC|COUNT|SUM|AVG|MIN|MAX|COALESCE|CAST|CONVERT|SUBSTRING|TRIM|UPPER|LOWER|LENGTH|REPLACE|ROUND|NVL|DECODE|EXTRACT|TO_CHAR|TO_DATE|TO_NUMBER';
  const re = new RegExp('\\b(' + keywords + ')\\b', 'gi');
  let formatted = sql
    .replace(re, m => m.toUpperCase())
    .replace(/\b(SELECT)\b/g, '\n$1')
    .replace(/\b(FROM)\b/g, '\n$1')
    .replace(/\b(WHERE)\b/g, '\n$1')
    .replace(/\b(AND|OR)\b(?=\s+\w)/g, '\n  $1')
    .replace(/\b(ORDER\s+BY)\b/g, '\n$1')
    .replace(/\b(GROUP\s+BY)\b/g, '\n$1')
    .replace(/\b(HAVING)\b/g, '\n$1')
    .replace(/\b(LIMIT)\b/g, '\n$1')
    .replace(/\b((LEFT|RIGHT|INNER|CROSS|FULL)?\s*JOIN)\b/gi, '\n$1')
    .replace(/\b(ON)\b/g, '\n  $1')
    .replace(/\b(UNION)\b/g, '\n$1\n')
    .replace(/\b(INSERT\s+INTO)\b/g, '\n$1')
    .replace(/\b(VALUES)\b/g, '\n$1')
    .replace(/\b(SET)\b/g, '\n$1')
    .replace(/^\n+/, '')
    .trim();
  return formatted;
}

function formatSqlHtml(sql, op) {
  const f = formatSql(sql);
  let html = esc(f)
    .replace(/\b(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|FULL|ON|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TABLE|INDEX|INTO|VALUES|SET|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|AS|DISTINCT|UNION|ALL|BETWEEN|IN|NOT|NULL|IS|LIKE|EXISTS|CASE|WHEN|THEN|ELSE|END|BEGIN|COMMIT|ROLLBACK|TRUNCATE|MERGE|USING|WITH|RECURSIVE|FETCH|NEXT|ROWS|ONLY|ASC|DESC)\b/gi, '<span class="kw">$&</span>')
    .replace(/\b(COUNT|SUM|AVG|MIN|MAX|COALESCE|CAST|CONVERT|SUBSTRING|TRIM|UPPER|LOWER|LENGTH|REPLACE|ROUND|NVL|DECODE|EXTRACT|TO_CHAR|TO_DATE|TO_NUMBER)\b/gi, '<span class="fn">$&</span>')
    .replace(/'[^']*'/g, '<span class="str">$&</span>');
  if (op) {
    html = html.replace('<span class="kw">' + op.toUpperCase() + '</span>', '<span class="kw kw-' + op + '">' + op.toUpperCase() + '</span>');
  }
  return html;
}

function formatXml(xml) {
  let indent = 0;
  let result = '';
  let s = xml.replace(/>\s+</g, '><').trim();
  const parts = s.match(/<[^>]+>|[^<]+/g) || [];
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    if (p.startsWith('</')) {
      indent = Math.max(0, indent - 1);
      result += '\n' + '  '.repeat(indent) + p;
    } else if (p.startsWith('<')) {
      if (p.endsWith('/>') || p.startsWith('<?')) {
        result += '\n' + '  '.repeat(indent) + p;
      } else {
        const next = parts[i + 1];
        const afterNext = parts[i + 2];
        if (next && next.startsWith('</')) {
          result += '\n' + '  '.repeat(indent) + p + next;
          i += 1;
        } else if (next && afterNext && !next.startsWith('<') && afterNext.startsWith('</')) {
          result += '\n' + '  '.repeat(indent) + p + next.trim() + afterNext;
          i += 2;
        } else {
          result += '\n' + '  '.repeat(indent) + p;
          indent++;
        }
      }
    } else {
      const trimmed = p.trim();
      if (trimmed) {
        result += '\n' + '  '.repeat(indent) + trimmed;
      }
    }
  }
  return result.trim();
}

function formatXmlHtml(xml) {
  const pretty = formatXml(xml);
  let h = pretty
    .replace(/&(?!(?:amp|lt|gt|quot|apos|#[0-9]+|#x[0-9a-fA-F]+);)/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  h = h.replace(/(&lt;\?[\s\S]*?\?&gt;)/g, '<span class="pi">$1</span>');
  h = h.replace(/(&lt;\/?)([\w:-]+)/g, '$1<span class="tag">$2</span>');
  h = h.replace(/([\w:-]+)(=)/g, '<span class="attr">$1</span>$2');
  h = h.replace(/(")([^"]*)(")/g, '<span class="str">$1$2$3</span>');
  return h;
}

const sqlModal = document.getElementById('sqlModal');
const sqlModalBody = document.getElementById('sqlModalBody');
document.getElementById('sqlModalClose').addEventListener('click', () => { sqlModal.classList.remove('open'); sqlModalBody.className = 'modal-body'; });
sqlModal.addEventListener('click', (e) => { if (e.target === sqlModal) { sqlModal.classList.remove('open'); sqlModalBody.className = 'modal-body'; } });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && sqlModal.classList.contains('open')) { sqlModal.classList.remove('open'); sqlModalBody.className = 'modal-body'; } });

function detectSqlOp(sql) {
  const s = sql.trimStart().toUpperCase();
  if (/^WITH\b/.test(s)) return 'with';
  if (/^SELECT\b/.test(s)) return 'select';
  if (/^INSERT\b/.test(s)) return 'insert';
  if (/^UPDATE\b/.test(s)) return 'update';
  if (/^DELETE\b/.test(s)) return 'delete';
  if (/^CREATE\b/.test(s)) return 'create';
  if (/^ALTER\b/.test(s)) return 'alter';
  if (/^DROP\b/.test(s) || /^TRUNCATE\b/.test(s)) return 'drop';
  if (/^MERGE\b/.test(s)) return 'merge';
  if (/^EXEC\b/.test(s) || /^CALL\b/.test(s) || /^DECLARE\b/.test(s)) return 'exec';
  if (/^BEGIN\b/.test(s)) return 'begin';
  if (/^COMMIT\b/.test(s)) return 'commit';
  return '';
}

function openSqlModal(msg, opClass) {
  const isReu = currentFile && currentFile.toLowerCase().includes('reu');
  const isXml = !isReu && /^\s*</.test(msg) && msg.includes('>') && (/<\//.test(msg) || /\/\s*>/.test(msg));
  document.getElementById('sqlModalTitle').textContent = isReu ? 'SQL Query' : isXml ? 'XML' : 'Mensaje completo';
  const op = opClass || (isReu ? detectSqlOp(msg) : '');
  sqlModalBody.className = 'modal-body' + (op ? ' op-' + op : '');
  sqlModalBody.innerHTML = isReu ? formatSqlHtml(msg, op) : isXml ? formatXmlHtml(msg) : esc(msg);
  sqlModal.classList.add('open');
}

function colStyle(key) {
  const w = colWidths[key];
  return w ? 'width:' + w + 'px' : '';
}

function startResize(col, e) {
  resizeState.col = col;
  resizeState.startX = e.clientX;
  resizeState.startW = colWidths[col] || 0;
  document.body.style.cursor = 'col-resize';
  document.body.style.userSelect = 'none';
  e.stopPropagation();
  e.preventDefault();
}

function buildResizeHandlers(table) {
  const thead = table.querySelector('thead');
  if (!thead) return;
  const hRow = thead.querySelector('.h-row');
  if (!hRow) return;
  hRow.querySelectorAll('th').forEach((th, i) => {
    const key = COL_KEYS[i];
    if (!key) return;
    const handle = document.createElement('div');
    handle.className = 'resize-handle';
    handle.addEventListener('mousedown', (e) => startResize(key, e));
    th.appendChild(handle);
    // Apply stored width
    if (colWidths[key]) {
      th.style.width = colWidths[key] + 'px';
    }
  });
}

document.addEventListener('mousemove', (e) => {
  if (!resizeState.col) return;
  const dx = e.clientX - resizeState.startX;
  let newW = Math.max(30, resizeState.startW + dx);
  // Find matching col
  const key = resizeState.col;
  colWidths[key] = newW;
  saveWidths();
  // Update all col elements and th elements for this column
  const idx = COL_KEYS.indexOf(key);
  if (idx >= 0) {
    document.querySelectorAll('table').forEach(tbl => {
      const colEl = tbl.querySelector('colgroup col:nth-child(' + (idx+1) + ')');
      if (colEl) colEl.style.width = newW + 'px';
      const hRow = tbl.querySelector('thead .h-row');
      if (hRow) {
        const th = hRow.children[idx];
        if (th) th.style.width = newW + 'px';
      }
    });
  }
});

document.addEventListener('mouseup', () => {
  if (!resizeState.col) return;
  resizeState.col = null;
  document.body.style.cursor = '';
  document.body.style.userSelect = '';
});

document.addEventListener('click', () => {
  document.querySelectorAll('.user-dropdown.open').forEach(el => el.classList.remove('open'));
});

function getFiltered() {
  return allRecords;
}

function renderRows(records) {
  let rows = '';
  for (const rec of records) {
    const color = rec.c;
    let barColor = 'transparent', dotColor = '#475569', badgeText = '';
    if (color && typeof color === 'object') {
      if (color.fromValue) { badgeText = simplifyLevel((color.fromValue.value||'')).toUpperCase(); dotColor = colorFor(color.fromValue.value); barColor = dotColor; }
      else if (color.fixed) { dotColor = color.fixed.color||'#475569'; barColor = dotColor; }
    }
    const vars = rec.v||{};
    const ts = esc(vars.timestamp||vars.time||'');
    const comp = esc(vars.component||'');
    const proc = esc(vars.proc||'');
    const thread = esc(vars.thread||'');
    const user = esc(vars.user||'');
    const msg = esc(vars.message||'');
    const source = esc(vars.source||'');
    const line = esc(vars.line||'');
    const sourceC = esc(vars.sourceC||'');
    const lineC = esc(vars.lineC||'');
    const mShort = msg.length > 200 ? msg.slice(0,200)+'...' : msg;
    const opClass = rec.op || '';
    const opColor = OP_COLORS[opClass] || '';
    const opBar = opColor ? '<span class="op-bar" style="background:'+opColor+';box-shadow:0 0 6px '+opColor+'55"></span>' : '';
    const fnShort = source.split('\\').pop().split('/').pop();
    const scShort = sourceC.split('\\').pop().split('/').pop();

    rows += '<tr>';
    rows += '<td><span class="bar" style="background:' + barColor + ';' + (barColor!=='transparent'?'box-shadow:0 0 6px '+barColor+'55':'') + '"></span></td>';
    rows += '<td>' + (badgeText ? '<span class="badge" style="background:'+dotColor+'18;color:'+dotColor+'">' + badgeText + '</span>' : '') + '</td>';
    rows += '<td>' + ts + '</td>';
    rows += '<td>' + comp + '</td>';
    rows += '<td>' + proc + '</td>';
    rows += '<td>' + thread + '</td>';
    rows += '<td>' + user + '</td>';
    rows += '<td class="msg-cell">' + opBar + mShort + '</td>';
    rows += '<td title="' + esc(source) + '">' + fnShort + '</td>';
    rows += '<td>' + line + '</td>';
    rows += '<td title="' + esc(sourceC) + '">' + scShort + '</td>';
    rows += '<td>' + lineC + '</td>';
    rows += '</tr>';
  }
  return rows;
}

function buildPagin() {
  const prev = pageSkip === 0;
  const next = pageSkip + pageLimit >= allTotal;
  return '<div class="pagin">' +
    '<button id="pgPrev"' + (prev ? ' disabled' : '') + '>&#9664; Prev</button> ' +
    '<span class="pgInfo">' + (allTotal > 0 ? (pageSkip + 1) + '&ndash;' + Math.min(pageSkip + pageLimit, allTotal) : '0') + ' of ' + allTotal + '</span> ' +
    '<button id="pgNext"' + (next ? ' disabled' : '') + '>&#9654; Next</button>' +
    '</div>';
}

function doRenderAll() {
  const filtered = getFiltered();
  const colgroup = '<colgroup>' +
    COL_KEYS.map(k => '<col style="' + colStyle(k) + '">').join('') +
    '</colgroup>';

  const hasContent = allTotal > 0 || filtered.length > 0;
  if (!hasContent) {
    container.innerHTML = '<div style="text-align:center;padding:64px 24px;color:var(--text-muted);font-size:13px">No entries match filters</div>';
    countEl.textContent = '0';
    totalEl.textContent = '0';
    return;
  }

  container.innerHTML = '<table>' + colgroup + '<thead>' +
    '<tr class="h-row"><th></th><th>VM</th><th>Fecha</th><th>Objeto</th><th>Procedimiento</th><th>Thr</th><th>Usuario</th><th>Texto</th><th>Fuente</th><th>L&iacute;nea</th><th>Fuente C</th><th>L&iacute;nea C</th></tr>' +
    '<tr class="f-row">' +
      '<th></th>' +
      '<th><input class="col-filter" id="flevel" placeholder="vm…" value="' + esc(colFilters.level) + '"></th>' +
      '<th><div class="date-range"><input type="datetime-local" id="fdateFrom" value="' + esc(colFilters.dateFrom) + '"><input type="datetime-local" id="fdateTo" value="' + esc(colFilters.dateTo) + '"></div></th>' +
      '<th><input class="col-filter" id="fcomp" placeholder="obj…" value="' + esc(colFilters.comp) + '"></th>' +
      '<th><input class="col-filter" id="fproc" placeholder="proc…" value="' + esc(colFilters.proc) + '"></th>' +
      '<th><input class="col-filter" id="fthread" placeholder="thr…" value="' + esc(colFilters.thread) + '"></th>' +
      '<th>' + buildUserDropdown() + '</th>' +
      '<th><input class="col-filter" id="fmsg" placeholder="texto…" value="' + esc(colFilters.msg) + '"></th>' +
      '<th><input class="col-filter" id="fsource" placeholder="src…" value="' + esc(colFilters.source) + '"></th>' +
      '<th><input class="col-filter" id="fline" placeholder="ln…" value="' + esc(colFilters.line) + '"></th>' +
      '<th><input class="col-filter" id="fsourceC" placeholder=".cpp…" value="' + esc(colFilters.sourceC) + '"></th>' +
      '<th><input class="col-filter" id="flineC" placeholder="ln…" value="' + esc(colFilters.lineC) + '"></th>' +
    '</tr>' +
    '</thead><tbody>' + renderRows(filtered) + '</tbody></table>';

  document.getElementById('paginator').innerHTML = buildPagin();

  countEl.textContent = filtered.length;
  totalEl.textContent = allTotal;

  const tbl = container.querySelector('table');
  if (tbl) buildResizeHandlers(tbl);

  container.querySelectorAll('tbody tr').forEach(el => {
    el.addEventListener('click', (e) => {
      if (e.target.closest('.msg-cell')) return;
      el.classList.toggle('expanded');
    });
  });

  const msgCells = container.querySelectorAll('.msg-cell');
  for (let i = 0; i < msgCells.length; i++) {
    msgCells[i].addEventListener('click', (e) => {
      e.stopPropagation();
      const record = filtered[i];
      if (record) openSqlModal(((record.v||{}).message||''), record.op);
    });
  }

  document.querySelectorAll('.col-filter').forEach(inp => {
    inp.addEventListener('input', () => {
      const id = inp.id;
      if (id === 'flevel') colFilters.level = inp.value;
      else if (id === 'fdateFrom') colFilters.dateFrom = inp.value;
      else if (id === 'fdateTo') colFilters.dateTo = inp.value;
      else if (id === 'fcomp') colFilters.comp = inp.value;
      else if (id === 'fproc') colFilters.proc = inp.value;
      else if (id === 'fthread') colFilters.thread = inp.value;
      else if (id === 'fmsg') colFilters.msg = inp.value;
      else if (id === 'fsource') colFilters.source = inp.value;
      else if (id === 'fline') colFilters.line = inp.value;
      else if (id === 'fsourceC') colFilters.sourceC = inp.value;
      else if (id === 'flineC') colFilters.lineC = inp.value;
      fetchPage();
    });
  });

  document.getElementById('fhideTriggers')?.addEventListener('change', (e) => {
    colFilters.hideTriggers = e.target.checked;
    fetchPage();
  });

  const userDD = document.getElementById('user-filter');
  if (userDD) {
    const btn = userDD.querySelector('.user-dropbtn');
    const content = userDD.querySelector('.user-dropdown-content');
    if (btn && content) {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const open = userDD.classList.toggle('open');
        if (open) {
          const r = btn.getBoundingClientRect();
          content.style.left = r.left + 'px';
          content.style.top = (r.bottom + 4) + 'px';
          content.style.width = '';
          content.style.minWidth = '';
        }
      });
    }
    userDD.querySelectorAll('.user-opt input').forEach(cb => {
      cb.addEventListener('change', () => {
        const u = cb.dataset.user;
        if (u === '__all__') {
          colFilters.user.clear();
        } else {
          if (cb.checked) colFilters.user.add(u);
          else colFilters.user.delete(u);
          // uncheck "All" when any individual is toggled
          const allCb = userDD.querySelector('[data-user="__all__"]');
          if (allCb) allCb.checked = colFilters.user.size === 0;
        }
        fetchPage();
      });
    });
  }

  document.getElementById('pgPrev')?.addEventListener('click', () => {
    if (pageSkip > 0) { pageSkip = Math.max(0, pageSkip - pageLimit); fetchPage(); }
  });
  document.getElementById('pgNext')?.addEventListener('click', () => {
    if (pageSkip + pageLimit < allTotal) { pageSkip += pageLimit; fetchPage(); }
  });
}

async function fetchPage(forceRefresh) {
  if (!currentFile && !allRecords.length) { doRenderAll(); return; }
  showSpinner();
  const params = new URLSearchParams();
  if (currentFile) params.set('file', currentFile);
  params.set('skip', pageSkip);
  params.set('limit', pageLimit);
  if (forceRefresh === true) params.set('refresh', 'true');
  if (colFilters.level) params.set('level', colFilters.level);
  if (colFilters.comp) params.set('comp', colFilters.comp);
  if (colFilters.proc) params.set('proc', colFilters.proc);
  if (colFilters.thread) params.set('thread', colFilters.thread);
  if (colFilters.user.size === 1) {
    const u = colFilters.user.values().next().value;
    if (u) params.set('user', u);
  }
  if (colFilters.msg) params.set('msg', colFilters.msg);
  if (colFilters.source) params.set('source', colFilters.source);
  if (colFilters.line) params.set('line', colFilters.line);
  if (colFilters.sourceC) params.set('sourceC', colFilters.sourceC);
  if (colFilters.lineC) params.set('lineC', colFilters.lineC);
  if (colFilters.dateFrom) params.set('dateFrom', colFilters.dateFrom);
  if (colFilters.dateTo) params.set('dateTo', colFilters.dateTo);
  if (colFilters.hideTriggers) params.set('hideTriggers', 'true');
  const q = searchInput.value.trim();
  if (q) params.set('q', q);
  try {
    const res = await fetch('/api/query?' + params.toString());
    if (!res.ok) throw new Error(res.status + '');
    const data = await res.json();
    allRecords = data.r || [];
    allTotal = data.t || 0;
  } catch (_) {
    container.innerHTML = '';
    allRecords = [];
    allTotal = 0;
  }
  hideSpinner();
  updateColVisibility();
  doRenderAll();
}

function updateColVisibility() {
  // nth-child by variable name: 1=bar(none), 2=level, 3=ts, 4=comp, 5=proc, 6=thread, 7=user, 8=msg, 9=source, 10=line, 11=sourceC, 12=lineC
  const COL_VARS = [null, 'level', 'timestamp', 'component', 'proc', 'thread', 'user', 'message', 'source', 'line', 'sourceC', 'lineC'];
  const empty = [];
  for (let n = 2; n <= 12; n++) {
    const v = COL_VARS[n-1];
    let has = false;
    for (const r of allRecords) {
      if (((r.v||{})[v]||'')) { has = true; break; }
    }
    if (!has) empty.push(n);
  }
  let el = document.getElementById('colhider');
  if (!el) { el = document.createElement('style'); el.id = 'colhider'; document.head.appendChild(el); }
  if (empty.length === 0) { el.textContent = ''; return; }
  const sels = empty.map(i => ['colgroup col:nth-child('+i+')','thead th:nth-child('+i+')','tbody td:nth-child('+i+')'].join(','));
  el.textContent = sels.join(',') + '{display:none}';
}

searchInput.addEventListener('input', fetchPage);

document.getElementById('uploadBtn').addEventListener('click', () => {
  document.getElementById('file-input').click();
});
document.getElementById('file-input').addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  container.innerHTML = '';
  allRecords = [];
  allTotal = 0;
  currentFile = '';
  document.getElementById('count').textContent = '0';
  document.getElementById('total').textContent = '0';
  showSpinner();
  await new Promise(r => setTimeout(r, 20));
  try {
    const res = await fetch('/api/upload?name=' + encodeURIComponent(file.name), { method: 'POST', body: file });
    if (!res.ok) throw new Error(res.status + ' ' + res.statusText);
    const data = await res.json();
    currentFile = data.path;
    pageSkip = 0;
    colFilters.level = ''; colFilters.comp = ''; colFilters.proc = ''; colFilters.thread = '';
    colFilters.user = new Set(); colFilters.msg = '';
    colFilters.source = ''; colFilters.line = ''; colFilters.sourceC = ''; colFilters.lineC = '';
    colFilters.hideTriggers = false;
    await fetchPage();
  } catch (_) {
    hideSpinner();
    container.innerHTML = '';
    allRecords = [];
    allTotal = 0;
    currentFile = '';
    document.getElementById('count').textContent = 'error';
    doRenderAll();
  }
  hideSpinner();
  e.target.value = '';
});

document.getElementById('refreshBtn')?.addEventListener('click', () => {
  const btn = document.getElementById('refreshBtn');
  btn.classList.remove('spin');
  void btn.offsetWidth;
  btn.classList.add('spin');
  fetchPage(true);
});

function setTheme(t) {
  document.documentElement.setAttribute('data-theme', t);
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = t === 'dark' ? '\u2600' : '\u263E';
  try { localStorage.setItem('lv_theme', t); } catch (_) {}
}

document.getElementById('themeToggle')?.addEventListener('click', () => {
  const cur = document.documentElement.getAttribute('data-theme') || 'dark';
  setTheme(cur === 'dark' ? 'light' : 'dark');
});

(function initTheme() {
  const saved = (() => { try { return localStorage.getItem('lv_theme'); } catch (_) { return null; } })();
  setTheme(saved || 'dark');
})();

(function init() {
  doRenderAll();
})();

function showSpinner() { document.getElementById('spinner').style.display = 'flex'; }
function hideSpinner() { document.getElementById('spinner').style.display = 'none'; }
</script>
</body>
</html>"##;
