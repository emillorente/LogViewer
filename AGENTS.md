## Goal
- Build a web-based log viewer (Rust + JavaScript) that auto-detects structured log formats (CORE.OUT ~-delimited and reu.out SQL trace), displays all original columns, and provides per-column filters with pagination.

## Constraints & Preferences
- 11 original system columns: VM, Objeto, Procedimiento, Usuario, Hoa (Fecha), Thr, Texto, Fuente, Línea, Fuente C, Línea C.
- Columns resizable via drag handles, widths persist in localStorage.
- Dark + light theme toggle (icons show target: ☀ in dark, ☾ in light), persisted in localStorage.
- Filters: text inputs for most columns, multi-select dropdown for Usuario, date range pickers (from/to) for Fecha. All applied server-side.
- Global search across all fields combined with AND; column filters combine with AND.
- Pagination with Prev/Next, 2000 records per page, server-side.
- Server returns only a page of records, not the full file.
- User filter dropdown must be left-aligned and full column width.
- Must auto-detect CORE.OUT format (tilde-delimited) and reu.out format (SQL trace with CONTEXT headers).
- Must auto-select the correct view file based on filename (view_core.json for .OUT, view_reu.json for reu).
- No pre-loaded files — app starts empty, only loads via "Open file…" upload button.
- Trigger toggle (`☐ triggers`) in toolbar to hide records whose Objeto starts with `TRIGGER`; persists across file switches.
- XML detection in CORE.OUT messages: if text starts with `<` and contains `>` and either `</` or `/>`, format as XML in modal with syntax highlighting.
- SQL operation detection in reu: first keyword (SELECT, INSERT, etc.) detected, colored bar shown in Texto column, border + colored keyword in modal.

## Done
### UI / Frontend
- Replaced user pill toggles with a multi-select dropdown (checkboxes, full-width button, fixed positioning to avoid clipping).
- Replaced Hoa text filter with two `<input type="date">` (Desde / Hasta) for date range filtering.
- Added dark/light theme toggle (☀ in dark, ☾ in light) with CSS variables and localStorage persistence.
- Theme toggle icons represent the target state (click ☀ to go light).
- Added `formatSql()` / `formatSqlHtml()` for basic SQL syntax highlighting with line-break formatting.
- Added `formatXml()` / `formatXmlHtml()` for XML pretty-printing with syntax highlighting (tags, attributes, values, processing instructions, inline for text-only elements).
- Added click-to-expand modal for Texto column: shows full formatted SQL/XML, dynamic title, closable via ×/Escape/click-outside.
- Added `updateColVisibility()` that hides columns whose variable field is empty across current page.
- Added SQL operation detection (`detectSqlOp()`) with colored bar (`.op-bar`) in Texto column + colored keyword in modal.
- Added trigger filter checkbox (`☐ triggers`) in toolbar; persists when switching files, resets on new upload.
- Replaced overlay-based file input with explicit `<button>` that triggers hidden `<input type="file">` via JS.
- Removed file-picker dropdown; app starts empty, no default file auto-loading.
- Added loading spinner (centered, animated ring) during upload and query fetches.
- Old data cleared immediately when starting a new upload.

### Backend / Rust
- Created `LogCoreReader` in readers.rs: byte-by-byte state machine that reads `~@_~`-delimited records (handles multi-line XML messages spanning many physical lines in CORE.OUT).
- Created `LogQueryReader` in readers.rs that merges SQL trace header + SQL text into one record.
- Created `view_reu.json` regex for CONTEXT fields (timing, level, component, proc, timestamp, source, line) + SQL query as message.
- Added `detect_reader()` that reads first 512 bytes and returns `LogCoreReader` (if `~@_~` detected), `LogQueryReader` (if starts with `/***`), or `LogFile` (fallback line-by-line).
- Changed `process()` from generic `R: LogReader` to `Box<dyn LogReader>` for dynamic dispatch.
- View auto-detection per-request in `handle_query`/`handle_upload` via `view_for_file()` (filename-based).
- Upload handler returns only `{ path }` (no record counting); total is computed on first server-side query.
- Query handler applies all filters server-side: column filters (case-insensitive substring), date range (ISO datetime), global search (space-separated AND), trigger filter.
- `date_str_to_iso()` converts datetime strings from both formats (`dd/MM/yy HH:mm:ss,fff` for CORE.OUT, `yy/MM/dd HH:mm:ss` for reu.out) to ISO 8601.
- Date/time filter uses `<input type="datetime-local">` for hour-level precision; `dateTo` normalized to end-of-minute (`:59`).
- In-memory record cache (`RwLock<HashMap<String, Vec<Record>>>`): first query loads file into RAM, subsequent queries filter from cache (<10ms).
- Added `Clone` derives on `Record` and `Color` for cache storage.
- Increased upload content-length limit from 100 MB to 1 GB.

### Code Quality
- Added `#![warn(clippy::all)]` to lib.rs.
- Changed edition from 2018 to 2021 in Cargo.toml.
- Made reader fields (`file`, `pos`) private in `LogFile`, `LogCoreReader`, `LogQueryReader`.
- Merged `read_line_trim` into same impl block as `open` in `LogQueryReader`.
- `detect_reader` propagates errors with `?` (removed `unwrap_or(0)`).
- Renamed `next_triable` → `try_next` in `FilteredLogIterator`.
- Changed `&Vec<Operation>` → `&[Operation]` in `print_if_branch`.
- Implemented `Display` for `Expression`.
- Removed unused `base_dir` field from `AppState` and `--dir` CLI arg.
- Removed dead `QueryResult` struct and `serde_derive::Serialize` import.
- Removed unused `matchesColumnFilters()` and `dateToIso()` JS functions.
- Removed unused `loadRecords()` replaced by `fetchPage()`.
- Upload handler no longer counts all records (saves ~6s on CORE.OUT).

## In Progress
- (none)

## Blocked
- (none)

## Key Decisions
- Used `position: fixed` + JS-calculated coordinates for the user dropdown to avoid clipping by the scrollable table container.
- Changed `process()` from generic type to `Box<dyn LogReader>` so `detect_reader()` returns either reader type dynamically.
- Reader auto-detection based on file content signature (first 512 bytes) rather than filename extension.
- View file auto-detection based on filename patterns ("reu" → view_reu.json, ".OUT" → view_core.json).
- Date filter uses ISO date comparison by converting the log's `dd/MM/yy` format to `yyyy-MM-dd`.
- Theme toggle uses `data-theme` attribute on `<html>` with CSS variables for both themes; icons represent the target.
- Upload returns only file path (no total) to avoid double-scanning large files.
- All filtering done server-side; frontend sends filter params to `/api/query`.
- Pagination is server-side; frontend requests page 0–1999, 2000–3999, etc.
- 2000 records per page (pageLimit).
- `setTimeout(20ms)` yield after `showSpinner()` ensures browser paints the spinner before blocking on upload/query.
- Spinner uses `position: fixed` with `z-index: 9998` (just below modal).

## Relevant Files
- `src/web.rs`: server routes + embedded frontend HTML/CSS/JS (all UI rendering, filter logic, SQL/XML formatting, column visibility, trigger filter, operation detection, spinner).
- `src/readers.rs`: `LogFile` (line-by-line), `LogQueryReader` (reu multi-line merger), `LogCoreReader` (tilde-delimited for CORE.OUT), `detect_reader()`.
- `src/process.rs`: `process()` takes `Box<dyn LogReader>`, `FilteredLogIterator`.
- `src/cli.rs`: CLI entry with per-file view auto-detection.
- `src/filters.rs`: regex patterns, View/Operation/Condition types, Display impls.
- `src/tests.rs`: unit tests.
- `src/lib.rs`: library root, `Record`, `Color` types.
- `Cargo.toml`: features (cli, web, json), edition 2021.
- `view_core.json`: regex view for CORE.OUT (15 `~`-delimited fields, 11 captured).
- `view_reu.json`: regex view for reu.out (CONTEXT fields + SQL).
- `examples/CORE.OUT`: ~382K-record delimited log file (156 MB, some records span 22+ lines).
- `examples/reu.out`: ~3.8K-query SQL trace log file (13 MB).
- `AGENTS.md`: this file.
