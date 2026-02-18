import io
import csv
import time
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import streamlit as st


# =============================
# Data structures
# =============================
@dataclass
class FileResult:
    file_name: str
    status: str  # "success" or "failed"
    rows: int = 0
    cols: int = 0
    sheets: Optional[List[str]] = None
    error: Optional[str] = None
    tb: Optional[str] = None
    read_seconds: float = 0.0
    size_bytes: int = 0


# =============================
# Helpers
# =============================
def mb(x_bytes: int) -> float:
    return x_bytes / (1024 * 1024)


def format_seconds(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def file_kind(name: str) -> str:
    lower = name.lower()
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith((".xlsx", ".xls")):
        return "excel"
    return ""


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def ema_update(prev: Optional[float], x: float, alpha: float = 0.25) -> float:
    return x if prev is None else (alpha * x + (1 - alpha) * prev)


# =============================
# Session caching (per browser session)
# =============================
def build_signature(
    mode: str,
    add_sources: bool,
    delimiter: str,
    encoding: str,
    header_row: int,
    sheet_filter: Optional[List[str]],
    uploaded_files: List[Any],
    chunk_rows: int,
) -> Tuple:
    file_sig = tuple(sorted((f.name, getattr(f, "size", 0) or 0) for f in uploaded_files))
    sheet_sig = tuple(sheet_filter) if sheet_filter else ()
    return (mode, add_sources, delimiter, encoding, header_row, sheet_sig, file_sig, chunk_rows)


def get_cached(signature: Tuple):
    cache = st.session_state.get("_combine_cache", {})
    return cache.get(signature)


def set_cached(signature: Tuple, value: dict):
    if "_combine_cache" not in st.session_state:
        st.session_state["_combine_cache"] = {}
    st.session_state["_combine_cache"][signature] = value


# =============================
# FAST header readers (Pass 1)
# =============================
def get_csv_columns_fast(uploaded_file, sep: str, encoding: str = "utf-8") -> List[str]:
    """Fast CSV header read using Python csv module (no pandas)."""
    try:
        uploaded_file.seek(0)
    except Exception:
        pass

    text = io.TextIOWrapper(uploaded_file, encoding=encoding or "utf-8", newline="")
    try:
        reader = csv.reader(text, delimiter=sep)
        header = next(reader, [])
        return [h.strip() for h in header]
    finally:
        try:
            text.detach()
        except Exception:
            pass


def open_excelfile(uploaded_file) -> pd.ExcelFile:
    """Open ExcelFile once (fast sheet names + parse)."""
    try:
        uploaded_file.seek(0)
    except Exception:
        pass
    name = uploaded_file.name.lower()
    engine = "openpyxl" if name.endswith(".xlsx") else "xlrd"
    return pd.ExcelFile(uploaded_file, engine=engine)


def get_excel_columns_header_only(xf: pd.ExcelFile, sheet_name: str) -> List[str]:
    """Read only header row from an Excel sheet (nrows=0)."""
    df0 = xf.parse(sheet_name=sheet_name, nrows=0)
    return list(df0.columns)


# =============================
# CSV read: PyArrow fast path with fallback + ALWAYS chunked (Pass 2)
# =============================
def iter_csv_chunks(uploaded_file, csv_kwargs: dict, chunk_rows: int = 200_000):
    """
    Always yields chunks via chunksize (memory-friendly). pandas.read_csv supports chunksize. [3](https://github.com/MicrosoftDocs/azure-ai-docs/blob/main/articles/ai-services/document-intelligence/quickstarts/get-started-studio.md)
    Tries engine='pyarrow' first, falls back if unsupported/unavailable (engine param). [3](https://github.com/MicrosoftDocs/azure-ai-docs/blob/main/articles/ai-services/document-intelligence/quickstarts/get-started-studio.md)
    """
    try:
        uploaded_file.seek(0)
    except Exception:
        pass

    # Try PyArrow engine
    try:
        reader = pd.read_csv(
            uploaded_file,
            **csv_kwargs,
            engine="pyarrow",
            chunksize=chunk_rows,
        )
        for chunk in reader:
            yield chunk
        return
    except Exception:
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

    # Fallback engine
    reader = pd.read_csv(
        uploaded_file,
        **csv_kwargs,
        chunksize=chunk_rows,
        low_memory=True
    )
    for chunk in reader:
        yield chunk


# =============================
# Transform helpers
# =============================
def add_source_columns(df: pd.DataFrame, source_file: str, source_sheet: Optional[str], add_sources: bool) -> pd.DataFrame:
    if add_sources:
        df["Source_File"] = source_file
        if source_sheet is not None:
            df["Source_Sheet"] = source_sheet
    return df


def align_to_union(df: pd.DataFrame, union_cols: List[str]) -> pd.DataFrame:
    return df.reindex(columns=union_cols)


# =============================
# UI
# =============================
st.set_page_config(page_title="CSV/Excel Combiner", layout="wide")
st.title("🧩 CSV / Excel Combiner → Download Combined CSV")

with st.sidebar:
    st.header("📌 Mode")
    mode = st.selectbox(
        "Choose functionality",
        [
            "Combine CSV files",
            "Combine Excel files (all sheets)",
            "Combine Excel files (select sheets)",
            "Combine Mixed (CSV + Excel)",
            "GL workflow: Combine → Download CSV (for Row Zero import)",
        ],
    )

    st.header("⚙️ Options")
    debug_mode = st.checkbox("Debug mode (show tracebacks)", value=True)
    add_sources = st.checkbox("Add Source_File / Source_Sheet columns", value=True)

    st.subheader("CSV options")
    delimiter = st.text_input("Delimiter", value=",", help="Comma is default. Use \\t for tab.")
    encoding = st.text_input("Encoding (optional)", value="", help="Leave blank for default.")
    header_row = st.number_input("Header row index", min_value=0, value=0, step=1)

    st.subheader("Performance")
    chunk_rows = st.number_input("CSV chunk rows", min_value=50_000, value=200_000, step=50_000)

    st.subheader("Output")
    output_name = st.text_input("Output base name", value="combined_data")

# File types
if mode == "Combine CSV files":
    file_types = ["csv"]
elif mode.startswith("Combine Excel"):
    file_types = ["xlsx", "xls"]
else:
    file_types = ["csv", "xlsx", "xls"]

uploaded_files = st.file_uploader("Upload files", type=file_types, accept_multiple_files=True)
if not uploaded_files:
    st.info("Upload one or more files to begin.")
    st.stop()

total_bytes = sum(getattr(f, "size", 0) or 0 for f in uploaded_files)
st.caption(f"Total upload size: **{mb(total_bytes):.2f} MB** across **{len(uploaded_files)}** file(s).")

# CSV kwargs
sep = "\t" if delimiter == r"\t" else delimiter
csv_kwargs = {"sep": sep, "header": int(header_row)}
if encoding.strip():
    csv_kwargs["encoding"] = encoding.strip()

# Excel sheet selection (select-sheets mode)
sheet_filter: Optional[List[str]] = None
if mode == "Combine Excel files (select sheets)":
    with st.spinner("Discovering sheet names from uploaded Excel files…"):
        all_sheet_names = set()
        for f in uploaded_files:
            if file_kind(f.name) != "excel":
                continue
            try:
                xf = open_excelfile(f)
                all_sheet_names.update(xf.sheet_names)
            except Exception:
                pass

    sheet_filter = st.multiselect(
        "Select sheet(s) to include (across all Excel uploads)",
        options=sorted(all_sheet_names),
        default=sorted(all_sheet_names)[:1] if all_sheet_names else []
    )
    if not sheet_filter:
        st.warning("Select at least one sheet to include.")
        st.stop()

    for f in uploaded_files:
        try:
            f.seek(0)
        except Exception:
            pass

# Combine action
run = st.button("🚀 Combine now")

signature = build_signature(
    mode=mode,
    add_sources=add_sources,
    delimiter=delimiter,
    encoding=encoding,
    header_row=int(header_row),
    sheet_filter=sheet_filter,
    uploaded_files=uploaded_files,
    chunk_rows=int(chunk_rows),
)

cached = get_cached(signature)

# Cached output (no re-read on UI tweaks). Streamlit reruns scripts often; session caching avoids repeating heavy work. [1](https://docs.celigo.com/hc/en-us/articles/15482613370907-Set-up-a-connection-to-Azure-AI-Document-Intelligence)
if cached and not run:
    st.success("Using cached results from this browser session (no re-read).")
    st.subheader("✅ Processing Summary")
    st.dataframe(cached["summary_df"], use_container_width=True)

    st.subheader("📌 Combined Output")
    st.write(cached["shape_text"])
    with st.expander("Preview (first 100 rows)"):
        st.dataframe(cached["preview_df"], use_container_width=True)

    st.download_button(
        "⬇️ Download Combined CSV",
        data=cached["csv_bytes"],
        file_name=f"{output_name}.csv",
        mime="text/csv"
    )

    st.subheader("📤 Import to Row Zero")
    st.markdown("👉 **https://rowzero.com/import**")
    st.stop()

if not run:
    st.info("Click **Combine now** to start processing.")
    st.stop()


# =============================
# Processing pipeline with per-phase progress %
# =============================
overall_box = st.empty()
status_box = st.empty()

# Phase progress widgets
phase1_box = st.container()
phase2_box = st.container()
phase3_box = st.container()

t0_total = time.perf_counter()

# -----------------------------
# Phase 1: Header scan progress %
# -----------------------------
with phase1_box:
    st.subheader("Phase 1/3: 🔎 Header scan")
    p1_bar = st.progress(0)
    p1_text = st.empty()

status_box.write("🔎 Scanning headers to determine unified column set…")

t_pass1_start = time.perf_counter()
ema_unit_seconds = None
alpha = 0.25

union_cols_set = set()
if add_sources:
    union_cols_set.add("Source_File")
    union_cols_set.add("Source_Sheet")

units = []  # ("csv", file) or ("excel_sheet", file, sheet)
excel_xf_cache: Dict[str, pd.ExcelFile] = {}
enc = encoding.strip() or "utf-8"

# Build Pass1 units
for f in uploaded_files:
    kind = file_kind(f.name)
    if kind == "csv":
        units.append(("csv", f))
    elif kind == "excel":
        try:
            xf = open_excelfile(f)
            excel_xf_cache[f.name] = xf
            included = xf.sheet_names
            if mode == "Combine Excel files (select sheets)" and sheet_filter is not None:
                included = [s for s in included if s in sheet_filter]
            for s in included:
                units.append(("excel_sheet", f, s))
        except Exception:
            continue

total_units = max(1, len(units))
done_units = 0

for unit in units:
    u0 = time.perf_counter()
    try:
        if unit[0] == "csv":
            cols = get_csv_columns_fast(unit[1], sep=sep, encoding=enc)
            union_cols_set.update(cols)
        else:
            f, sheet_name = unit[1], unit[2]
            xf = excel_xf_cache.get(f.name)
            if xf is None:
                xf = open_excelfile(f)
                excel_xf_cache[f.name] = xf
            cols = get_excel_columns_header_only(xf, sheet_name)
            union_cols_set.update(cols)
    except Exception:
        pass

    u1 = time.perf_counter()
    ema_unit_seconds = ema_update(ema_unit_seconds, u1 - u0, alpha=alpha)

    done_units += 1
    pct1 = int(done_units / total_units * 100)
    p1_bar.progress(pct1)

    remaining_units = total_units - done_units
    eta1 = (ema_unit_seconds or 0) * remaining_units

    p1_text.write(
        f"📊 **Phase 1 Progress:** {pct1}% "
        f"({done_units}/{total_units} header units) | "
        f"ETA: {format_seconds(eta1)}"
    )

union_cols = list(union_cols_set)
t_pass1_end = time.perf_counter()
p1_bar.progress(100)
p1_text.write(
    f"✅ **Phase 1 Complete:** 100% | "
    f"Time: {format_seconds(t_pass1_end - t_pass1_start)} | "
    f"Union columns: {len(union_cols)}"
)

# -----------------------------
# Phase 2: Read + build output progress % (70% CSV / 30% Excel)
# -----------------------------
with phase2_box:
    st.subheader("Phase 2/3: 📥 Read + build combined output")
    p2_bar = st.progress(0)
    p2_text = st.empty()

status_box.write("📥 Reading data and building combined output…")

t_pass2_start = time.perf_counter()

# Totals for Phase2 progress
total_csv_bytes = sum((getattr(f, "size", 0) or 0) for f in uploaded_files if file_kind(f.name) == "csv")
processed_csv_bytes = 0

# Excel sheet count total
excel_work = []
for f in uploaded_files:
    if file_kind(f.name) != "excel":
        continue
    try:
        xf = excel_xf_cache.get(f.name)
        if xf is None:
            xf = open_excelfile(f)
            excel_xf_cache[f.name] = xf
        included = xf.sheet_names
        if mode == "Combine Excel files (select sheets)" and sheet_filter is not None:
            included = [s for s in included if s in sheet_filter]
        for s in included:
            excel_work.append((f, s))
    except Exception:
        pass

total_excel_sheets = len(excel_work)
done_excel_sheets = 0

# EMA for ETA (optional)
ema_csv_mbps = None
ema_excel_sheet_sec = None

# Output stream
out_bytes = io.BytesIO()
text_wrapper = io.TextIOWrapper(out_bytes, encoding="utf-8", newline="")
wrote_header = False

results: List[FileResult] = []
preview_rows: List[pd.DataFrame] = []
preview_target = 100
total_rows_written = 0

def update_phase2_progress_and_eta():
    # ratios
    csv_ratio = (processed_csv_bytes / total_csv_bytes) if total_csv_bytes > 0 else 1.0
    excel_ratio = (done_excel_sheets / total_excel_sheets) if total_excel_sheets > 0 else 1.0

    # 70% CSV / 30% Excel
    combined_ratio = 0.7 * csv_ratio + 0.3 * excel_ratio
    pct2 = int(clamp01(combined_ratio) * 100)

    p2_bar.progress(pct2)

    # optional ETA
    # fallbacks until EMA exists
    csv_speed = ema_csv_mbps or 2.0  # MB/s fallback
    excel_speed = ema_excel_sheet_sec or 2.0  # sec/sheet fallback

    rem_csv = max(0, total_csv_bytes - processed_csv_bytes)
    rem_excel = max(0, total_excel_sheets - done_excel_sheets)

    eta_csv = (mb(rem_csv) / max(csv_speed, 1e-6)) if total_csv_bytes > 0 else 0.0
    eta_excel = rem_excel * excel_speed
    eta2 = eta_csv + eta_excel

    p2_text.write(
        f"📊 **Phase 2 Progress:** {pct2}% "
        f"(CSV: {int(clamp01(csv_ratio)*100)}%, Excel: {int(clamp01(excel_ratio)*100)}%) | "
        f"ETA: {format_seconds(eta2)}"
    )

# Pass2 processing
for f in uploaded_files:
    name = f.name
    size_b = getattr(f, "size", 0) or 0
    lower = name.lower()
    t0 = time.perf_counter()

    try:
        if lower.endswith(".csv"):
            file_rows = 0
            file_cols = 0

            # We'll count bytes read per-file using file pointer if available.
            # If tell() doesn't advance reliably, we'll add the remainder at end of file.
            file_bytes_counted = 0
            try:
                f.seek(0)
                last_pos = f.tell()
            except Exception:
                last_pos = None

            for chunk in iter_csv_chunks(f, csv_kwargs, chunk_rows=int(chunk_rows)):
                # byte progress update
                if last_pos is not None:
                    try:
                        cur_pos = f.tell()
                        if cur_pos is not None and cur_pos >= last_pos:
                            incr = cur_pos - last_pos
                            last_pos = cur_pos
                            incr = max(0, incr)
                            file_bytes_counted += incr
                            processed_csv_bytes = min(total_csv_bytes, processed_csv_bytes + incr)

                            # update mbps EMA based on time delta
                            dt = max(1e-6, time.perf_counter() - t0)
                            mbps = mb(incr) / dt
                            ema_csv_mbps = ema_update(ema_csv_mbps, mbps, alpha=0.20)
                            t0 = time.perf_counter()
                    except Exception:
                        pass

                chunk = add_source_columns(chunk, source_file=name, source_sheet=None, add_sources=add_sources)
                chunk = align_to_union(chunk, union_cols)

                chunk.to_csv(text_wrapper, index=False, header=not wrote_header)
                wrote_header = True

                file_rows += len(chunk)
                file_cols = max(file_cols, chunk.shape[1])
                total_rows_written += len(chunk)

                if sum(len(p) for p in preview_rows) < preview_target:
                    needed = preview_target - sum(len(p) for p in preview_rows)
                    preview_rows.append(chunk.head(needed))

                update_phase2_progress_and_eta()

            # ensure we account for file bytes even if tell() was unreliable
            remaining_for_file = max(0, size_b - file_bytes_counted)
            if total_csv_bytes > 0 and remaining_for_file > 0:
                processed_csv_bytes = min(total_csv_bytes, processed_csv_bytes + remaining_for_file)

            update_phase2_progress_and_eta()

            t1 = time.perf_counter()
            results.append(FileResult(
                file_name=name,
                status="success",
                rows=file_rows,
                cols=file_cols,
                sheets=None,
                error=None,
                tb=None,
                read_seconds=t1 - t0,
                size_bytes=size_b
            ))

        elif lower.endswith((".xlsx", ".xls")):
            xf = excel_xf_cache.get(name)
            if xf is None:
                xf = open_excelfile(f)
                excel_xf_cache[name] = xf

            included = xf.sheet_names
            if mode == "Combine Excel files (select sheets)" and sheet_filter is not None:
                included = [s for s in included if s in sheet_filter]

            file_rows = 0
            file_cols = 0

            for sheet_name in included:
                s0 = time.perf_counter()
                sdf = xf.parse(sheet_name=sheet_name)  # full sheet (selected only)
                s1 = time.perf_counter()

                done_excel_sheets += 1
                ema_excel_sheet_sec = ema_update(ema_excel_sheet_sec, s1 - s0, alpha=0.25)

                sdf = add_source_columns(sdf, source_file=name, source_sheet=sheet_name, add_sources=add_sources)
                sdf = align_to_union(sdf, union_cols)

                sdf.to_csv(text_wrapper, index=False, header=not wrote_header)
                wrote_header = True

                file_rows += len(sdf)
                file_cols = max(file_cols, sdf.shape[1])
                total_rows_written += len(sdf)

                if sum(len(p) for p in preview_rows) < preview_target:
                    needed = preview_target - sum(len(p) for p in preview_rows)
                    preview_rows.append(sdf.head(needed))

                update_phase2_progress_and_eta()

            t1 = time.perf_counter()
            results.append(FileResult(
                file_name=name,
                status="success",
                rows=file_rows,
                cols=file_cols,
                sheets=included,
                error=None,
                tb=None,
                read_seconds=t1 - t0,
                size_bytes=size_b
            ))

        else:
            raise ValueError(f"Unsupported file type for '{name}'")

    except Exception as e:
        t1 = time.perf_counter()
        tb = traceback.format_exc()
        results.append(FileResult(
            file_name=name,
            status="failed",
            rows=0,
            cols=0,
            sheets=None,
            error=str(e),
            tb=tb,
            read_seconds=t1 - t0,
            size_bytes=size_b
        ))

    # keep user reassured that things are running
    status_box.write(f"Working on: **{name}**")

text_wrapper.flush()
csv_bytes = out_bytes.getvalue()

t_pass2_end = time.perf_counter()
p2_bar.progress(100)
p2_text.write(
    f"✅ **Phase 2 Complete:** 100% | "
    f"Time: {format_seconds(t_pass2_end - t_pass2_start)}"
)

# -----------------------------
# Phase 3: Finalize progress % (simple steps)
# -----------------------------
with phase3_box:
    st.subheader("Phase 3/3: 🧾 Finalize")
    p3_bar = st.progress(0)
    p3_text = st.empty()

t_pass3_start = time.perf_counter()

p3_text.write("📊 **Phase 3 Progress:** 0% (Building preview)")
p3_bar.progress(0)

preview_df = pd.concat(preview_rows, ignore_index=True) if preview_rows else pd.DataFrame()
p3_text.write("📊 **Phase 3 Progress:** 33% (Building summary)")
p3_bar.progress(33)

summary_df = pd.DataFrame([{
    "File": r.file_name,
    "Status": r.status,
    "Size (MB)": f"{mb(r.size_bytes):.2f}",
    "Read time (s)": f"{r.read_seconds:.2f}",
    "Rows": r.rows,
    "Cols": r.cols,
    "Sheets (Excel)": ", ".join(r.sheets) if r.sheets else "",
    "Error": r.error or ""
} for r in results])

p3_text.write("📊 **Phase 3 Progress:** 66% (Caching + finalizing)")
p3_bar.progress(66)

t1_total = time.perf_counter()
total_seconds = t1_total - t0_total

shape_text = (
    f"**Combined rows (written):** {total_rows_written:,} | "
    f"**Columns (union):** {len(union_cols):,} | "
    f"**Total time:** {total_seconds:.2f}s"
)

# Cache results (session-only) so UI tweaks don't re-read
set_cached(signature, {
    "summary_df": summary_df,
    "preview_df": preview_df,
    "csv_bytes": csv_bytes,
    "shape_text": shape_text
})

p3_text.write("📊 **Phase 3 Progress:** 100% (Complete)")
p3_bar.progress(100)

t_pass3_end = time.perf_counter()

overall_box.success(
    f"🎉 Completed. Total runtime: {format_seconds(total_seconds)} | "
    f"Phase1: {format_seconds(t_pass1_end - t_pass1_start)}, "
    f"Phase2: {format_seconds(t_pass2_end - t_pass2_start)}, "
    f"Phase3: {format_seconds(t_pass3_end - t_pass3_start)}"
)

# =============================
# Output UI
# =============================
st.success("✅ Completed. Results cached for this session (UI changes won't re-read).")

st.subheader("✅ Processing Summary")
st.dataframe(summary_df, use_container_width=True)

if debug_mode:
    failed = [r for r in results if r.status == "failed"]
    if failed:
        st.subheader("🧪 Debug")
        for r in failed:
            with st.expander(f"Error details: {r.file_name}"):
                st.write("**Error:**", r.error)
                st.code(r.tb or "", language="text")

st.subheader("📌 Combined Output")
st.write(shape_text)

with st.expander("Preview (first 100 rows)"):
    st.dataframe(preview_df, use_container_width=True)

st.download_button(
    "⬇️ Download Combined CSV",
    data=csv_bytes,
    file_name=f"{output_name}.csv",
    mime="text/csv"
)

st.subheader("📤 Import to Row Zero")
st.markdown("👉 **https://rowzero.com/import**")