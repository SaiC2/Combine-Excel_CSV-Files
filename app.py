import io
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


# =============================
# CSV read: PyArrow fast path with fallback + ALWAYS chunked
# =============================
def iter_csv_chunks(uploaded_file, csv_kwargs: dict, chunk_rows: int = 200_000):
    """
    Yield CSV chunks (DataFrames). Always uses chunksize for memory efficiency. [1](https://github.com/MicrosoftDocs/azure-ai-docs/blob/main/articles/ai-services/document-intelligence/quickstarts/get-started-studio.md)
    Tries engine='pyarrow' first, falls back to default engine if unsupported/unavailable. [1](https://github.com/MicrosoftDocs/azure-ai-docs/blob/main/articles/ai-services/document-intelligence/quickstarts/get-started-studio.md)
    """
    # Ensure we start from the beginning
    try:
        uploaded_file.seek(0)
    except Exception:
        pass

    # Try pyarrow engine first
    try:
        reader = pd.read_csv(
            uploaded_file,
            **csv_kwargs,
            engine="pyarrow",
            chunksize=chunk_rows,   # chunk iterator
        )
        for chunk in reader:
            yield chunk
        return
    except Exception:
        # fallback to pandas default engine
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

    reader = pd.read_csv(
        uploaded_file,
        **csv_kwargs,
        chunksize=chunk_rows,       # chunk iterator
        low_memory=True
    )
    for chunk in reader:
        yield chunk


def get_csv_columns(uploaded_file, csv_kwargs: dict) -> List[str]:
    """
    Read CSV header only (nrows=0) to get columns quickly.
    Uses PyArrow engine first, fallback if needed. [1](https://github.com/MicrosoftDocs/azure-ai-docs/blob/main/articles/ai-services/document-intelligence/quickstarts/get-started-studio.md)
    """
    try:
        uploaded_file.seek(0)
    except Exception:
        pass

    try:
        df0 = pd.read_csv(uploaded_file, **csv_kwargs, engine="pyarrow", nrows=0)
        return list(df0.columns)
    except Exception:
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
        df0 = pd.read_csv(uploaded_file, **csv_kwargs, nrows=0)
        return list(df0.columns)


# =============================
# Excel read (same as your original, plus header-only helper)
# =============================
def read_excel_file_all_sheets(uploaded_file) -> Dict[str, pd.DataFrame]:
    name = uploaded_file.name.lower()
    if name.endswith(".xlsx"):
        return pd.read_excel(uploaded_file, sheet_name=None, engine="openpyxl")
    elif name.endswith(".xls"):
        return pd.read_excel(uploaded_file, sheet_name=None, engine="xlrd")
    else:
        raise ValueError("Unsupported Excel file extension. Use .xlsx or .xls")


def get_excel_sheet_columns(uploaded_file, sheet_name: str) -> List[str]:
    """
    Read Excel header only (nrows=0) to get columns. pandas.read_excel supports nrows/usecols etc. [3](https://docs.streamlit.io/deploy/streamlit-community-cloud/share-your-app)
    """
    try:
        uploaded_file.seek(0)
    except Exception:
        pass

    name = uploaded_file.name.lower()
    engine = "openpyxl" if name.endswith(".xlsx") else "xlrd"
    df0 = pd.read_excel(uploaded_file, sheet_name=sheet_name, nrows=0, engine=engine)
    return list(df0.columns)


# =============================
# Source columns + alignment
# =============================
def add_source_columns_inplace(df: pd.DataFrame, source_file: str, source_sheet: Optional[str], add_sources: bool):
    if add_sources:
        df["Source_File"] = source_file
        if source_sheet is not None:
            df["Source_Sheet"] = source_sheet
    return df


def align_to_union(df: pd.DataFrame, union_cols: List[str]) -> pd.DataFrame:
    # reindex ensures missing columns are created and extra columns are dropped
    return df.reindex(columns=union_cols)


# =============================
# Session caching
# =============================
def build_signature(
    mode: str,
    add_sources: bool,
    delimiter: str,
    encoding: str,
    header_row: int,
    sheet_filter: Optional[List[str]],
    uploaded_files: List[Any],
    chunk_rows: int
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
# Streamlit UI
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
    # Always chunk CSVs: user asked no toggle
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
csv_kwargs = {
    "sep": "\t" if delimiter == r"\t" else delimiter,
    "header": int(header_row),
}
if encoding.strip():
    csv_kwargs["encoding"] = encoding.strip()

# Excel sheet selection
sheet_filter: Optional[List[str]] = None
if mode == "Combine Excel files (select sheets)":
    all_sheet_names = set()
    # discover sheets (as you had)
    for f in uploaded_files:
        try:
            if f.name.lower().endswith((".xlsx", ".xls")):
                d = read_excel_file_all_sheets(f)
                all_sheet_names.update(d.keys())
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

# Signature to reuse results across reruns (session-only)
signature = build_signature(
    mode=mode,
    add_sources=add_sources,
    delimiter=delimiter,
    encoding=encoding,
    header_row=int(header_row),
    sheet_filter=sheet_filter,
    uploaded_files=uploaded_files,
    chunk_rows=int(chunk_rows)
)

cached = get_cached(signature)

if cached and not run:
    # Show cached results immediately without re-reading
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

    if cached.get("parquet_bytes") is not None:
        st.download_button(
            "⬇️ Download Combined Parquet",
            data=cached["parquet_bytes"],
            file_name=f"{output_name}.parquet",
            mime="application/octet-stream"
        )

    st.stop()

if not run:
    st.info("Click **Combine now** to start processing.")
    st.stop()


# =============================
# Processing pipeline (two-pass union columns + streaming CSV write)
# =============================
progress = st.progress(0)
status_box = st.empty()

results: List[FileResult] = []
preview_rows: List[pd.DataFrame] = []
preview_target = 100
total_rows_written = 0

t0_total = time.perf_counter()

# ---- PASS 1: determine union columns
status_box.write("🔎 Scanning headers to determine unified column set…")
union_cols_set = set()

# If add_sources, ensure these exist in union
if add_sources:
    union_cols_set.add("Source_File")
    if mode != "Combine CSV files":
        # Source_Sheet only appears for Excel; safe to include anyway
        union_cols_set.add("Source_Sheet")

# Build union from headers
for f in uploaded_files:
    name = f.name
    kind = file_kind(name)
    try:
        if kind == "csv":
            cols = get_csv_columns(f, csv_kwargs)
            union_cols_set.update(cols)

        elif kind == "excel":
            # load sheet names (min work)
            sheets_dict = read_excel_file_all_sheets(f)
            included = list(sheets_dict.keys())
            if mode == "Combine Excel files (select sheets)" and sheet_filter is not None:
                included = [s for s in included if s in sheet_filter]

            # header-only read per selected sheet
            for s in included:
                cols = list(sheets_dict[s].columns)  # since we already loaded; keeps code simple
                union_cols_set.update(cols)

        else:
            continue
    except Exception:
        # ignore header failures, will be reported in pass 2
        pass

union_cols = list(union_cols_set)

# ---- PASS 2: stream rows and build CSV bytes
status_box.write("📥 Reading data and building combined output…")
out_bytes = io.BytesIO()
text_wrapper = io.TextIOWrapper(out_bytes, encoding="utf-8", newline="")

wrote_header = False

for idx, f in enumerate(uploaded_files, start=1):
    name = f.name
    size_b = getattr(f, "size", 0) or 0
    lower = name.lower()

    t0 = time.perf_counter()

    try:
        if lower.endswith(".csv"):
            # CSV: always chunked
            chunk_iter = iter_csv_chunks(f, csv_kwargs, chunk_rows=int(chunk_rows))
            file_rows = 0
            file_cols = 0

            for chunk in chunk_iter:
                chunk = add_source_columns_inplace(chunk, source_file=name, source_sheet=None, add_sources=add_sources)
                chunk = align_to_union(chunk, union_cols)

                # write header only once for whole combined file
                chunk.to_csv(text_wrapper, index=False, header=not wrote_header)
                wrote_header = True

                file_rows += len(chunk)
                file_cols = max(file_cols, chunk.shape[1])

                # collect preview
                if sum(len(p) for p in preview_rows) < preview_target:
                    needed = preview_target - sum(len(p) for p in preview_rows)
                    preview_rows.append(chunk.head(needed))

                total_rows_written += len(chunk)

            t1 = time.perf_counter()
            results.append(FileResult(
                file_name=name, status="success",
                rows=file_rows, cols=file_cols, sheets=None,
                error=None, tb=None,
                read_seconds=t1 - t0, size_bytes=size_b
            ))

        elif lower.endswith((".xlsx", ".xls")):
            # Excel: read as before (sheet_name=None -> dict). Could be heavy for huge xlsx, but out of scope for this tweak.
            sheets_dict = read_excel_file_all_sheets(f)
            included = list(sheets_dict.keys())
            if mode == "Combine Excel files (select sheets)" and sheet_filter is not None:
                included = [s for s in included if s in sheet_filter]

            file_rows = 0
            file_cols = 0

            for sheet_name in included:
                sdf = sheets_dict[sheet_name]
                sdf = add_source_columns_inplace(sdf, source_file=name, source_sheet=sheet_name, add_sources=add_sources)
                sdf = align_to_union(sdf, union_cols)

                sdf.to_csv(text_wrapper, index=False, header=not wrote_header)
                wrote_header = True

                file_rows += len(sdf)
                file_cols = max(file_cols, sdf.shape[1])

                if sum(len(p) for p in preview_rows) < preview_target:
                    needed = preview_target - sum(len(p) for p in preview_rows)
                    preview_rows.append(sdf.head(needed))

                total_rows_written += len(sdf)

            t1 = time.perf_counter()
            results.append(FileResult(
                file_name=name, status="success",
                rows=file_rows, cols=file_cols, sheets=included,
                error=None, tb=None,
                read_seconds=t1 - t0, size_bytes=size_b
            ))

        else:
            raise ValueError(f"Unsupported file type for '{name}'")

    except Exception as e:
        t1 = time.perf_counter()
        tb = traceback.format_exc()
        results.append(FileResult(
            file_name=name, status="failed",
            rows=0, cols=0, sheets=None,
            error=str(e), tb=tb,
            read_seconds=t1 - t0, size_bytes=size_b
        ))

    # progress update
    progress.progress(int(idx / len(uploaded_files) * 100))
    status_box.write(f"Processed {idx}/{len(uploaded_files)}: **{name}**")

# finalize output
text_wrapper.flush()
csv_bytes = out_bytes.getvalue()

# Build preview df
preview_df = pd.concat(preview_rows, ignore_index=True) if preview_rows else pd.DataFrame()

# Summary DF
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

t1_total = time.perf_counter()
total_seconds = t1_total - t0_total

shape_text = f"**Combined rows (written):** {total_rows_written:,} | **Columns (union):** {len(union_cols):,} | **Total time:** {total_seconds:.2f}s"

# Optional parquet (only if GL workflow)
parquet_bytes = None
if mode == "GL workflow: Combine → Download Parquet + CSV (for Row Zero import)":
    try:
        # Convert preview_df? No—convert full dataset would require rebuilding as DataFrame.
        # For this lightweight tweak set, we skip parquet in streaming mode.
        # If you want true parquet output, we can implement a parquet writer stream separately.
        st.info("Parquet output is disabled in streaming mode for now (can be added if you want).")
        parquet_bytes = None
    except Exception:
        parquet_bytes = None

# Cache results in THIS browser session (session_state), so UI tweaks don't re-read.
set_cached(signature, {
    "summary_df": summary_df,
    "preview_df": preview_df,
    "csv_bytes": csv_bytes,
    "parquet_bytes": parquet_bytes,
    "shape_text": shape_text
})

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