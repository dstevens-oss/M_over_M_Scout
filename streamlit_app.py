import streamlit as st
import pandas as pd
from io import BytesIO


# ---------- Helper functions ----------

def load_file(uploaded_file):
    file_name = uploaded_file.name
    ext = file_name.split(".")[-1].lower()

    if ext == "csv":
        return pd.read_csv(uploaded_file)
    elif ext in ["xlsx", "xls"]:
        return pd.read_excel(uploaded_file)
    else:
        raise ValueError(f"Unsupported file type: {file_name}")


def find_column_case_insensitive(df: pd.DataFrame, target: str):
    target_lower = target.lower()
    for col in df.columns:
        if col.lower() == target_lower:
            return col
    return None


def find_first_existing_column(df: pd.DataFrame, candidates):
    for name in candidates:
        col = find_column_case_insensitive(df, name)
        if col is not None:
            return col
    return None


def classify_file(df: pd.DataFrame):
    has_run = find_column_case_insensitive(df, "run timestamp") is not None
    has_created = find_column_case_insensitive(df, "Created Timestamp") is not None

    if has_run and not has_created:
        return "scan"
    elif has_created and not has_run:
        return "citations"
    elif has_run and has_created:
        return "scan"
    else:
        return "unknown"


def get_timestamp(df: pd.DataFrame, kind: str):
    if kind == "scan":
        col = find_column_case_insensitive(df, "run timestamp")
    elif kind == "citations":
        col = find_column_case_insensitive(df, "Created Timestamp")
    else:
        return pd.NaT

    if col is None:
        return pd.NaT

    series = df[col].dropna()
    if series.empty:
        return pd.NaT

    return pd.to_datetime(series.iloc[0], errors="coerce")


def format_date_label(ts: pd.Timestamp, add_citations: bool = False):
    if pd.isna(ts):
        base = "Unknown Date"
    else:
        base = f"{ts.strftime('%B')} {ts.day}"
    return f"{base} citations" if add_citations else base


def monthday_for_filename(ts: pd.Timestamp):
    if pd.isna(ts):
        return "unknown"
    return f"{ts.strftime('%b')}-{ts.day}"


def build_month_over_month_sheet(workbook, scan_infos):
    if len(scan_infos) != 2:
        return

    scan_infos_sorted = sorted(
        scan_infos, key=lambda info: (pd.isna(info["timestamp"]), info["timestamp"])
    )

    earlier_scan = scan_infos_sorted[0]
    later_scan = scan_infos_sorted[1]

    df_earlier = earlier_scan["df"]
    df_later = later_scan["df"]

    earlier_label = format_date_label(earlier_scan["timestamp"])
    later_label = format_date_label(later_scan["timestamp"])

    if df_earlier.shape[1] <= 6:
        return

    metric_columns = list(df_earlier.columns[6:])
    rows = []

    for col in metric_columns:
        metric_name = str(col)
        metric_type = "Benchmark" if "benchmark" in metric_name.lower() else "Metric"

        earlier_series = pd.to_numeric(df_earlier[col], errors="coerce")
        later_series = pd.to_numeric(df_later[col], errors="coerce")

        earlier_avg = earlier_series.mean()
        later_avg = later_series.mean()

        pct_change = None
        if not pd.isna(earlier_avg) and earlier_avg != 0:
            pct_change = (later_avg - earlier_avg) / earlier_avg

        rows.append((metric_type, metric_name, earlier_avg, later_avg, pct_change))

    ws = workbook.create_sheet(title="month over month", index=0)

    headers = ["Metric Type", "Metric Name", earlier_label, later_label, "Percent Change"]
    for col_idx, header in enumerate(headers, start=1):
        ws.cell(row=1, column=col_idx, value=header)

    for row_idx, (metric_type, metric_name, earlier_avg, later_avg, pct_change) in enumerate(rows, start=2):
        ws.cell(row=row_idx, column=1, value=metric_type)
        ws.cell(row=row_idx, column=2, value=metric_name)
        ws.cell(row=row_idx, column=3, value=None if pd.isna(earlier_avg) else float(round(earlier_avg, 2)))
        ws.cell(row=row_idx, column=4, value=None if pd.isna(later_avg) else float(round(later_avg, 2)))

        cell = ws.cell(row=row_idx, column=5)
        if pct_change is None:
            cell.value = "N/A"
        else:
            cell.value = float(pct_change)
            cell.number_format = "0.0%"


def normalize_rank_for_calc(value):
    if pd.isna(value):
        return 41.0
    s = str(value).strip().lower()
    if s in ["", "unranked", "nan"]:
        return 41.0
    try:
        return float(s)
    except ValueError:
        return 41.0


def build_performance_by_location_sheet(workbook, scan_infos):
    if len(scan_infos) != 2:
        return

    scan_infos_sorted = sorted(
        scan_infos, key=lambda info: (pd.isna(info["timestamp"]), info["timestamp"])
    )
    earlier_scan = scan_infos_sorted[0]
    later_scan = scan_infos_sorted[1]

    df_earlier = earlier_scan["df"].copy()
    df_later = later_scan["df"].copy()

    business_col_earlier = find_first_existing_column(
        df_earlier, ["Business Name", "Location Name", "Listing Name", "Name"]
    )
    address_col_earlier = find_first_existing_column(
        df_earlier, ["Address", "Full Address", "Street Address", "Address 1"]
    )

    business_col_later = find_first_existing_column(
        df_later, ["Business Name", "Location Name", "Listing Name", "Name"]
    )
    address_col_later = find_first_existing_column(
        df_later, ["Address", "Full Address", "Street Address", "Address 1"]
    )

    if not all([business_col_earlier, address_col_earlier, business_col_later, address_col_later]):
        ws = workbook.create_sheet(title="Performance by Location", index=0)
        ws.cell(row=1, column=1, value="Change")
        ws.cell(row=1, column=2, value="Listing Count")
        ws.cell(row=1, column=3, value="Avg Rank Improvement")
        ws.cell(row=1, column=4, value="Median Rank Improvement")
        ws.cell(row=2, column=1, value="Error")
        ws.cell(row=2, column=3, value="Missing Business/Address columns")
        return

    def make_key(df, bcol, acol):
        return df[bcol].astype(str).str.strip() + "||" + df[acol].astype(str).str.strip()

    df_earlier["_key"] = make_key(df_earlier, business_col_earlier, address_col_earlier)
    df_later["_key"] = make_key(df_later, business_col_later, address_col_later)

    rank_col_earlier = find_first_existing_column(df_earlier, ["Google Rank", "Rank"])
    rank_col_later = find_first_existing_column(df_later, ["Google Rank", "Rank"])

    if rank_col_earlier is None or rank_col_later is None:
        ws = workbook.create_sheet(title="Performance by Location", index=0)
        ws.cell(row=1, column=1, value="Change")
        ws.cell(row=1, column=2, value="Listing Count")
        ws.cell(row=1, column=3, value="Avg Rank Improvement")
        ws.cell(row=1, column=4, value="Median Rank Improvement")
        ws.cell(row=2, column=1, value="Error")
        ws.cell(row=2, column=3, value="Missing rank column")
        return

    df_e = df_earlier[["_key", rank_col_earlier]].rename(columns={rank_col_earlier: "rank_earlier"})
    df_l = df_later[["_key", rank_col_later]].rename(columns={rank_col_later: "rank_later"})
    merged = pd.merge(df_e, df_l, on="_key", how="outer")

    categories = ["Improved", "Declined", "No Change", "New Entry", "Removed"]
    counts = {cat: 0 for cat in categories}
    improvements = {cat: [] for cat in ["Improved", "Declined", "No Change"]}

    for _, row in merged.iterrows():
        has_e = not pd.isna(row.get("rank_earlier"))
        has_l = not pd.isna(row.get("rank_later"))

        if has_e and has_l:
            r_e = normalize_rank_for_calc(row["rank_earlier"])
            r_l = normalize_rank_for_calc(row["rank_later"])
            imp = r_e - r_l

            if r_l < r_e:
                cat = "Improved"
            elif r_l > r_e:
                cat = "Declined"
            else:
                cat = "No Change"

            counts[cat] += 1
            improvements[cat].append(imp)

        elif has_e and not has_l:
            counts["Removed"] += 1
        elif not has_e and has_l:
            counts["New Entry"] += 1

    ws = workbook.create_sheet(title="Performance by Location", index=0)
    ws.cell(row=1, column=1, value="Change")
    ws.cell(row=1, column=2, value="Listing Count")
    ws.cell(row=1, column=3, value="Avg Rank Improvement")
    ws.cell(row=1, column=4, value="Median Rank Improvement")

    row_idx = 2
    for cat in categories:
        ws.cell(row=row_idx, column=1, value=cat)
        ws.cell(row=row_idx, column=2, value=counts[cat])

        imp_list = improvements.get(cat, [])
        if imp_list:
            imp_series = pd.Series(imp_list)
            ws.cell(row=row_idx, column=3, value=float(round(imp_series.mean(), 2)))
            ws.cell(row=row_idx, column=4, value=float(round(imp_series.median(), 2)))
        else:
            ws.cell(row=row_idx, column=3, value="N/A")
            ws.cell(row=row_idx, column=4, value="N/A")

        row_idx += 1


# ---------- Main UI ----------

st.set_page_config(page_title="Month over Month Analysis", layout="wide")
st.title("Month over Month Analysis")

st.markdown(
    "Upload **2–4 files** (scan + optional citations), then click **Run Analysis**."
)

uploaded_files = st.file_uploader(
    "Upload files",
    type=["csv", "xlsx", "xls"],
    accept_multiple_files=True,
)

# Show uploaded file names
if uploaded_files:
    st.markdown("### Files selected:")
    for f in uploaded_files:
        st.write(f"- {f.name}")

run_clicked = st.button("Run Analysis")

output = None
download_filename = None

if run_clicked and uploaded_files:
    try:
        # Dancing penguin animation
        st.markdown(
            """
            <div style="text-align: center; margin: 30px 0;">
              <div class="penguin-dance">🐧</div>
              <div style="font-size: 18px; color: #666; margin-top: 10px;">
                Processing your analysis... hang tight!
              </div>
            </div>
            <style>
            @keyframes wobble {
              0%   { transform: translateX(-15px) rotate(-5deg); }
              50%  { transform: translateX(15px) rotate(5deg); }
              100% { transform: translateX(-15px) rotate(-5deg); }
            }
            .penguin-dance {
              font-size: 110px;
              animation: wobble 0.9s infinite ease-in-out;
              display: inline-block;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        # Load, classify, timestamp
        file_infos = []
        for uploaded_file in uploaded_files:
            df = load_file(uploaded_file)
            kind = classify_file(df)
            ts = get_timestamp(df, kind)
            sheet_name = format_date_label(ts, add_citations=(kind == "citations"))
            file_infos.append({
                "name": uploaded_file.name,
                "df": df,
                "kind": kind,
                "timestamp": ts,
                "sheet_name": sheet_name[:31],
            })

        scan_infos = [f for f in file_infos if f["kind"] == "scan"]
        citations_infos = [f for f in file_infos if f["kind"] == "citations"]

        scans_sorted = sorted(scan_infos, key=lambda x: x["timestamp"])
        citations_sorted = sorted(citations_infos, key=lambda x: x["timestamp"])

        # Filename
        earlier_ts = scans_sorted[0]["timestamp"]
        later_ts = scans_sorted[1]["timestamp"]
        download_filename = f"analysis_{monthday_for_filename(earlier_ts)}_to_{monthday_for_filename(later_ts)}.xlsx"

        # Build file
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:

            # Write scans
            for info in scans_sorted:
                info["df"].to_excel(writer, index=False, sheet_name=info["sheet_name"])

            # Write citations
            for info in citations_sorted:
                info["df"].to_excel(writer, index=False, sheet_name=info["sheet_name"])

            # Add analytics sheets
            build_month_over_month_sheet(writer.book, scans_sorted)
            build_performance_by_location_sheet(writer.book, scans_sorted)

        output.seek(0)
        st.success("Analysis complete! Scroll down to download.")

    except Exception as e:
        st.error(f"Error: {str(e)}")
        import traceback
        st.error(traceback.format_exc())


# Download button always at bottom
if output is not None:
    st.download_button(
        label="📥 Download Results",
        data=output.getvalue(),
        file_name=download_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
