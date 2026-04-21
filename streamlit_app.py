from __future__ import annotations

from io import BytesIO
from pathlib import Path
import re

import pandas as pd
import streamlit as st


GENERIC_DOMAINS_PATH = Path(__file__).parent / "config" / "generic_email_domains.txt"
EMAIL_PATTERN = re.compile(r"^[A-Z0-9._%+\-']+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)
SEGMENT_KEYWORDS = (
    "subscriber",
    "registered",
    "registration",
    "segment",
    "status",
    "user type",
    "membership",
    "entitlement",
    "account type",
    "customer type",
)
DERIVED_SEGMENT_COLUMN = "Derived User Segment"


def load_generic_domains(path: Path) -> list[str]:
    if not path.exists():
        return []

    domains = []
    for line in path.read_text().splitlines():
        value = line.strip().lower()
        if not value or value.startswith("#"):
            continue
        domains.append(value.lstrip("@"))
    return sorted(set(domains))


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [str(col).strip() for col in cleaned.columns]
    return cleaned


def load_uploaded_file(uploaded_file) -> pd.DataFrame:
    suffix = Path(uploaded_file.name).suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(uploaded_file)
        return normalise_columns(df)

    if suffix in {".xlsx", ".xlsm", ".xls"}:
        sheets = pd.read_excel(uploaded_file, sheet_name=None)
        frames = []
        for sheet_name, sheet_df in sheets.items():
            cleaned = normalise_columns(sheet_df)
            cleaned["Source Sheet"] = sheet_name
            frames.append(cleaned)
        return pd.concat(frames, ignore_index=True)

    raise ValueError("Unsupported file type. Upload a CSV or Excel workbook.")


def detect_email_column(df: pd.DataFrame) -> str | None:
    by_name = [
        col
        for col in df.columns
        if "email" in str(col).strip().lower() or str(col).strip().lower() == "mail"
    ]
    if by_name:
        return by_name[0]

    best_col = None
    best_score = 0.0
    for col in df.columns:
        sample = df[col].dropna().astype(str).str.strip().head(200)
        if sample.empty:
            continue
        score = sample.str.match(EMAIL_PATTERN).mean()
        if score > best_score:
            best_score = score
            best_col = col
    return best_col if best_score >= 0.5 else None


def suggest_segment_column(df: pd.DataFrame) -> str | None:
    if DERIVED_SEGMENT_COLUMN in df.columns:
        return DERIVED_SEGMENT_COLUMN

    for col in df.columns:
        name = str(col).strip().lower()
        if any(keyword in name for keyword in SEGMENT_KEYWORDS):
            return col
    return "Source Sheet" if "Source Sheet" in df.columns else None


def normalise_email_series(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": "", "none": ""})
    )


def is_valid_email(value: str) -> bool:
    return bool(EMAIL_PATTERN.match(value))


def extract_domain(email: str) -> str:
    if "@" not in email:
        return ""
    return email.rsplit("@", 1)[1].lower()


def is_generic_domain(domain: str, generic_domains: set[str]) -> bool:
    return any(domain == generic or domain.endswith(f".{generic}") for generic in generic_domains)


def build_segmented_data(df: pd.DataFrame, segment_column: str | None) -> dict[str, pd.DataFrame]:
    if segment_column and segment_column in df.columns:
        working = df.copy()
        working[segment_column] = working[segment_column].fillna("Unknown").astype(str).str.strip()
        groups = {
            segment or "Unknown": segment_df.copy()
            for segment, segment_df in working.groupby(segment_column, dropna=False)
        }
    else:
        groups = {"All Users": df.copy()}

    groups = {"All Users": df.copy(), **groups}
    return groups


def reorder_segment_names(segment_names: list[str]) -> list[str]:
    priority = {
        "subscriber": 0,
        "subscribers": 0,
        "registered user": 1,
        "registered users": 1,
        "registered": 1,
        "registration": 1,
        "all users": 2,
    }
    return sorted(
        segment_names,
        key=lambda name: (priority.get(str(name).strip().lower(), 3), str(name).strip().lower()),
    )


def derive_user_segments(df: pd.DataFrame) -> pd.DataFrame:
    access_count_column = next(
        (col for col in df.columns if str(col).strip().lower() == "access count"),
        None,
    )
    if access_count_column is None:
        return df

    working = df.copy()
    access_counts = pd.to_numeric(working[access_count_column], errors="coerce")
    working[DERIVED_SEGMENT_COLUMN] = access_counts.map(
        lambda value: "Subscriber" if pd.notna(value) and value > 1 else "Registered User"
    )
    return working


def analyse_segment(df: pd.DataFrame, email_column: str, generic_domains: set[str]) -> dict[str, pd.DataFrame | int]:
    working = df.copy()
    working["Email Address"] = normalise_email_series(working[email_column])
    working["Is Valid Email"] = working["Email Address"].map(is_valid_email)
    working["Email Domain"] = working["Email Address"].map(extract_domain)
    working["Is Generic Domain"] = working["Email Domain"].map(
        lambda domain: is_generic_domain(domain, generic_domains) if domain else False
    )

    corporate_rows = working[working["Is Valid Email"] & ~working["Is Generic Domain"]].copy()

    domain_summary = (
        corporate_rows.groupby("Email Domain", dropna=False)
        .agg(
            Instances=("Email Address", "size"),
            Unique_Emails=("Email Address", "nunique"),
            Sample_Emails=("Email Address", lambda values: ", ".join(sorted(pd.unique(values))[:5])),
        )
        .reset_index()
        .sort_values(["Instances", "Unique_Emails", "Email Domain"], ascending=[False, False, True])
    )

    email_summary = (
        corporate_rows.groupby(["Email Address", "Email Domain"], dropna=False)
        .size()
        .reset_index(name="Instances")
        .sort_values(["Instances", "Email Domain", "Email Address"], ascending=[False, True, True])
    )

    summary = pd.DataFrame(
        [
            {
                "Total rows": len(working),
                "Valid emails": int(working["Is Valid Email"].sum()),
                "Corporate email rows": len(corporate_rows),
                "Unique corporate domains": int(domain_summary["Email Domain"].nunique()),
                "Unique corporate emails": int(corporate_rows["Email Address"].nunique()),
            }
        ]
    )

    return {
        "summary": summary,
        "domain_summary": domain_summary,
        "email_summary": email_summary,
        "corporate_rows": corporate_rows,
    }


def filter_result_by_domain(
    result: dict[str, pd.DataFrame | int],
    predicate,
) -> dict[str, pd.DataFrame | int]:
    corporate_rows = result["corporate_rows"]
    filtered_rows = corporate_rows[corporate_rows["Email Domain"].map(predicate)].copy()

    domain_summary = (
        filtered_rows.groupby("Email Domain", dropna=False)
        .agg(
            Instances=("Email Address", "size"),
            Unique_Emails=("Email Address", "nunique"),
            Sample_Emails=("Email Address", lambda values: ", ".join(sorted(pd.unique(values))[:5])),
        )
        .reset_index()
        .sort_values(["Instances", "Unique_Emails", "Email Domain"], ascending=[False, False, True])
    )

    email_summary = (
        filtered_rows.groupby(["Email Address", "Email Domain"], dropna=False)
        .size()
        .reset_index(name="Instances")
        .sort_values(["Instances", "Email Domain", "Email Address"], ascending=[False, True, True])
    )

    summary = pd.DataFrame(
        [
            {
                "Total rows": int(result["summary"].iloc[0]["Total rows"]),
                "Valid emails": int(result["summary"].iloc[0]["Valid emails"]),
                "Corporate email rows": len(filtered_rows),
                "Unique corporate domains": int(domain_summary["Email Domain"].nunique()),
                "Unique corporate emails": int(filtered_rows["Email Address"].nunique()),
            }
        ]
    )

    return {
        "summary": summary,
        "domain_summary": domain_summary,
        "email_summary": email_summary,
        "corporate_rows": filtered_rows,
    }


def build_special_results(results: dict[str, dict[str, pd.DataFrame | int]]) -> dict[str, dict[str, pd.DataFrame | int]]:
    segment_lookup = {str(name).strip().lower(): name for name in results}
    special_results = {}
    special_specs = [
        ("Education - Subscribers", {"subscriber", "subscribers"}, lambda domain: domain.endswith(".ac.uk")),
        (
            "Education - Registered Users",
            {"registered user", "registered users", "registered", "registration"},
            lambda domain: domain.endswith(".ac.uk"),
        ),
        ("Government - Subscribers", {"subscriber", "subscribers"}, lambda domain: "gov.uk" in domain),
        (
            "Government - Registered Users",
            {"registered user", "registered users", "registered", "registration"},
            lambda domain: "gov.uk" in domain,
        ),
    ]

    for label, segment_aliases, predicate in special_specs:
        source_name = next((segment_lookup[alias] for alias in segment_aliases if alias in segment_lookup), None)
        if source_name is None:
            continue
        special_results[label] = filter_result_by_domain(results[source_name], predicate)

    return special_results


def to_excel_download(results: dict[str, dict[str, pd.DataFrame | int]]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for segment_name, segment_results in results.items():
            safe_name = re.sub(r"[^A-Za-z0-9]+", "_", segment_name).strip("_")[:24] or "segment"
            for suffix, key in [("summary", "summary"), ("domains", "domain_summary"), ("emails", "email_summary"), ("rows", "corporate_rows")]:
                sheet_name = f"{safe_name}_{suffix}"[:31]
                segment_results[key].to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)
    return output.getvalue()


def render_segment(segment_name: str, result: dict[str, pd.DataFrame | int], top_n: int) -> None:
    summary = result["summary"].iloc[0]
    domain_summary = result["domain_summary"]
    email_summary = result["email_summary"]
    corporate_rows = result["corporate_rows"]

    metric_columns = st.columns(5)
    metric_columns[0].metric("Rows", f"{int(summary['Total rows']):,}")
    metric_columns[1].metric("Valid emails", f"{int(summary['Valid emails']):,}")
    metric_columns[2].metric("Corporate rows", f"{int(summary['Corporate email rows']):,}")
    metric_columns[3].metric("Corporate domains", f"{int(summary['Unique corporate domains']):,}")
    metric_columns[4].metric("Corporate emails", f"{int(summary['Unique corporate emails']):,}")

    st.subheader("Top corporate domains")
    top_domains = domain_summary.head(top_n)
    st.dataframe(top_domains, use_container_width=True)
    if not top_domains.empty:
        chart_data = top_domains.set_index("Email Domain")["Instances"]
        st.bar_chart(chart_data)

        selected_domain = st.selectbox(
            f"Inspect domain in {segment_name}",
            options=domain_summary["Email Domain"].tolist(),
            index=0,
            key=f"inspect_{segment_name}",
        )
        st.dataframe(
            corporate_rows[corporate_rows["Email Domain"] == selected_domain],
            use_container_width=True,
        )
    else:
        st.info("No corporate domains found for this segment with the current generic-domain filter.")

    st.subheader("Top corporate email addresses")
    st.dataframe(email_summary.head(top_n), use_container_width=True)

    st.download_button(
        label=f"Download {segment_name} domain summary CSV",
        data=domain_summary.to_csv(index=False).encode("utf-8"),
        file_name=f"{segment_name.lower().replace(' ', '_')}_domain_summary.csv",
        mime="text/csv",
        key=f"download_domains_{segment_name}",
    )
    st.download_button(
        label=f"Download {segment_name} corporate rows CSV",
        data=corporate_rows.to_csv(index=False).encode("utf-8"),
        file_name=f"{segment_name.lower().replace(' ', '_')}_corporate_rows.csv",
        mime="text/csv",
        key=f"download_rows_{segment_name}",
    )


def main() -> None:
    st.set_page_config(page_title="Corporate Email Finder", layout="wide")
    st.title("Corporate Email Finder")
    st.write(
        "Upload a Piano export to find the top non-generic email domains among subscribers, registered users, or any other segment in the file."
    )

    generic_domains = load_generic_domains(GENERIC_DOMAINS_PATH)

    with st.sidebar:
        st.header("Settings")
        st.caption(f"Generic domains file: `{GENERIC_DOMAINS_PATH}`")
        top_n = st.slider("Rows to show in rankings", min_value=5, max_value=50, value=15, step=5)
        st.text_area(
            "Current generic domains",
            value="\n".join(generic_domains),
            height=240,
            disabled=True,
            help="Edit the text file directly to change the filtered domains.",
        )

    uploaded_file = st.file_uploader("Upload Piano export", type=["csv", "xlsx", "xlsm", "xls"])
    if uploaded_file is None:
        st.info("Upload a CSV or Excel file to begin.")
        return

    try:
        df = derive_user_segments(load_uploaded_file(uploaded_file))
    except Exception as exc:
        st.error(f"Could not read file: {exc}")
        return

    if df.empty:
        st.warning("The uploaded file has no rows.")
        return

    email_guess = detect_email_column(df)
    segment_guess = suggest_segment_column(df)

    st.subheader("Input preview")
    st.dataframe(df.head(10), use_container_width=True)

    selectors = st.columns(2)
    email_column = selectors[0].selectbox(
        "Email column",
        options=df.columns.tolist(),
        index=df.columns.get_loc(email_guess) if email_guess in df.columns else 0,
    )

    segment_options = ["None"] + df.columns.tolist()
    segment_default = segment_options.index(segment_guess) if segment_guess in df.columns else 0
    segment_column_choice = selectors[1].selectbox(
        "Segment column",
        options=segment_options,
        index=segment_default,
        help="Pick the column that separates subscribers and registered users, if present.",
    )
    segment_column = None if segment_column_choice == "None" else segment_column_choice

    segmented_data = build_segmented_data(df, segment_column)
    generic_domain_set = set(generic_domains)
    results = {
        segment_name: analyse_segment(segment_df, email_column, generic_domain_set)
        for segment_name, segment_df in segmented_data.items()
    }
    special_results = build_special_results(results)
    all_results = {**results, **special_results}

    excel_bytes = to_excel_download(all_results)
    st.download_button(
        label="Download all results as Excel",
        data=excel_bytes,
        file_name="corporate_email_analysis.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    base_segment_names = reorder_segment_names(list(results.keys()))
    special_segment_names = [
        name
        for name in [
            "Education - Subscribers",
            "Education - Registered Users",
            "Government - Subscribers",
            "Government - Registered Users",
        ]
        if name in special_results
    ]
    segment_names = base_segment_names + special_segment_names
    tabs = st.tabs(segment_names)
    for tab, segment_name in zip(tabs, segment_names):
        with tab:
            render_segment(segment_name, all_results[segment_name], top_n)


if __name__ == "__main__":
    main()
