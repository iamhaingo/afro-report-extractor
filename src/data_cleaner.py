import csv
import pandas as pd
from pathlib import Path
import logging
import re
import pycountry
from datetime import datetime


_log = logging.getLogger(__name__)


def clean_and_combine_csvs(directory_path: Path, output_file: Path):
    combined_data = []

    # Process each CSV file in numeric order based on filename
    for input_file in sorted(
        directory_path.glob("*.csv"),
        key=lambda x: int(re.findall(r"\d+", x.stem)[-1])
        if re.findall(r"\d+", x.stem)
        else 0,
    ):
        rows_total = rows_kept = 0

        with open(input_file, "r", encoding="utf-8", errors="replace") as infile:
            reader = csv.reader(infile)
            for row in reader:
                rows_total += 1

                # Skip completely empty rows
                if not any(cell.strip() for cell in row):
                    continue

                # Skip rows with fewer than 8 columns
                if len(row) < 8:
                    continue

                # Keep rows that are event marker headers
                if len(row) > 1 and row[1] in {"New Events", "Ongoing Events"}:
                    combined_data.append(row)
                    rows_kept += 1
                    continue

                # Skip rows that are too wide (likely malformed)
                if len(row) > 11:
                    continue

                # If all filters pass, keep the row
                combined_data.append(row)
                rows_kept += 1

        # Optional: logging can be re-enabled if needed
        # _log.info(f"Processed {input_file.name}. Kept {rows_kept}/{rows_total} rows")

    # Write the combined data to the output CSV
    with open(output_file, "w", encoding="utf-8", newline="") as outfile:
        csv.writer(outfile).writerows(combined_data)

    # _log.info(f"Combined cleaned data written to {output_file}")


def assign_event_types(combined: pd.DataFrame) -> pd.DataFrame:
    # Drop completely empty rows
    combined = combined.dropna(how="all").reset_index(drop=True)

    # If 'Country' column is missing, assume no header and assign default
    expected_headers = [
        "Country",
        "Event",
        "Grade",
        "Date notified to WCO",
        "Start of reporting period",
        "End of reporting period",
        "Total cases",
        "Cases Confirmed",
        "Deaths",
        "CFR",
    ]
    if "Country" not in combined.columns:
        if combined.shape[1] == len(expected_headers):
            combined.columns = expected_headers
        else:
            raise ValueError(
                f"Expected {len(expected_headers)} columns, "
                f"but got {combined.shape[1]} — cannot assign headers safely."
            )

    # Mark section headers
    combined.loc[combined["Country"] == "New Events", "Event Type"] = "New"
    combined.loc[combined["Country"] == "Ongoing Events", "Event Type"] = "Ongoing"
    combined.loc[combined["Country"] == "Closed Events", "Event Type"] = "Closed"

    # Forward fill event type from section headers
    combined["Event Type"] = combined["Event Type"].ffill()

    # Remove section header rows
    combined = combined[
        ~combined["Country"].isin(["New Events", "Ongoing Events", "Closed Events"])
    ]

    return combined


def attach_descriptions_by_length(
    df: pd.DataFrame, length_threshold: int = 100
) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    df["DESCRIPTION"] = None

    cleaned_rows = []
    i = 0

    while i < len(df):
        current_row = df.iloc[i].copy()

        # Check if the next row exists and is likely a description
        if i + 1 < len(df):
            next_row = df.iloc[i + 1]
            next_first_col = str(next_row.iloc[0])  # ensure it's a string

            if len(next_first_col) > length_threshold:
                current_row["DESCRIPTION"] = next_first_col
                cleaned_rows.append(current_row)
                i += 2  # skip the description row
                continue

        # No description row; keep as-is
        cleaned_rows.append(current_row)
        i += 1

    return pd.DataFrame(cleaned_rows).reset_index(drop=True)


def clean_combined_data(df):
    # Columns to convert to numeric
    numeric_columns = [
        "Total cases",
        "Cases Confirmed",
        "Deaths",
        # "CFR"  # Uncomment if CFR is also needed
    ]

    # Convert numeric columns
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "").str.strip(), errors="coerce"
            )

    # Convert date columns to datetime
    date_columns = [
        "Date notified to WCO",
        "Start of reporting period",
        "End of reporting period",
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def add_iso3_column(df: pd.DataFrame, country_col: str = "Country") -> pd.DataFrame:
    def get_iso3(country_name):
        try:
            return pycountry.countries.lookup(country_name).alpha_3
        except LookupError:
            return None  # or 'UNK' if preferred

    df["ISO3"] = df[country_col].apply(get_iso3)
    return df


def add_pdf_name_column(df: pd.DataFrame, pdf_path: Path) -> pd.DataFrame:
    if not isinstance(pdf_path, Path):
        raise TypeError("pdf_path must be a pathlib.Path object")
    if pdf_path.suffix.lower() != ".pdf":
        raise ValueError("Provided path does not point to a PDF file")
    pdf_name = pdf_path.stem
    df = df.copy()
    df["NAME"] = pdf_name
    return df


def add_week_and_date(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    week = int(filename[3:5])
    date = filename[6:]

    if len(date) == 12:
        sd, sm, ed, em, y = date[:2], date[2:4], date[4:6], date[6:8], date[8:]
    else:
        sd, ed, sm, em, y = date[:2], date[2:4], date[4:6], date[4:6], date[6:]

    try:
        s_date = datetime.strptime(f"{sd}-{sm}-{y}", "%d-%m-%Y")
    except ValueError:
        s_date = datetime.strptime(f"{sd}-{sm}-{int(y) - 1}", "%d-%m-%Y")

    e_date = datetime.strptime(f"{ed}-{em}-{y}", "%d-%m-%Y")

    if s_date.year != e_date.year:
        drange = f"{s_date.strftime('%d %b %Y')} - {e_date.strftime('%-d %b %Y')}"
    elif s_date.month != e_date.month:
        drange = f"{s_date.strftime('%d %b')} - {e_date.strftime('%-d %b %Y')}"
    else:
        drange = f"{s_date.strftime('%-d')} - {e_date.strftime('%-d %b %Y')}"

    df["WEEK"] = week
    df["DATE"] = drange
    return df


def rearrange(combined: pd.DataFrame) -> pd.DataFrame:
    # Rename columns
    combined = combined.rename(
        columns={
            "Date notified to WCO": "DATE_NOTIFY",
            "Start of reporting period": "DATE_START",
            "End of reporting period": "DATE_END",
            "Cases Confirmed": "CASES_CONFIRMED",
            "Total cases": "CASES_TOTAL",
            "Deaths": "DEATHS",
            "Event Type": "EVENT_TYPE",
            "Grade": "GRADE",
            "Event": "EVENT_NAME",
            "Country": "COUNTRY",
        }
    )

    combined = combined.rename(
        columns={
            "Start of reporting": "DATE_START",
            "End of reporting": "DATE_END",
        }
    )

    # Reorder columns
    column_order = [
        "NAME",
        "WEEK",
        "DATE",
        "EVENT_TYPE",
        "ISO3",
        "COUNTRY",
        "EVENT_NAME",
        "GRADE",
        "DATE_NOTIFY",
        "DATE_START",
        "DATE_END",
        "CASES_TOTAL",
        "CASES_CONFIRMED",
        "DEATHS",
        "CFR",
        "DESCRIPTION",
    ]

    combined = combined[column_order]

    return combined
