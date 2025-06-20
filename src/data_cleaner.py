import csv
import pandas as pd
from pathlib import Path
import logging
import re

_log = logging.getLogger(__name__)


def clean_and_combine_csvs(directory_path: Path, output_file: Path):
    combined_data: list[list[str]] = []
    for input_file in sorted(
        directory_path.glob("*.csv"), key=lambda x: int(re.findall(r"\d+", x.stem)[-1])
    ):
        rows_total = rows_kept = 0
        with open(input_file, "r", encoding="utf-8", errors="replace") as infile:
            reader = csv.reader(infile)
            for row in reader:
                rows_total += 1
                if not row or all(not cell.strip() for cell in row):
                    continue
                if len(row) < 8:
                    continue
                if (len(row) > 1 and row[1] in ["New Events", "Ongoing Events"]) or any(
                    marker in str(row) for marker in ["New Events", "Ongoing Events"]
                ):
                    combined_data.append(row)
                    rows_kept += 1
                    continue
                if len(row) > 11:
                    continue
                if len(row) > 3:
                    if any(len(str(cell)) > 70 for cell in row[1:]):
                        continue
                    prefix = " ".join(str(cell) for cell in row[1:3])
                    if any(prefix in str(cell) for cell in row[3:]):
                        continue
                combined_data.append(row)
                rows_kept += 1
        _log.info(f"Processed {input_file.name}. Kept {rows_kept}/{rows_total} rows")

    with open(output_file, "w", encoding="utf-8", newline="") as outfile:
        csv.writer(outfile).writerows(combined_data)
    _log.info(f"Combined cleaned data written to {output_file}")


def assign_event_types(combined_df: pd.DataFrame) -> pd.DataFrame:
    combined_df.loc[combined_df["Country"] == "New Events", "Event Type"] = "New"
    combined_df.loc[combined_df["Country"] == "Ongoing Events", "Event Type"] = (
        "Ongoing"
    )
    combined_df.loc[combined_df["Country"] == "Closed Events", "Event Type"] = "Closed"

    new_events_index = combined_df.index[
        combined_df["Country"] == "New Events"
    ].tolist()
    ongoing_events_index = combined_df.index[
        combined_df["Country"] == "Ongoing Events"
    ].tolist()
    closed_events_index = combined_df.index[
        combined_df["Country"] == "Closed Events"
    ].tolist()

    if new_events_index:
        end_index = (
            ongoing_events_index[0]
            if ongoing_events_index
            else (closed_events_index[0] if closed_events_index else len(combined_df))
        )
        for i in range(new_events_index[0] + 1, end_index):
            combined_df.loc[i, "Event Type"] = "New"

    if ongoing_events_index:
        end_index = closed_events_index[0] if closed_events_index else len(combined_df)
        for i in range(ongoing_events_index[0] + 1, end_index):
            combined_df.loc[i, "Event Type"] = "Ongoing"

    if closed_events_index:
        for i in range(closed_events_index[0] + 1, len(combined_df)):
            combined_df.loc[i, "Event Type"] = "Closed"

    combined_df = combined_df[
        ~combined_df["Country"].isin(["New Events", "Ongoing Events", "Closed Events"])
    ]

    return combined_df
