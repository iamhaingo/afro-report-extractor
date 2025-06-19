import logging
import time
import os
import pandas as pd
from pathlib import Path

from src.pdf_processor import extract_tables_from_pdf
from src.data_cleaner import clean_and_combine_csvs, assign_event_types
from src.utils.file_manip import delete_directory, delete_individual_csv
from src.config import (
    INPUT_PDF_DIR,
    BASE_OUTPUT_DIR,
)  # adjust config to use directory instead of single file

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)


def process_pdf(pdf_path: Path, output_dir: Path, combined_output_file: Path):
    # Clean output directory
    delete_directory(output_dir)

    # Extract tables from PDF
    csv_files = extract_tables_from_pdf(pdf_path, output_dir)

    # Clean and combine CSVs
    clean_and_combine_csvs(output_dir, combined_output_file)

    # Read combined CSV
    combined_df = pd.read_csv(combined_output_file)

    # Assign event types
    combined_df = assign_event_types(combined_df)

    # Save final CSV
    combined_df.to_csv(combined_output_file, index=False)
    _log.info(f"Processed {pdf_path} -> {combined_output_file}")

    # Delete individual CSVs
    delete_individual_csv(csv_files, combined_output_file)


def main():
    start_time = time.time()

    # Ensure base output directory exists
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

    # Process all PDFs in input directory
    for filename in os.listdir(INPUT_PDF_DIR):
        if filename.lower().endswith(".pdf"):
            try:
                pdf_path = os.path.join(INPUT_PDF_DIR, filename)
                name_without_ext = os.path.splitext(filename)[0]
                output_dir = os.path.join(BASE_OUTPUT_DIR, name_without_ext)
                combined_output_file = os.path.join(
                    output_dir, f"{name_without_ext}_combined.csv"
                )

                process_pdf(
                    Path(pdf_path), Path(output_dir), Path(combined_output_file)
                )
            except Exception as e:
                _log.error(f"Error processing {filename}: {str(e)}")
                with open(os.path.join(BASE_OUTPUT_DIR, "error_log.txt"), "a") as f:
                    f.write(f"File: {filename}\nError: {str(e)}\n\n")

    _log.info(f"Total time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
