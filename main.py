import logging
import time
import pandas as pd

from src.pdf_processor import extract_tables_from_pdf
from src.data_cleaner import clean_and_combine_csvs, assign_event_types
from src.utils.file_manip import delete_directory
from src.config import INPUT_PDF_PATH, OUTPUT_DIR, COMBINED_OUTPUT_FILE

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)


def main():
    start_time = time.time()

    # Delete scratch directory if it exists
    delete_directory(OUTPUT_DIR)

    # Extract tables from PDF
    extract_tables_from_pdf(INPUT_PDF_PATH, OUTPUT_DIR)

    # Clean and combine CSV files
    clean_and_combine_csvs(OUTPUT_DIR, COMBINED_OUTPUT_FILE)

    # Read combined CSV
    combined_df = pd.read_csv(COMBINED_OUTPUT_FILE)

    # Assign event types
    combined_df = assign_event_types(combined_df)

    # Save final combined data
    combined_df.to_csv(COMBINED_OUTPUT_FILE, index=False)
    _log.info(f"Final combined data saved to {COMBINED_OUTPUT_FILE}")

    end_time = time.time() - start_time
    _log.info(f"Total processing time: {end_time:.2f} seconds")


if __name__ == "__main__":
    main()
