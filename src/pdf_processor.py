from docling.document_converter import DocumentConverter

import logging
import time
from pathlib import Path
import pandas as pd

_log = logging.getLogger(__name__)


def convert_document_and_export_tables(input_doc_path: Path, output_dir: Path):
    logging.basicConfig(level=logging.INFO)

    doc_converter = DocumentConverter()

    start_time = time.time()

    conv_res = doc_converter.convert(input_doc_path)

    output_dir.mkdir(parents=True, exist_ok=True)

    doc_filename = conv_res.input.file.stem

    csv_files: list[Path] = []
    for table_ix, table in enumerate(conv_res.document.tables):
        table_df: pd.DataFrame = table.export_to_dataframe()
        print(f"## Table {table_ix}")
        print(table_df.to_markdown())

        element_csv_filename = output_dir / f"{doc_filename}-table-{table_ix + 1}.csv"
        _log.info(f"Saving CSV table to {element_csv_filename}")
        table_df.to_csv(element_csv_filename, index=False)
        csv_files.append(element_csv_filename)

    elapsed_time = time.time() - start_time
    _log.info(f"Document converted and tables exported in {elapsed_time:.2f} seconds.")

    return csv_files
