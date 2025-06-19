from pathlib import Path
import shutil


def delete_directory(directory: Path):
    if directory.exists():
        shutil.rmtree(directory)
        print(f"Deleted directory: {directory}")


def delete_individual_csv(individual_csv_files: list[Path], combined_file: Path):
    if combined_file.exists() and combined_file.stat().st_size > 0:
        for csv_file in individual_csv_files:
            try:
                csv_file.unlink()
            except Exception as _:
                pass
