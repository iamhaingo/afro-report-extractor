from pathlib import Path
import shutil


def delete_directory(directory: Path):
    if directory.exists():
        shutil.rmtree(directory)
        print(f"Deleted directory: {directory}")
