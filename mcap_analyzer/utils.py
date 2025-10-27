import datetime
from pathlib import Path

def create_output_directory(base_path: Path = Path("./results")) -> Path:
    """
    Creates a unique output directory including the execution timestamp.

    Args:
        base_path: The base path where the output directory will be created.

    Returns:
        The path to the created output directory.
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = base_path / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir
