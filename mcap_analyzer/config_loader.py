import yaml
from pathlib import Path
import sys

def load_config(config_path: Path) -> dict:
    """
    Loads a YAML configuration file from the specified path and returns it as a dictionary.

    Args:
        config_path: The path to the configuration file.

    Returns:
        A dictionary containing the configuration.

    Raises:
        FileNotFoundError: If the file is not found.
        yaml.YAMLError: If the YAML file fails to parse.
    """
    if not config_path.is_file():
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"Error: Invalid YAML format in configuration file: {e}", file=sys.stderr)
        raise
