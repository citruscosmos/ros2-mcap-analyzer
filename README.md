# ROS2 MCAP Data Analysis Tool

## Overview

This is a Python-based command-line tool for analyzing ROS2 messages recorded in MCAP (`.mcap`) files. It takes an MCAP file or a directory of MCAP files and a YAML configuration file as input. Based on the settings, it extracts data, performs calculations, and outputs analysis results to the console and a Markdown file.

## Features

- **Flexible Configuration**: Define complex analysis tasks using a simple YAML file.
- **Advanced Data Parsing**:
    - Extract nested fields from ROS2 messages.
    - Perform on-the-fly calculations (e.g., converting m/s to km/h).
    - Cast data to specific types (`float64`, `uint32`, etc.).
    - Extract and convert binary data segments from `uint8[]` fields.
- **Extensible Analysis**: The tool uses a strategy pattern to make it easy to add new analysis types. Currently supported:
    - `timestamp(freq:HZ)`: Calculates statistics for timestamp data, including period, frequency, and jitter/drift.
    - `basic_stats`: Computes basic statistics (mean, standard deviation, max, min).
    - `none`: Simply parses the data and outputs it to a CSV without further analysis.
- **Multiple Output Formats**:
    - **Markdown Report**: Generates a clean, human-readable summary of all analysis tasks.
    - **Intermediate CSV**: Outputs a CSV file for each task, containing raw and parsed values for further inspection.
    - **Console Output**: Prints a summary of the results directly to the console.
- **Secure by Design**: Uses `asteval` for safe evaluation of mathematical expressions defined in the configuration.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Install dependencies:**
    Make sure you have Python 3.10 or higher installed.
    ```bash
    pip install -r requirements.txt
    ```

## Usage

Run the analysis from the command line by providing the path to your MCAP data and your configuration file.

```bash
python -m mcap_analyzer.main <path_to_mcap_source> <path_to_config.yaml>
```

-   `<path_to_mcap_source>`: Can be a path to a single `.mcap` file or a directory containing multiple `.mcap` files.
-   `<path_to_config.yaml>`: The path to your YAML configuration file.

Results, including the Markdown report and intermediate CSVs, will be saved in a new directory under `results/` named with the execution timestamp (e.g., `results/20251025_143000/`).

## Configuration (`config.yaml`)

The analysis process is controlled by a YAML file, which contains a list of `analyses` tasks.

### Example `config.yaml`:
```yaml
analyses:
  # Task 1: Convert velocity from m/s to km/h and calculate basic stats
  - id: "velocity_analysis_kmh"
    topic_name: "/vehicle/status/velocity"
    field_names: "twist.linear.x"
    parse_string: "twist.linear.x(type:float64) * 3.6"
    analysis_type: "basic_stats"

  # Task 2: Analyze the jitter and frequency of IMU timestamps
  - id: "imu_timestamp_jitter"
    topic_name: "/sensor/imu/data"
    field_names: "header.stamp.sec, header.stamp.nanosec"
    parse_string: "header.stamp.sec(type:uint64) * 1000000000 + header.stamp.nanosec(type:uint64)"
    analysis_type: "timestamp(freq:100)" # Analyze against an expected 100Hz frequency

  # Task 3: Extract a float64 value from a custom binary topic
  - id: "custom_binary_data"
    topic_name: "/custom/binary_topic"
    field_names: "data"
    # Extracts 8 bytes starting from the 8th byte (0-indexed) and unpacks as float64
    parse_string: "data(byte:8-8,type:float64)"
    analysis_type: "none" # No analysis, just save the parsed value to CSV
```

### `parse_string` Directives:

The `parse_string` field defines how to calculate the final value. It uses special directives inside parentheses `()` to control how raw field values are interpreted.

-   `field_name(default)`: Uses the field's native Python type. This is the default if no directive is provided.
-   `field_name(type:type_name)`: Casts the field value to the specified `type_name` (e.g., `float64`, `uint32`).
-   `field_name(byte:start-length)`: Treats the field (typically `uint8[]` or `bytes`) as a binary blob and extracts a slice of `length` bytes starting from `start` (0-indexed).
-   `field_name(byte:start-length,type:type_name)`: Extracts the byte slice as above and then unpacks it as the specified `type_name` (e.g., `float64`, `int32`).
