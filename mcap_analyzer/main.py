import argparse
from pathlib import Path
import sys
from typing import List, Dict, Any
import pandas as pd

from mcap_analyzer.config_loader import load_config
from mcap_analyzer.utils import create_output_directory
from mcap_analyzer.reporter import Reporter
from mcap_analyzer.mcap_parser import McapParser
from mcap_analyzer.analysis.base_analyzer import BaseAnalyzer
from mcap_analyzer.analysis.none_analyzer import NoneAnalyzer
from mcap_analyzer.analysis.basic_stats_analyzer import BasicStatsAnalyzer
from mcap_analyzer.analysis.timestamp_analyzer import TimestampAnalyzer

def get_analyzer(analysis_type: str) -> BaseAnalyzer:
    """Factory function to return the appropriate analyzer instance based on the analysis_type string."""
    if analysis_type.startswith('timestamp'):
        return TimestampAnalyzer(analysis_type)
    elif analysis_type == 'basic_stats':
        return BasicStatsAnalyzer()
    elif analysis_type == 'none':
        return NoneAnalyzer()
    else:
        print(f"Warning: Unknown analysis_type '{analysis_type}'. Treating as 'none'.", file=sys.stderr)
        return NoneAnalyzer()

def get_mcap_files(source_path: Path) -> List[Path]:
    """Gets a list of MCAP files from the specified path."""
    if not source_path.exists():
        print(f"Error: MCAP data source not found: {source_path}", file=sys.stderr)
        sys.exit(1)

    if source_path.is_dir():
        files = sorted(list(source_path.glob('**/*.mcap')))
        if not files:
            print(f"Error: No MCAP files found in the directory: {source_path}", file=sys.stderr)
            sys.exit(1)
        return files
    elif source_path.is_file():
        if source_path.suffix != '.mcap':
            print(f"Error: The specified file is not an .mcap file: {source_path}", file=sys.stderr)
            sys.exit(1)
        return [source_path]
    else:
        print(f"Error: Invalid path specified: {source_path}", file=sys.stderr)
        sys.exit(1)

def process_task(df: pd.DataFrame, task: Dict[str, Any], reporter: Reporter):
    """Analyzes a DataFrame based on a task config and records the result."""
    if df.empty:
        print(f"Warning: DataFrame for task '{task['id']}' is empty. Skipping analysis.")
        return

    analysis_type = task.get('analysis_type', 'none')
    analyzer = get_analyzer(analysis_type)
    result = analyzer.analyze(df)
    reporter.add_analysis_result(task['id'], task['topic_name'], analysis_type, result)


def run_analysis(mcap_source_path: Path, config_path: Path):
    """The main function that executes the entire analysis process."""
    try:
        config = load_config(config_path)
        if 'analyses' not in config or not isinstance(config['analyses'], list):
            print("Error: The configuration file must contain a list named 'analyses'.", file=sys.stderr)
            sys.exit(1)
    except (FileNotFoundError, ValueError) as e:
        print(f"Failed to load configuration file: {e}", file=sys.stderr)
        sys.exit(1)

    mcap_files = get_mcap_files(mcap_source_path)
    output_dir = create_output_directory()
    reporter = Reporter(output_dir)

    print(f"Output directory: {output_dir}")
    print(f"Target MCAP files: {[str(f) for f in mcap_files]}")

    for task in config['analyses']:
        try:
            print(f"\n--- Starting analysis task '{task['id']}' ---")

            parser = McapParser(task)
            df = parser.process_mcap_files(mcap_files)

            if df.empty:
                print(f"Warning: No messages found for topic '{task['topic_name']}' or they could not be processed. Skipping task.")
                continue

            reporter.save_intermediate_csv(task['id'], df)
            process_task(df, task, reporter)

        except Exception as e:
            print(f"Error: An unexpected error occurred while processing task '{task.get('id', 'N/A')}': {e}", file=sys.stderr)
            continue

    reporter.print_console_report()
    reporter.write_markdown_report()
    print(f"\n✅ Analysis complete. Check the results in {output_dir}")


def run_analysis_from_csv(csv_source_path: Path, config_path: Path):
    """The main function that executes the entire analysis process from CSV files."""
    try:
        config = load_config(config_path)
        if 'analyses' not in config or not isinstance(config['analyses'], list):
            print("Error: The configuration file must contain a list named 'analyses'.", file=sys.stderr)
            sys.exit(1)
    except (FileNotFoundError, ValueError) as e:
        print(f"Failed to load configuration file: {e}", file=sys.stderr)
        sys.exit(1)

    if not csv_source_path.is_dir():
        print(f"Error: The specified CSV source is not a directory: {csv_source_path}", file=sys.stderr)
        sys.exit(1)

    output_dir = create_output_directory()
    reporter = Reporter(output_dir)

    print(f"Output directory: {output_dir}")
    print(f"Re-processing from CSVs in: {csv_source_path}")

    for task in config['analyses']:
        task_id = task['id']
        csv_file_path = csv_source_path / f"{task_id}.csv"

        if not csv_file_path.is_file():
            print(f"Warning: Intermediate CSV file for task '{task_id}' not found at '{csv_file_path}'. Skipping task.", file=sys.stderr)
            continue

        try:
            print(f"\n--- Starting analysis task '{task['id']}' from CSV ---")
            df = pd.read_csv(csv_file_path)

            if df.empty:
                print(f"Warning: CSV file for task '{task['id']}' is empty. Skipping task.")
                continue

            process_task(df, task, reporter)

        except Exception as e:
            print(f"Error: An unexpected error occurred while re-processing task '{task.get('id', 'N/A')}': {e}", file=sys.stderr)
            continue

    reporter.print_console_report()
    reporter.write_markdown_report()
    print(f"\n✅ Analysis complete. Check the results in {output_dir}")


def main():
    """Entry point that handles command-line arguments and starts the analysis."""
    parser = argparse.ArgumentParser(description="ROS2 MCAP Data Analysis Tool")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--mcap", dest="mcap_source", type=Path, help="Path to a single MCAP file or a directory of MCAP files.")
    group.add_argument("--csv", dest="csv_source", type=Path, help="Path to a directory with intermediate CSV files to re-process.")
    parser.add_argument("config", type=Path, help="Path to the analysis configuration YAML file.")
    args = parser.parse_args()

    if args.csv_source:
        run_analysis_from_csv(args.csv_source, args.config)
    else:
        run_analysis(args.mcap_source, args.config)

if __name__ == "__main__":
    main()
