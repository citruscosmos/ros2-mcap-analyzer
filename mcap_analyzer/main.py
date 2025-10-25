import argparse
from pathlib import Path
import sys
from typing import List, Dict, Any

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
        files = sorted(list(source_path.glob('*.mcap')))
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
            analysis_type = task.get('analysis_type', 'none')
            analyzer = get_analyzer(analysis_type)
            result = analyzer.analyze(df)
            reporter.add_analysis_result(task['id'], task['topic_name'], analysis_type, result)

        except Exception as e:
            print(f"Error: An unexpected error occurred while processing task '{task.get('id', 'N/A')}': {e}", file=sys.stderr)
            continue

    reporter.print_console_report()
    reporter.write_markdown_report()
    print(f"\n✅ Analysis complete. Check the results in {output_dir}")

def main():
    """Entry point that handles command-line arguments and starts the analysis."""
    parser = argparse.ArgumentParser(description="ROS2 MCAP Data Analysis Tool")
    parser.add_argument("mcap_source", type=Path, help="Path to a single MCAP file or a directory containing MCAP files.")
    parser.add_argument("config", type=Path, help="Path to the analysis configuration YAML file.")
    args = parser.parse_args()

    run_analysis(args.mcap_source, args.config)

if __name__ == "__main__":
    main()
