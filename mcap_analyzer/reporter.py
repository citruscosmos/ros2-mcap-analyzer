from pathlib import Path
import pandas as pd
import datetime
from typing import Dict, Any

class Reporter:
    """Manages the generation of analysis result reports."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.results: Dict[str, Dict[str, Any]] = {}
        self.start_time = datetime.datetime.now()

    def save_intermediate_csv(self, task_id: str, df: pd.DataFrame):
        """Saves the intermediate DataFrame as a CSV file."""
        csv_path = self.output_dir / f"{task_id}.csv"
        df.to_csv(csv_path, index=False)
        print(f"Saved intermediate CSV to: {csv_path}")

    def add_analysis_result(self, task_id: str, topic_name: str, analysis_type: str, result: dict, start_time_ns: int, end_time_ns: int, message_count: int):
        """Stores the result of an analysis task."""
        self.results[task_id] = {
            "topic_name": topic_name,
            "analysis_type": analysis_type,
            "result": result,
            "start_time_ns": start_time_ns,
            "end_time_ns": end_time_ns,
            "message_count": message_count
        }

    def _format_stats_table(self, title: str, data: Dict[str, float], unit: str) -> str:
        """Helper function to generate a Markdown table for statistical data."""
        # Format to 4 significant figures
        formatted_data = {k: f"{v:.4g}" for k, v in data.items()}

        return (
            f"### {title} [{unit}]\n"
            "| Statistic | Value |\n"
            "| :--- | :--- |\n"
            f"| Mean | {formatted_data.get('mean', 'N/A')} |\n"
            f"| Std Dev | {formatted_data.get('std', 'N/A')} |\n"
            f"| Max | {formatted_data.get('max', 'N/A')} |\n"
            f"| Min | {formatted_data.get('min', 'N/A')} |\n\n"
        )

    def write_markdown_report(self):
        """Writes the final analysis results to a Markdown file."""
        md_path = self.output_dir / "result.md"
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(f"# MCAP Analysis Report (Generated on: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')})\n\n")

            for task_id, data in self.results.items():
                f.write(f"## Task: {task_id} (Topic: {data['topic_name']})\n\n")

                # Overall Stats Section
                f.write("### Overall Stats\n")
                f.write(f"- **Total Messages:** {data['message_count']}\n")
                f.write(f"- **Start Time:** {pd.to_datetime(data['start_time_ns'], unit='ns')} (UTC)\n")
                f.write(f"- **End Time:** {pd.to_datetime(data['end_time_ns'], unit='ns')} (UTC)\n\n")

                result = data['result']
                analysis_type = data['analysis_type']

                if analysis_type == 'none':
                    f.write("(No analysis performed for type 'none'.)\n\n")
                elif not result:
                    f.write("No analysis results available.\n\n")
                elif analysis_type.startswith('timestamp'):
                    f.write(f"Specified Frequency: {result.get('specified_frequency_hz', 'N/A')} Hz "
                            f"(Expected Period: {result.get('expected_period_s', 'N/A'):.4g} s)\n\n")
                    f.write(self._format_stats_table("Period", result.get('period_s', {}), "s"))
                    f.write(self._format_stats_table("Frequency", result.get('frequency_hz', {}), "Hz"))
                    f.write(self._format_stats_table("Jitter/Drift from ToS", result.get('jitter_drift_s', {}), "s"))
                elif analysis_type == 'basic_stats':
                     f.write(self._format_stats_table("Basic Statistics", result.get('basic_stats', {}), "unit-less"))

                f.write("---\n\n")

        print(f"Markdown report saved to: {md_path}")

    def print_console_report(self):
        """Prints the final analysis results to the console."""
        print("\n--- MCAP Analysis Report ---")
        for task_id, data in self.results.items():
            print(f"\n## Task: {task_id} (Topic: {data['topic_name']})")
            result = data['result']
            analysis_type = data['analysis_type']

            if analysis_type == 'none':
                print("(No analysis performed for type 'none'.)")
            elif not result:
                 print("No analysis results available.")
            elif analysis_type.startswith('timestamp'):
                print(f"  Specified Frequency: {result.get('specified_frequency_hz', 'N/A')} Hz")
                print("  Period (s):", result.get('period_s', {}))
                print("  Frequency (Hz):", result.get('frequency_hz', {}))
                print("  Drift (s):", result.get('jitter_drift_s', {}))
            elif analysis_type == 'basic_stats':
                print("  Basic Statistics:", result.get('basic_stats', {}))
        print("\n--- End of Report ---")
