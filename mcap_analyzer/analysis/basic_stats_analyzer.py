from .base_analyzer import BaseAnalyzer
import pandas as pd
import numpy as np

class BasicStatsAnalyzer(BaseAnalyzer):
    """Analyzer for the 'basic_stats' type. Calculates basic statistics."""

    def analyze(self, df: pd.DataFrame) -> dict:
        """
        Calculates basic statistics (mean, std dev, max, min).
        """
        if 'parsed_value' not in df.columns or df['parsed_value'].empty:
            return {}

        series = df['parsed_value']
        stats = {
            "mean": series.mean(),
            "std": series.std(),
            "max": series.max(),
            "min": series.min(),
            "count": series.count()
        }
        return {"basic_stats": stats}
