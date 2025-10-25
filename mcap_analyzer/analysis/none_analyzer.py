from .base_analyzer import BaseAnalyzer
import pandas as pd

class NoneAnalyzer(BaseAnalyzer):
    """Analyzer for the 'none' type. Performs no analysis."""

    def analyze(self, df: pd.DataFrame) -> dict:
        """
        Performs no analysis and returns an empty dictionary.
        """
        return {}
