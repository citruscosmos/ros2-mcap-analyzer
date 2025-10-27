from abc import ABC, abstractmethod
import pandas as pd

class BaseAnalyzer(ABC):
    """Abstract base class for analysis strategies."""

    @abstractmethod
    def analyze(self, df: pd.DataFrame) -> dict:
        """
        Analyzes the DataFrame and returns the results as a dictionary.

        Args:
            df: The DataFrame to analyze. It must have a 'parsed_value' column.

        Returns:
            A dictionary containing the analysis results.
        """
        pass
