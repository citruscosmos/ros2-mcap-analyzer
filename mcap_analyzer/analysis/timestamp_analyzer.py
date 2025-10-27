from .base_analyzer import BaseAnalyzer
import pandas as pd
import numpy as np
import re

class TimestampAnalyzer(BaseAnalyzer):
    """Analyzer for the 'timestamp' type. Calculates statistics on timestamps."""

    def __init__(self, analysis_type_config: str):
        """
        Args:
            analysis_type_config: Configuration string in 'timestamp(freq:HZ)' format.
        """
        self.freq_hz = self._parse_freq(analysis_type_config)
        if self.freq_hz <= 0:
            raise ValueError("Frequency must be a positive value.")
        self.expected_period_ns = 1e9 / self.freq_hz

    def _parse_freq(self, config_str: str) -> float:
        """Parses the frequency from the 'timestamp(freq:HZ)' string."""
        match = re.search(r'freq:([\d\.]+)', config_str)
        if not match:
            raise ValueError(f"Frequency not specified in config: {config_str}")
        return float(match.group(1))

    def _calculate_stats(self, series: pd.Series) -> dict:
        """Calculates statistics from a pandas Series."""
        if series.empty:
            return {"mean": np.nan, "std": np.nan, "max": np.nan, "min": np.nan}
        return {
            "mean": series.mean(),
            "std": series.std(),
            "max": series.max(),
            "min": series.min(),
        }

    def analyze(self, df: pd.DataFrame) -> dict:
        """
        Calculates period, frequency, and jitter/drift from timestamp data.
        """
        if 'parsed_value' not in df.columns or len(df['parsed_value']) < 2:
            return {}

        timestamps_ns = df['parsed_value'].astype(np.int64)

        # Period (s)
        period_ns = timestamps_ns.diff().dropna()
        period_s = period_ns / 1e9
        period_stats = self._calculate_stats(period_s)

        # Frequency (Hz)
        # Avoid division by zero
        frequency_hz = (1e9 / period_ns[period_ns > 0]).dropna()
        frequency_stats = self._calculate_stats(frequency_hz)

        # Jitter/Drift from Time of Start (ToS) [s]
        t_start_ns = timestamps_ns.iloc[0]

        # Recommended implementation from spec:
        # deviation = ((t_i_ns - t_start_ns + T_expected_ns/2) % T_expected_ns) - (T_expected_ns/2)
        # This keeps the deviation within the range [-T/2, +T/2]
        deviation_ns = ((timestamps_ns - t_start_ns + self.expected_period_ns / 2) % self.expected_period_ns) - (self.expected_period_ns / 2)
        deviation_s = deviation_ns / 1e9
        jitter_stats = self._calculate_stats(deviation_s)

        return {
            "specified_frequency_hz": self.freq_hz,
            "expected_period_s": self.expected_period_ns / 1e9,
            "period_s": period_stats,
            "frequency_hz": frequency_stats,
            "jitter_drift_s": jitter_stats,
        }
