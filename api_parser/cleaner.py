import statistics
from collections import deque
import logging

logger = logging.getLogger(__name__)

class TelemetryCleaner:
    def __init__(self, window_size=10):
        self.window_size = window_size
        self.history = {}  # Tracks rolling window per (sector, spn)
        
        # Absolute bounds for physical impossibilities
        self.hard_bounds = {
            "7001": (0.0, 14.0),     # pH
            "7002": (0.0, 100.0),    # Moisture %
            "7003": (-20.0, 60.0),   # Temp C
            "7004": (0.0, 100.0),    # Humidity %
            "7005": (0.0, 500.0)     # Precip mm
        }

    def process(self, packet):
        """Cleans, smooths, and standardizes a single ISOBUS packet."""
        sector = packet['location_context']['sector']
        spn = packet['isobus_header']['spn']
        key = (sector, spn)

        if key not in self.history:
            self.history[key] = deque(maxlen=self.window_size)

        val = packet.get('value')
        packet['cleaning_flags'] = []

        # 1. Missing Value Interpolation
        if val is None:
            if len(self.history[key]) > 0:
                val = statistics.mean(self.history[key])
                packet['cleaning_flags'].append("INTERPOLATED_MEAN")
            else:
                packet['cleaning_flags'].append("DROPPED_NO_DATA")
                return None 

        # 2. Hard Bounds Checking
        bounds = self.hard_bounds.get(spn)
        if bounds and not (bounds[0] <= val <= bounds[1]):
            packet['cleaning_flags'].append("DROPPED_OUT_OF_BOUNDS")
            return None 

        # 3. Spike Removal (Rolling Median)
        if len(self.history[key]) >= 3:
            current_median = statistics.median(self.history[key])
            # If value deviates by more than 40% from median, smooth it
            if current_median != 0 and abs(val - current_median) / abs(current_median) > 0.40:
                val = current_median
                packet['cleaning_flags'].append("SPIKE_SMOOTHED_MEDIAN")

        # Update buffer with the cleaned value
        self.history[key].append(val)
        packet['value'] = round(val, 2)

        # 4. Data Standardization (Z-Score Normalization)
        if len(self.history[key]) > 1:
            mean = statistics.mean(self.history[key])
            std = statistics.stdev(self.history[key])
            z_score = (val - mean) / std if std > 0 else 0.0
        else:
            z_score = 0.0

        packet['value_normalized'] = round(z_score, 4)
        
        return packet