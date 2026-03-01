import json
import statistics
from collections import deque
import logging

logger = logging.getLogger(__name__)

class TelemetryCleaner:
    def __init__(self, window_size=10):
        self.window_size = window_size
        self.history = {}  # Tracks rolling window per (sector, path)
        
        # Absolute bounds for physical impossibilities
        # Updated to check string substrings instead of SPNs
        self.hard_bounds = {
            "ph": (0.0, 14.0),           # Was 7001
            "moisture": (0.0, 100.0),    # Was 7002
            "temperature": (-20.0, 60.0),# Was 7003
            "humidity": (0.0, 100.0),    # Was 7004
            "precipitation": (0.0, 500.0)# Was 7005
        }

    def _get_bounds(self, path):
        """Finds hard bounds if the path contains known keywords like 'ph' or 'temperature'."""
        for key, bounds in self.hard_bounds.items():
            if key in path.lower():
                return bounds
        return None

    def _set_nested(self, d, path, val):
        """Helper to reconstruct nested dictionaries from a path string."""
        keys = path.split('.')
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = val

    def process(self, packet):
        """Cleans, smooths, and standardizes a multi-metric ISOBUS packet."""
        sector = packet.get('location', {}).get('sector')
        if sector is None:
            return None  # Invalid packet

        cleaned_data = {}
        normalized_data = {}
        cleaning_flags = {}

        # 1. Recursive helper to extract all metrics from the nested dictionary
        def traverse(data_node, current_path=""):
            for k, v in data_node.items():
                path = f"{current_path}.{k}" if current_path else k
                if isinstance(v, dict):
                    traverse(v, path)
                else:
                    clean_metric(path, v)

        # 2. Logic to clean individual metrics mapped to their sector
        def clean_metric(path, val):
            key = (sector, path)
            if key not in self.history:
                self.history[key] = deque(maxlen=self.window_size)

            flags = []

            # Missing Value Interpolation
            if val is None:
                if len(self.history[key]) > 0:
                    val = statistics.mean(self.history[key])
                    flags.append("INTERPOLATED_MEAN")
                else:
                    return  # Drop metric entirely if no data and no history

            # Hard Bounds Checking
            bounds = self._get_bounds(path)
            if bounds and not (bounds[0] <= val <= bounds[1]):
                return  # Drop out-of-bounds metric silently

            # Spike Removal (Rolling Median)
            if len(self.history[key]) >= 3:
                current_median = statistics.median(self.history[key])
                # If value deviates by more than 40% from median, smooth it
                if current_median != 0 and abs(val - current_median) / abs(current_median) > 0.40:
                    val = current_median
                    flags.append("SPIKE_SMOOTHED_MEDIAN")

            # Update buffer with the cleaned value
            self.history[key].append(val)
            cleaned_val = round(val, 2)

            # Data Standardization (Z-Score Normalization)
            if len(self.history[key]) > 1:
                mean = statistics.mean(self.history[key])
                std = statistics.stdev(self.history[key])
                z_score = (val - mean) / std if std > 0 else 0.0
            else:
                z_score = 0.0

            # Reconstruct the dictionaries
            self._set_nested(cleaned_data, path, cleaned_val)
            self._set_nested(normalized_data, path, round(z_score, 4))
            if flags:
                self._set_nested(cleaning_flags, path, flags)

        # Extract, clean, and rebuild
        traverse(packet.get('data', {}))

        # Update packet in-place
        packet['data'] = cleaned_data
        packet['data_normalized'] = normalized_data
        packet['cleaning_flags'] = cleaning_flags

        # If everything in the packet was dropped, throw out the whole packet
        if not cleaned_data:
            return None

        return packet


def run_pipeline(input_file="simulated_raw_telemetry_100k.jsonl", output_file="cleaned_telemetry_100k.jsonl"):
    print(f"Reading from {input_file}...")
    cleaner = TelemetryCleaner(window_size=10)
    
    processed_count = 0
    dropped_count = 0
    
    with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
        for line in f_in:
            if not line.strip():
                continue
                
            raw_packet = json.loads(line)
            cleaned_packet = cleaner.process(raw_packet)
            
            # If the packet had valid data after cleaning, write it to the new file
            if cleaned_packet is not None:
                f_out.write(json.dumps(cleaned_packet) + "\n")
                processed_count += 1
            else:
                dropped_count += 1
                
    print(f"✅ Pipeline complete!")
    print(f" - Successfully cleaned & saved: {processed_count} packets")
    print(f" - Dropped entirely (bad/empty data): {dropped_count} packets")
    print(f" - Output saved to: {output_file}")


if __name__ == "__main__":
    run_pipeline()