import json
import statistics
from collections import deque

class TelemetryCleaner:
    def __init__(self, window_size=10):
        self.window_size = window_size
        self.history = {} 
        
        self.hard_bounds = {
            "7001": (0.0, 14.0),     # pH
            "7002": (0.0, 100.0),    # Moisture %
            "7003": (-20.0, 60.0),   # Temp C
            "7004": (0.0, 100.0),    # Humidity %
            "7005": (0.0, 500.0)     # Precip mm
        }

    def process(self, packet):
        sector = packet.get('location', {}).get('sector')
        if sector is None: return None

        cleaned_observations = []

        for obs in packet.get("observations", []):
            spn = obs.get("isobus_header", {}).get("spn")
            val = obs.get("value")
            if not spn or val is None: continue

            key = (sector, spn)
            if key not in self.history:
                self.history[key] = deque(maxlen=self.window_size)

            # Hard Bounds Checking
            bounds = self.hard_bounds.get(spn)
            if bounds and not (bounds[0] <= val <= bounds[1]):
                continue 

            # Spike Removal
            if len(self.history[key]) >= 3:
                current_median = statistics.median(self.history[key])
                if current_median != 0 and abs(val - current_median) / abs(current_median) > 0.40:
                    val = current_median
                    obs["cleaning_flag"] = "SPIKE_SMOOTHED"

            self.history[key].append(val)
            obs["value"] = round(val, 2)

            # Z-Score Normalization
            if len(self.history[key]) > 1:
                mean = statistics.mean(self.history[key])
                std = statistics.stdev(self.history[key])
                z_score = (val - mean) / std if std > 0 else 0.0
            else:
                z_score = 0.0

            obs["value_normalized"] = round(z_score, 4)
            cleaned_observations.append(obs)

        if not cleaned_observations:
            return None

        packet["observations"] = cleaned_observations
        return packet

def run_cleaner(input_file="parsed_telemetry_100k.jsonl", output_file="cleaned_telemetry_100k.jsonl"):
    cleaner = TelemetryCleaner()
    processed = 0
    with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
        for line in f_in:
            if not line.strip(): continue
            parsed_packet = json.loads(line)
            cleaned_packet = cleaner.process(parsed_packet)
            if cleaned_packet:
                f_out.write(json.dumps(cleaned_packet) + "\n")
                processed += 1
                
    print(f"✅ CLEANER: Scrubbed and normalized {processed} packets into {output_file}")

if __name__ == "__main__":
    run_cleaner()