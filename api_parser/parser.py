import json

class AuraAgParser:
    def __init__(self):
        # Maps common words found in raw data to standardized ISOBUS SPNs
        self.keyword_to_spn = {
            "ph": "7001", "moisture": "7002", "ground_water": "7002",
            "temp": "7003", "humidity": "7004", "precip": "7005", "rain": "7005",
            "vpd": "7006", "wind": "7007", "solar": "7008",
            "flow": "1092", "pressure": "1093"
        }

    def _extract_metrics(self, data_node, current_path=""):
        """Recursively flattens nested dicts into a flat key-value list."""
        extracted = {}
        for k, v in data_node.items():
            path = f"{current_path}.{k}" if current_path else k
            if isinstance(v, dict):
                extracted.update(self._extract_metrics(v, path))
            else:
                extracted[path] = v
        return extracted

    def parse_packet(self, raw_packet):
        flattened_data = self._extract_metrics(raw_packet.get("data", {}))
        observations = []

        for path, value in flattened_data.items():
            assigned_spn = None
            path_lower = path.lower()
            
            # Map path to SPN based on keywords
            for keyword, spn in self.keyword_to_spn.items():
                if keyword in path_lower:
                    assigned_spn = spn
                    break
            
            # Fallback: if the path itself is just the SPN number
            if not assigned_spn and any(char.isdigit() for char in path):
                assigned_spn = ''.join(filter(str.isdigit, path))

            if assigned_spn:
                observations.append({
                    "isobus_header": {"spn": assigned_spn},
                    "value": value,
                    "original_path": path
                })

        parsed_packet = {
            "timestamp": raw_packet.get("timestamp", 0),
            "source": raw_packet.get("source", "Unknown"),
            "location": raw_packet.get("location", {}),
            "observations": observations
        }
        return parsed_packet

def run_parser(input_file="simulated_raw_telemetry_100k.jsonl", output_file="parsed_telemetry_100k.jsonl"):
    parser = AuraAgParser()
    processed = 0
    with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
        for line in f_in:
            if not line.strip(): continue
            raw_packet = json.loads(line)
            parsed_packet = parser.parse_packet(raw_packet)
            f_out.write(json.dumps(parsed_packet) + "\n")
            processed += 1
            
    print(f"✅ PARSER: Standardized {processed} packets into {output_file}")

if __name__ == "__main__":
    run_parser()