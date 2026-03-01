import json
import random
import math
import os

def set_nested(d, path, val):
    keys = path.split('.')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = val

def generate_telemetry_dataset(output_file="simulated_raw_telemetry_100k.jsonl", num_packets=10000):
    # Fallback config in case mapping_config.json is missing
    default_config = {
        "John_Deere_v1": {"mappings": {"7003": {"path": "environment.temperature"}, "7002": {"path": "soil.moisture"}, "1092": {"path": "irrigation.flow"}}},
        "CNH_Industrial_v2": {"mappings": {"7003": {"path": "ambient_temp_f"}, "7002": {"path": "ground_water_pct"}, "1093": {"path": "pump_pressure_psi"}}}
    }

    if os.path.exists('mapping_config.json'):
        with open('mapping_config.json', 'r') as f:
            config = json.load(f)
    else:
        config = default_config

    base_values = {
        "1092": 100.0, "1093": 300.0, "110": 1.0, "7001": 6.5, 
        "7002": 30.0, "7003": 25.0, "7004": 50.0, "7005": 0.0, 
        "7006": 1.5, "7007": 3.0, "7008": 600.0, "7009": 50.0, 
        "7010": 20.0, "7011": 20.0
    }

    std_devs = {k: v * 0.05 for k, v in base_values.items()}
    std_devs["110"] = 0.0; std_devs["7001"] = 0.2 
    std_devs["7003"] = 2.0; std_devs["7004"] = 5.0 

    sources = list(config.keys())
    packets_per_day = 500 
    
    start_heatwave = int(num_packets * 0.25)
    end_heatwave = int(num_packets * 0.75)

    with open(output_file, 'w') as f_out:
        for i in range(num_packets):
            source = random.choice(sources)
            mapping = config[source]["mappings"]
            current_day = i // packets_per_day
            
            # Heatwave Intensity
            heat_intensity = 0.0
            if start_heatwave <= i <= end_heatwave:
                progress = (i - start_heatwave) / (end_heatwave - start_heatwave)
                heat_intensity = math.sin(progress * math.pi)
            
            payload_data = {}
            for spn, map_info in mapping.items():
                path = map_info["path"]
                multiplier = map_info.get("multiplier", 1.0)
                current_base = base_values.get(spn, 10.0)
                
                # Apply heatwave stress
                if spn in ["110", "7003", "7011"]:
                    current_base += (20.0 * heat_intensity) 
                elif spn in ["7002", "7004"]: 
                    current_base *= (1.0 + 0.3 * heat_intensity) 
                
                std_val = random.gauss(current_base, std_devs.get(spn, 1.0))
                if spn in ["1092", "1093", "7002", "7004", "7005", "7006", "7007", "7008"] and std_val < 0:
                    std_val = 0.0
                    
                if source == "CNH_Industrial_v2" and spn in ["7003", "7011"]:
                    raw_val = (std_val / 0.5556) + 32.0 # F to C conversion mock
                else:
                    raw_val = std_val / multiplier
                    
                set_nested(payload_data, path, round(raw_val, 3))
                
            packet = {
                "timestamp": current_day,
                "source": source,
                "location": {"sector": random.randint(0, 1500), "crop": "Almonds"},
                "data": payload_data
            }
            f_out.write(json.dumps(packet) + "\n")
            
    print(f"✅ GENERATOR: Created {num_packets} packets in {output_file}")

if __name__ == "__main__":
    generate_telemetry_dataset()