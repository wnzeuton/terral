import json
import random

def set_nested(d, path, val):
    keys = path.split('.')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = val

def generate_telemetry_dataset(output_file="simulated_raw_telemetry_100k.jsonl", num_packets=10000):
    with open('mapping_config.json', 'r') as f:
        config = json.load(f)

    base_values = {
        "1092": 100.0, "1093": 300.0, "110": 1.0, "7001": 6.5, 
        "7002": 30.0, "7003": 25.0, "7004": 50.0, "7005": 0.0, 
        "7006": 1.5, "7007": 3.0, "7008": 600.0, "7009": 50.0, 
        "7010": 20.0, "7011": 20.0
    }

    std_devs = {k: v * 0.05 for k, v in base_values.items()}
    std_devs["110"] = 0.0 
    std_devs["7001"] = 0.2 
    std_devs["7003"] = 2.0 
    std_devs["7004"] = 5.0 

    sources = list(config.keys())

    with open(output_file, 'w') as f_out:
        for _ in range(num_packets):
            source = random.choice(sources)
            mapping = config[source]["mappings"]
            
            payload_data = {}
            for spn, map_info in mapping.items():
                path = map_info["path"]
                multiplier = map_info.get("multiplier", 1.0)
                
                std_val = random.gauss(base_values[spn], std_devs[spn])
                if spn in ["1092", "1093", "7002", "7004", "7005", "7006", "7007", "7008"] and std_val < 0:
                    std_val = 0.0
                    
                rand_val = random.random()
                if rand_val < 0.01:
                    std_val *= random.choice([0.2, 1.8])
                elif rand_val < 0.015:
                    if spn == "7001": std_val = 15.0      
                    elif spn == "7002": std_val = 110.0   
                    elif spn == "7003": std_val = 70.0    
                
                if source == "CNH_Industrial_v2" and spn in ["7003", "7011"]:
                    raw_val = (std_val / 0.5556) + 32.0
                else:
                    raw_val = std_val / multiplier
                    
                set_nested(payload_data, path, round(raw_val, 3))
                
            packet = {
                "source": source,
                "location": {"sector": random.choice(["1A", "1B", "2A", "2B"]), "crop": "Almonds"},
                "data": payload_data
            }
            f_out.write(json.dumps(packet) + "\n")
            
    print(f"✅ Generated {num_packets} packets in {output_file}")

if __name__ == "__main__":
    generate_telemetry_dataset()