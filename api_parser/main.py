import json
import math
import logging

logging.basicConfig(level=logging.INFO, format='\n%(message)s')
logger = logging.getLogger(__name__)

class IntelligenceHub:
    def __init__(self):
        self.solar_history = {} 
        self.last_prescription_time = {} 
        self.prescriptions_log = []

    def get_metric(self, observations, spn_target, key="value"):
        for obs in observations:
            if obs.get("isobus_header", {}).get("spn") == spn_target:
                return obs.get(key, 0.0)
        return 0.0

    def process_packet(self, packet, packet_index):
        sector = packet.get("location", {}).get("sector", "Unknown")
        timestamp_day = packet.get("timestamp", 0)
        obs = packet.get("observations", [])

        if sector not in self.solar_history:
            self.solar_history[sector] = []

        # Extract values via SPNs
        temp = self.get_metric(obs, "7003", "value")
        moisture = self.get_metric(obs, "7002", "value")
        wind_speed = self.get_metric(obs, "7007", "value")
        solar = self.get_metric(obs, "7008", "value")
        rain = self.get_metric(obs, "7005", "value")

        # Extract normalized Z-scores
        flow_z = self.get_metric(obs, "1092", "value_normalized")
        pressure_z = self.get_metric(obs, "1093", "value_normalized")
        temp_z = self.get_metric(obs, "7003", "value_normalized")

        self.solar_history[sector].append(solar)
        if len(self.solar_history[sector]) > 10:
            self.solar_history[sector].pop(0)

        d = math.sqrt(flow_z**2 + pressure_z**2 + temp_z**2)

        self.analyze_and_prescribe(sector, timestamp_day, packet_index, d, wind_speed, solar, rain, moisture, flow_z, pressure_z, temp_z)

    def analyze_and_prescribe(self, sector, timestamp_day, packet_index, d, wind_speed, solar, rain, moisture, flow_z, pressure_z, temp_z):
        last_time = self.last_prescription_time.get(sector)
        if last_time is not None and (timestamp_day - last_time) < 1:
            return

        prescription = None

        if temp_z > 1.5 and moisture < 35.0:
            prescription = {"diagnosis": "Severe Heat Stress", "action": "Trigger emergency cooling irrigation cycle.", "confidence_score": 0.94, "roi_impact": "Critical"}
        elif d > 2.5:
            avg_solar = sum(self.solar_history[sector]) / len(self.solar_history[sector]) if self.solar_history[sector] else 0.0
            if wind_speed > 8.0 and abs(flow_z) < 1.0:
                prescription = {"diagnosis": "Windward Drying", "action": "Increase flow by 15%.", "confidence_score": 0.88, "roi_impact": "Medium"}
            elif flow_z < -1.0 and pressure_z > 1.0:
                prescription = {"diagnosis": "Mineral Scaling", "action": "Execute targeted acid flush.", "confidence_score": 0.95, "roi_impact": "High"}
            elif avg_solar > 0 and solar < (0.70 * avg_solar):
                prescription = {"diagnosis": "Solar Shadow", "action": "Reduce flow by 20%.", "confidence_score": 0.82, "roi_impact": "Low"}
            elif rain > 5.0 and moisture > 25.0:
                prescription = {"diagnosis": "Precipitation Buffer", "action": "Pause irrigation.", "confidence_score": 0.99, "roi_impact": "High"}

        if prescription:
            work_order = {
                "packet_index": packet_index, "timestamp": timestamp_day, "sector": sector, 
                "diagnosis": prescription["diagnosis"], "action": prescription["action"], 
                "confidence_score": prescription["confidence_score"], "roi_impact": prescription["roi_impact"], 
                "trigger_metrics": {"D_score": round(d, 2)}
            }
            self.prescriptions_log.append(work_order)
            self.last_prescription_time[sector] = timestamp_day

    def save_outputs(self, output_file="prescriptions.jsonl"):
        with open(output_file, "w") as f:
            for p in self.prescriptions_log:
                f.write(json.dumps(p) + "\n")
        print(f"✅ MAIN: Analysis complete. Exported {len(self.prescriptions_log)} prescriptions to {output_file}")

def run_pipeline():
    hub = IntelligenceHub()
    count = 0
    try:
        with open("cleaned_telemetry_100k.jsonl", 'r') as f:
            for line in f:
                if not line.strip(): continue
                count += 1 
                hub.process_packet(json.loads(line), count)
    except FileNotFoundError:
        print("❌ ERROR: cleaned_telemetry_100k.jsonl not found. Run cleaner.py first.")
        return
                
    hub.save_outputs()

if __name__ == "__main__":
    run_pipeline()