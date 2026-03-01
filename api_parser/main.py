import json
import math
import logging
from datetime import datetime, timedelta
from parser import AuraAgParser
from cleaner import TelemetryCleaner

logging.basicConfig(level=logging.INFO, format='\n%(message)s')
logger = logging.getLogger(__name__)

class IntelligenceHub:
    def __init__(self):
        self.parser = AuraAgParser(config_path="mapping_config.json")
        self.cleaner = TelemetryCleaner(window_size=10)
        self.sector_state = {}
        self.solar_history = {} 
        self.last_prescription_time = {} 
        self.cleaned_log = []
        self.prescriptions_log = []

    def calculate_vpd(self, temp_c, rh_pct):
        svp = 0.61078 * math.exp((17.27 * temp_c) / (temp_c + 237.3))
        avp = svp * (rh_pct / 100.0)
        return round(svp - avp, 2)

    # ADDED: packet_index parameter
    def process_payload(self, source_name, raw_payload, location_context, packet_index=0):
        sector = location_context.get("sector", "Unknown")
        if sector not in self.sector_state:
            self.sector_state[sector] = {}
            self.solar_history[sector] = []

        observations = self.parser.process_payload(source_name, raw_payload, location_context)
        timestamp = datetime.utcnow().isoformat() + "Z"
        parsed_spns = {obs['isobus_header']['spn']: obs for obs in observations}
        
        if "7006" not in parsed_spns or parsed_spns["7006"]["value"] is None:
            temp_obs = parsed_spns.get("7003")
            rh_obs = parsed_spns.get("7004")
            if temp_obs and temp_obs.get("value") is not None and rh_obs and rh_obs.get("value") is not None:
                vpd_val = self.calculate_vpd(temp_obs["value"], rh_obs["value"])
                vpd_obs = {
                    "isobus_header": {"spn": "7006", "label": "Vapor_Pressure_Deficit", "unit": "kPa", "data_type": "instantaneous"},
                    "value": vpd_val,
                    "source_metadata": {"origin": "Calculated_Local", "protocol": "Internal", "timestamp": timestamp},
                    "location_context": location_context
                }
                observations.append(vpd_obs)

        for obs in observations:
            obs["source_metadata"]["timestamp"] = timestamp 
            cleaned_packet = self.cleaner.process(obs)
            
            if cleaned_packet and cleaned_packet.get("value") is not None:
                self.cleaned_log.append(cleaned_packet)
                spn = cleaned_packet['isobus_header']['spn']
                self.sector_state[sector][spn] = cleaned_packet

                if spn == "7008":
                    self.solar_history[sector].append(cleaned_packet["value"])
                    if len(self.solar_history[sector]) > 10:
                        self.solar_history[sector].pop(0)

        # Passing packet_index to the analysis stage
        self.analyze_and_prescribe(sector, timestamp, packet_index)

    def analyze_and_prescribe(self, sector, timestamp, packet_index):
        state = self.sector_state.get(sector, {})
        last_time = self.last_prescription_time.get(sector)
        current_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        
        if last_time and (current_time - last_time) < timedelta(minutes=10):
            return

        flow_z = state.get("1092", {}).get("value_normalized", 0.0)
        pressure_z = state.get("1093", {}).get("value_normalized", 0.0)
        vpd_z = state.get("7006", {}).get("value_normalized", 0.0)
        
        d = math.sqrt(flow_z**2 + pressure_z**2 + vpd_z**2)
        prescription = None

        if d > 2.5:
            wind_speed = state.get("7007", {}).get("value", 0.0)
            solar = state.get("7008", {}).get("value", 0.0)
            rain = state.get("7005", {}).get("value", 0.0)
            moisture = state.get("7002", {}).get("value", 0.0)
            avg_solar = sum(self.solar_history[sector]) / len(self.solar_history[sector]) if self.solar_history[sector] else 0.0

            if wind_speed > 8.0 and abs(flow_z) < 1.0:
                prescription = {"diagnosis": "Windward Drying", "action": "Increase flow by 15% to mitigate edge-effect transpiration.", "confidence_score": 0.88, "roi_impact": "Medium"}
            elif flow_z < -1.0 and pressure_z > 1.0:
                prescription = {"diagnosis": "Mineral Scaling", "action": "Execute targeted 10-minute acid flush to restore conductance.", "confidence_score": 0.95, "roi_impact": "High"}
            elif avg_solar > 0 and solar < (0.70 * avg_solar):
                prescription = {"diagnosis": "Solar Shadow", "action": "Reduce flow by 20% to prevent basin-zone waterlogging.", "confidence_score": 0.82, "roi_impact": "Low"}
            elif rain > 5.0 and moisture > 25.0:
                prescription = {"diagnosis": "Precipitation Buffer", "action": "Pause irrigation; leverage natural rainfall.", "confidence_score": 0.99, "roi_impact": "High"}

        if prescription:
            work_order = {
                "packet_index": packet_index, # ADDED: Saving the exact packet number
                "timestamp": timestamp, 
                "sector": sector, 
                "diagnosis": prescription["diagnosis"],
                "action": prescription["action"], 
                "confidence_score": prescription["confidence_score"],
                "roi_impact": prescription["roi_impact"], 
                "trigger_metrics": {"D_score": round(d, 2)}
            }
            self.prescriptions_log.append(work_order)
            self.last_prescription_time[sector] = current_time
            # ADDED: Live terminal feedback so you know it isn't frozen
            logger.info(f"💊 [PRESCRIPTION ISSUED] Packet #{packet_index} | Sector {sector} | {prescription['diagnosis']}")

    def save_outputs(self):
        with open("clean_telemetry.json", "w") as f:
            json.dump(self.cleaned_log, f, indent=2)
        with open("prescriptions.json", "w") as f:
            json.dump(self.prescriptions_log, f, indent=2)
        logger.info("💾 Saved logs to clean_telemetry.json and prescriptions.json")


def run_pipeline_simulation():
    hub = IntelligenceHub()
    logger.info("🚜 STARTING AURA-AG INTELLIGENCE HUB MASS SIMULATION 🚜")
    
    file_path = "simulated_raw_telemetry_100k.jsonl"
    count = 0
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                count += 1  # Increment first so it starts at Packet #1
                p = json.loads(line)
                # Pass the count as the packet_index
                hub.process_payload(p["source"], p["data"], p["location"], count)
                
                if count % 10000 == 0:
                    logger.info(f"⚙️ Processed {count} packets so far...")
    except FileNotFoundError:
        logger.error(f"❌ Could not find {file_path}. Please run generate_data.py first.")
        return
                
    hub.save_outputs()
    logger.info(f"✅ Simulation Complete. Processed {count} total packets. Logs exported.")

if __name__ == "__main__":
    run_pipeline_simulation()