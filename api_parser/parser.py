import json
import logging
from datetime import datetime
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

# --- HARDENED UNIFIED OBSERVATION SCHEMA ---
OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "isobus_header": {
            "type": "object", 
            "properties": {
                "spn": {"type": "string"}, 
                "label": {"type": "string"},
                "unit": {"type": "string"},
                "data_type": {"type": "string", "enum": ["instantaneous", "aggregated"]}
            },
            "required": ["spn", "label", "unit", "data_type"]
        },
        "value": {"type": ["number", "null"]},
        "source_metadata": {"type": "object"},
        "location_context": {
            "type": "object",
            "properties": {
                "sector": {"type": "string"},
                "crop": {"type": "string"}
            },
            "required": ["sector", "crop"]
        }
    },
    "required": ["isobus_header", "value", "source_metadata", "location_context"]
}

class AuraAgParser:
    def __init__(self, config_path="mapping_config.json"):
        with open(config_path, 'r') as f:
            self.manifest = json.load(f)

    @staticmethod
    def get_nested_value(data, path):
        keys = path.split('.')
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def process_payload(self, source_name, raw_payload, location_context):
        if source_name not in self.manifest:
            logger.error(f"❌ Unknown source: {source_name}. Skipping.")
            return []

        config = self.manifest[source_name]
        unified_observations = []

        for spn, mapping in config["mappings"].items():
            raw_val = self.get_nested_value(raw_payload, mapping["path"])
            
            # Allow None values to pass through for the cleaner to interpolate later
            converted_val = None
            if raw_val is not None:
                converted_val = float(raw_val) * mapping.get("multiplier", 1.0)
                if source_name == "CNH_Industrial_v2" and spn == "7003":
                    converted_val = (float(raw_val) - 32.0) * mapping.get("multiplier", 0.5556)
                converted_val = round(converted_val, 2)

            observation = {
                "isobus_header": {
                    "spn": spn,
                    "label": mapping["label"],
                    "unit": mapping["unit"],
                    "data_type": mapping["data_type"]
                },
                "value": converted_val,
                "source_metadata": {
                    "origin": source_name,
                    "protocol": config["context"]["protocol"],
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                },
                "location_context": location_context
            }
            
            try:
                validate(instance=observation, schema=OUTPUT_SCHEMA)
                unified_observations.append(observation)
            except ValidationError as e:
                logger.error(f"❌ Schema failed for SPN {spn}: {e.message}")

        return unified_observations