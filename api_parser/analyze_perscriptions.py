import json
from collections import Counter

def analyze_prescriptions(file_path="prescriptions.json"):
    try:
        with open(file_path, 'r') as f:
            prescriptions = json.load(f)
    except FileNotFoundError:
        print(f"❌ File {file_path} not found. Ensure you ran main.py first.")
        return

    total_prescriptions = len(prescriptions)
    
    if total_prescriptions == 0:
        print("✅ Pipeline ran cleanly. No anomalies detected.")
        return

    diagnoses_tally = Counter([p["diagnosis"] for p in prescriptions])
    sector_tally = Counter([p["sector"] for p in prescriptions])
    
    print("\n" + "="*60)
    print("📊 AURA-AG PRESCRIPTION ANALYSIS REPORT")
    print("="*60)
    print(f"Total Work Orders Issued: {total_prescriptions}\n")
    
    print("🚨 Breakdown by Diagnosis:")
    for diagnosis, count in diagnoses_tally.items():
        percentage = (count / total_prescriptions) * 100
        print(f"  - {diagnosis}: {count} ({percentage:.1f}%)")
        
    print("\n📍 Breakdown by Sector:")
    for sector, count in sector_tally.items():
        print(f"  - Sector {sector}: {count} incidents")

    print("\n⏱️ Chronological Timeline (When Anomalies Occurred):")
    print("-" * 60)
    # Sort by packet_index just in case, though they should already be in order
    sorted_prescriptions = sorted(prescriptions, key=lambda x: x.get("packet_index", 0))
    
    for p in sorted_prescriptions:
        idx = p.get("packet_index", "Unknown")
        diag = p.get("diagnosis", "Unknown Issue")
        sec = p.get("sector", "Unknown")
        print(f"  [Packet #{idx:>6}] ➡️ {diag} triggered in Sector {sec}")
        
    print("="*60 + "\n")

if __name__ == "__main__":
    analyze_prescriptions()