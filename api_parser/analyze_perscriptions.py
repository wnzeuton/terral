import json
from collections import Counter

def analyze_prescriptions(file_path="prescriptions.jsonl"):
    prescriptions = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    prescriptions.append(json.loads(line))
    except FileNotFoundError:
        print(f"❌ File {file_path} not found. Run main.py first.")
        return

    total_prescriptions = len(prescriptions)
    if total_prescriptions == 0:
        print("✅ Pipeline ran cleanly. No anomalies detected.")
        return

    diagnoses_tally = Counter([p.get("diagnosis", "Unknown") for p in prescriptions])
    sector_tally = Counter([p.get("sector", "Unknown") for p in prescriptions])
    
    print("\n" + "="*60)
    print("📊 AURA-AG PRESCRIPTION ANALYSIS REPORT")
    print("="*60)
    print(f"Total Work Orders Issued: {total_prescriptions}\n")
    
    print("🚨 Breakdown by Diagnosis:")
    for diagnosis, count in diagnoses_tally.items():
        percentage = (count / total_prescriptions) * 100
        print(f"  - {diagnosis}: {count} ({percentage:.1f}%)")
        
    print("\n📍 Breakdown by Sector:")
    for sector, count in sorted(sector_tally.items(), key=lambda x: str(x[0])):
        print(f"  - Sector {sector}: {count} incidents")

    print("\n⏱️ Chronological Timeline:")
    print("-" * 60)
    
    sorted_prescriptions = sorted(prescriptions, key=lambda p: p.get("timestamp", 0))
    for p in sorted_prescriptions:
        t = p.get("timestamp", 0)
        diag = p.get("diagnosis", "Unknown Issue")
        sec = p.get("sector", "Unknown")
        print(f"  [Day {t:>4}] ➡️ {diag} triggered in Sector {sec}")
        
    print("="*60 + "\n")

if __name__ == "__main__":
    analyze_prescriptions()