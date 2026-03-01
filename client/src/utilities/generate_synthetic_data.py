import json
import random
from datetime import datetime, timedelta

NUM_PLOTS = 1500
NUM_DAYS = 60
OUTPUT_FILE = "../almond_farm_timeseries.json"


def generate_plot_timeseries(plot_id):
    records = []
    start_date = datetime(2026, 2, 1)

    for day in range(NUM_DAYS):
        date = start_date + timedelta(days=day)

        # 🎯 Controlled distribution
        r = random.random()

        if r < 0.90:
            status = "Good"
        elif r < 0.98:
            status = "Slightly Abnormal"
        else:
            status = random.choice(["Highly Abnormal", "Action"])

        # --------------------------
        # GOOD CONDITIONS
        # --------------------------
        if status == "Good":
            soil_pH = round(random.uniform(6.2, 7.4), 2)
            soil_moisture = round(random.uniform(22, 35), 2)
            ambient_temp = round(random.uniform(18, 30), 2)
            humidity = round(random.uniform(40, 70), 2)
            precipitation = round(random.uniform(0, 5), 2)
            wind_speed = round(random.uniform(1, 10), 2)

            action = "No action needed"
            reasoning = (
                f"All parameters within optimal almond thresholds "
            )

        # --------------------------
        # SLIGHTLY ABNORMAL (YELLOW)
        # --------------------------
        elif status == "Slightly Abnormal":
            soil_pH = round(random.choice([
                random.uniform(5.8, 6.0),
                random.uniform(7.6, 8.0)
            ]), 2)

            soil_moisture = round(random.choice([
                random.uniform(15, 20),
                random.uniform(45, 55)
            ]), 2)

            ambient_temp = round(random.uniform(25, 34), 2)
            humidity = round(random.uniform(30, 45), 2)
            precipitation = round(random.uniform(0, 10), 2)
            wind_speed = round(random.uniform(8, 14), 2)

            action = random.choice([
                "Increase irrigation by 10% for 24 hours",
                "Schedule soil pH reassessment within 7 days",
                "Inspect young trees for wind stress damage"
            ])

            reasoning = (
                f"Parameters slightly outside optimal band. "
            )

        # --------------------------
        # RED CONDITIONS
        # --------------------------
        else:
            red_type = random.choice(["drought", "flood", "alkaline", "acidic"])

            if red_type == "drought":
                soil_pH = round(random.uniform(6.0, 7.5), 2)
                soil_moisture = round(random.uniform(5, 12), 2)
                ambient_temp = round(random.uniform(32, 40), 2)
                humidity = round(random.uniform(10, 25), 2)
                precipitation = 0
                wind_speed = round(random.uniform(5, 15), 2)

                action = "Deploy deep irrigation cycle (6 hours) and inspect drip lines"
                reasoning = (
                    f"Severe drought detected from soil moisture, humidity, and ambient temperature"
                )

            elif red_type == "flood":
                soil_pH = round(random.uniform(6.0, 7.5), 2)
                soil_moisture = round(random.uniform(65, 85), 2)
                ambient_temp = round(random.uniform(15, 25), 2)
                humidity = round(random.uniform(75, 95), 2)
                precipitation = round(random.uniform(20, 40), 2)
                wind_speed = round(random.uniform(2, 10), 2)

                action = "Suspend irrigation for 72 hours and inspect for root hypoxia"
                reasoning = (
                    f"Flooding risk determined by soil moisture and precipitation"
                )

            elif red_type == "alkaline":
                soil_pH = round(random.uniform(8.3, 9.2), 2)
                soil_moisture = round(random.uniform(20, 35), 2)
                ambient_temp = round(random.uniform(20, 30), 2)
                humidity = round(random.uniform(40, 60), 2)
                precipitation = round(random.uniform(0, 5), 2)
                wind_speed = round(random.uniform(1, 10), 2)

                action = "Apply elemental sulfur at 500 lbs/acre"
                reasoning = (
                    f"Soil alkalinity critical"
                )

            else:  # acidic
                soil_pH = round(random.uniform(4.8, 5.4), 2)
                soil_moisture = round(random.uniform(20, 35), 2)
                ambient_temp = round(random.uniform(20, 30), 2)
                humidity = round(random.uniform(40, 60), 2)
                precipitation = round(random.uniform(0, 5), 2)
                wind_speed = round(random.uniform(1, 10), 2)

                action = "Apply agricultural lime at 1 ton/acre"
                reasoning = (
                    f"Soil acidity critical"
                )

        soil_temp = round(ambient_temp - random.uniform(1, 3), 2)

        records.append({
            "plot_id": plot_id,
            "date": date.strftime("%Y-%m-%d"),
            "soil_pH": soil_pH,
            "soil_moisture": soil_moisture,
            "ambient_temperature": ambient_temp,
            "soil_temperature": soil_temp,
            "humidity": humidity,
            "precipitation": precipitation,
            "wind_speed": wind_speed,
            "status": status,
            "action": action,
            "reasoning": reasoning
        })

    return records


def main():
    all_records = []

    for plot_id in range(NUM_PLOTS):
        all_records.extend(generate_plot_timeseries(plot_id))

    with open(OUTPUT_FILE, "w") as f:
        json.dump({"plots": all_records}, f)

    print(f"Generated {len(all_records)} total records.")
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()