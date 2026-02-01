# ============================================================
# Wind Turbine Telemetry Data Generator
# ------------------------------------------------------------
# This script simulates real-time wind turbine sensor data
# and writes it to a log file in JSON Lines format.
#
# Purpose:
# - Mimic IoT sensor telemetry from multiple wind turbines
# - Generate continuous, timestamped data
# - Serve as an upstream data source for Kafka ingestion
#
# Output:
# - One JSON object per line in wind_turbine.log
#
# Tech Stack:
# - Python
# - JSON
# - File-based streaming simulation
# ============================================================


# ------------------- Standard Library Imports ----------------

import json                     # Convert Python dictionaries to JSON strings
import random                   # Generate random sensor values
import time                     # Control data generation rate
from datetime import datetime   # Add timestamps to each record


# ------------------- Log File Configuration -----------------

# File where simulated wind turbine telemetry is written
# Each line represents one sensor event (JSON format)
log_file_path = "wind_turbine.log"


# ------------------- Data Generation Function ----------------

def generate_data(turbine_id):
    """
    Generate simulated telemetry data for a single wind turbine.

    Parameters:
    - turbine_id (int): Unique numeric identifier for the turbine

    Returns:
    - dict: Wind turbine sensor data with timestamp
    """
    return {
        "Turbine_ID": f"Turbine_{turbine_id}",          # Unique turbine identifier
        "Nacelle_Position": round(random.uniform(0, 360), 2),   # Nacelle angle (degrees)
        "Wind_direction": round(random.uniform(0, 360), 2),     # Wind direction (degrees)
        "Ambient_Air_temp": round(random.uniform(-30, 45), 2),  # Ambient air temperature (°C)
        "Bearing_Temp": round(random.uniform(10, 70), 2),       # Bearing temperature (°C)
        "BladePitchAngle": round(random.uniform(0, 210), 2),    # Blade pitch angle
        "GearBoxSumpTemp": round(random.uniform(20, 130), 2),   # Gearbox sump temperature (°C)
        "Generator_Speed": round(random.uniform(40, 60), 2),    # Generator speed (RPM)
        "Hub_Speed": random.randint(1, 5),                      # Hub rotational speed
        "Power": round(random.uniform(50, 1500), 2),            # Power output (kW)
        "Wind_Speed": round(random.uniform(2, 25), 2),          # Wind speed (m/s)
        "GearTemp": round(random.uniform(50, 350), 2),          # Gear temperature (°C)
        "GeneratorTemp": round(random.uniform(25, 150), 2),     # Generator temperature (°C)
        "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Event timestamp
    }


# ------------------- Continuous Data Simulation ----------------

try:
    # Infinite loop to simulate continuous IoT data stream
    while True:

        # Generate data for 12 turbines per cycle
        for turbine_id in range(1, 13):

            # Generate telemetry data for current turbine
            data = generate_data(turbine_id)

            # Append data to log file as a single JSON line
            with open(log_file_path, "a") as logfile:
                logfile.write(json.dumps(data) + "\n")

            # Small delay between turbine events
            time.sleep(0.5)

        # Log completion of one full turbine data cycle
        print("One round of turbine data written...")

        # Pause before starting the next cycle
        time.sleep(2)


# ------------------- Graceful Shutdown ------------------------

except KeyboardInterrupt:
    # Allows clean shutdown when user presses Ctrl+C
    print("Data generation stopped.")


# ------------------- End of Generator Script ------------------
