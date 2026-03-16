# Import necessary libraries
import json                     # For converting Python dict to JSON string
import random                   # For generating random float and integer values
import time                     # For adding delays between data generations
from datetime import datetime   # For adding timestamps to the data

# Path to the log file where simulated data will be stored
log_file_path = "wind_turbine.log"

# Function to generate simulated wind turbine data for a given turbine ID
def generate_data(turbine_id):
    return {
        "Turbine_ID": f"Turbine_{turbine_id}",  # Unique ID for each turbine
        "Nacelle_Position": round(random.uniform(0, 360), 2),  # Position of nacelle in degrees
        "Wind_direction": round(random.uniform(0, 360), 2),    # Wind direction in degrees
        "Ambient_Air_temp": round(random.uniform(-30, 45), 2), # Ambient temperature in Celsius
        "Bearing_Temp": round(random.uniform(10, 70), 2),      # Temperature of turbine bearing
        "BladePitchAngle": round(random.uniform(0, 210), 2),   # Angle of blade pitch
        "GearBoxSumpTemp": round(random.uniform(20, 130), 2),  # Temperature of gearbox oil sump
        "Generator_Speed": round(random.uniform(40, 60), 2),   # Speed of the generator (rpm)
        "Hub_Speed": random.randint(1, 5),                     # Speed of the hub (arbitrary unit)
        "Power": round(random.uniform(50, 1500), 2),           # Power output in kW
        "Wind_Speed": round(random.uniform(2, 25), 2),         # Wind speed in m/s
        "GearTemp": round(random.uniform(50, 350), 2),         # Gear temperature in Celsius
        "GeneratorTemp": round(random.uniform(25, 150), 2),    # Generator temperature in Celsius
        "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Current timestamp
    }

# Start an infinite loop to simulate continuous data generation
try:
    while True:
        # Simulate data for 12 turbines
        for turbine_id in range(1, 13):
            data = generate_data(turbine_id)  # Generate data for the current turbine

            # Open the log file in append mode and write the data as a JSON line
            with open(log_file_path, "a") as logfile:
                logfile.write(json.dumps(data) + "\n")  # Append one JSON line

            time.sleep(0.5)  # Short delay between writing each turbine's data

        # Indicate that one full cycle (all turbines) has been logged
        print("One round of turbine data written...")
        time.sleep(2)  # Wait before starting the next round of data

# Allow graceful shutdown using keyboard interrupt (Ctrl+C)
except KeyboardInterrupt:
    print("Data generation stopped.")
    
# END of CODE
