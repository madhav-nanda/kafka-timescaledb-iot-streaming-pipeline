# Import Kafka Producer from the Confluent Kafka library
from confluent_kafka import Producer

# Import standard Python libraries
import json      # For parsing and dumping JSON data
import time      # For delays if needed (currently not used here)

# ------------------- Kafka Configuration -------------------

# Define Kafka configuration settings
conf = {
    'bootstrap.servers': '192.168.232.255:9092',         # Kafka broker IP address and port
    'client.id': 'wind_turbine_fullfile_producer'        # Unique ID for this Kafka client
}

# Create a Kafka producer instance using the above configuration
producer = Producer(conf)

# Define the topic to which messages will be published
TOPIC_NAME = "windturbine-data" # Create the topic name on Kafka server

# Create a topic
# bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server <broker_host>:9092 --partitions <n> --replication-factor <n>

# ------------------- Log File Configuration -------------------

# Path to the log file containing the wind turbine data (produced by earlier script)
log_file_path = "wind_turbine.log"  # Update path if your file is located elsewhere

# ------------------- Kafka Delivery Callback -------------------

# Callback function to report the result of message delivery (success or error)
def delivery_report(err, msg):
    if err:
        print(f"Kafka delivery failed: {err}")  # Print error if delivery fails
    else:
        # Print success message with topic and partition info
        print(f"Kafka message delivered to {msg.topic()} [{msg.partition()}]")

# ------------------- Kafka Message Production -------------------

try:
    # Open the log file in read mode
    with open(log_file_path, "r") as logfile:
        # Iterate over each line (JSON object) in the log file
        for line in logfile:
            line = line.strip()  # Remove leading/trailing whitespace
            if not line:
                continue  # Skip empty lines

            try:
                # Parse the JSON line into a Python dictionary
                json_data = json.loads(line)

                # Produce the message to Kafka, serialize it back to JSON string
                # Attach delivery report callback for acknowledgment
                producer.produce(
                    TOPIC_NAME,
                    value = json.dumps(json_data),
                    callback = delivery_report
                )

                # Non-blocking call to serve delivery reports
                producer.poll(0)

            except json.JSONDecodeError:
                # Handle any line that fails to parse as JSON
                print(f"Invalid JSON line skipped: {line}")

    # Block until all messages are sent to Kafka
    producer.flush()
    print("All messages sent!")

# ------------------- Graceful Exit -------------------

except KeyboardInterrupt:
    # Handle manual stop (Ctrl+C) and ensure all messages are sent
    print("\nStopping ingestion...")
    producer.flush()
    
# END of CODE
