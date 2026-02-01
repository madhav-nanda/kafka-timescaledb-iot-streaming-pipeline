# ============================================================
# Wind Turbine Data Kafka Producer
# ------------------------------------------------------------
# This script reads simulated wind turbine telemetry data
# (JSON lines) from a log file and publishes each record
# to an Apache Kafka topic using the Confluent Kafka client.
#
# Purpose:
# - Demonstrate file-based ingestion into Kafka
# - Validate JSON records before publishing
# - Ensure reliable delivery using callbacks and flush
#
# Tech Stack:
# - Python
# - Apache Kafka (Confluent client)
# - Linux Virtual Machine
# ============================================================


# ------------------- Kafka Client Import --------------------

# Import Kafka Producer from the Confluent Kafka library
# (industry-grade, high-performance Kafka client)
from confluent_kafka import Producer


# ------------------- Standard Library Imports ----------------

import json      # Used to parse and serialize JSON data
import time      # Used for optional delays / future throttling


# ------------------- Kafka Configuration --------------------

# Kafka producer configuration
# - bootstrap.servers: Kafka broker running inside VM
# - client.id: Logical name for this producer instance
conf = {
    'bootstrap.servers': 'KAFKA_BROKER_IP:9092',   # Replace with your Kafka broker IP / hostname (example: "localhost:9092")
    'client.id': 'wind_turbine_fullfile_producer'
}

# Create Kafka producer instance
producer = Producer(conf)


# ------------------- Kafka Topic ----------------------------

# Kafka topic to which turbine data will be published
# (Topic must exist on the Kafka broker)
TOPIC_NAME = "windturbine-data"

# Example topic creation command (run on Kafka server):
# bin/kafka-topics.sh --create \
#   --topic windturbine-data \
#   --bootstrap-server <broker_host>:9092 \
#   --partitions <n> \
#   --replication-factor <n>


# ------------------- Input Data Source ----------------------

# Path to the log file containing JSON-formatted wind turbine data
# Each line represents one telemetry event
log_file_path = "wind_turbine.log"


# ------------------- Delivery Report Callback ----------------

def delivery_report(err, msg):
    """
    Callback function invoked by Kafka client after message delivery.

    Parameters:
    - err: Error information if delivery failed
    - msg: Kafka message metadata (topic, partition, offset)

    Purpose:
    - Confirms successful delivery
    - Helps with debugging and reliability verification
    """
    if err:
        print(f"Kafka delivery failed: {err}")
    else:
        print(f"Kafka message delivered to {msg.topic()} [{msg.partition()}]")


# ------------------- Message Production Logic ----------------

try:
    # Open the turbine log file in read mode
    with open(log_file_path, "r") as logfile:

        # Iterate over each JSON line in the file
        for line in logfile:

            # Remove leading/trailing whitespace
            line = line.strip()

            # Skip empty lines to avoid processing errors
            if not line:
                continue

            try:
                # Parse JSON string into Python dictionary
                json_data = json.loads(line)

                # Produce message to Kafka topic
                # - value is serialized back to JSON string
                # - callback confirms delivery status
                producer.produce(
                    TOPIC_NAME,
                    value=json.dumps(json_data),
                    callback=delivery_report
                )

                # Serve delivery reports (non-blocking)
                producer.poll(0)

            except json.JSONDecodeError:
                # Handle invalid or corrupted JSON entries safely
                print(f"Invalid JSON line skipped: {line}")

    # Ensure all buffered messages are delivered before exiting
    producer.flush()
    print("All messages sent!")


# ------------------- Graceful Shutdown ----------------------

except KeyboardInterrupt:
    # Handle manual interruption (Ctrl+C)
    # Ensures pending messages are delivered before exit
    print("\nStopping ingestion...")
    producer.flush()


# ------------------- End of Producer Script -----------------
