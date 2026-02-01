# ============================================================
# Kafka Message Verification Consumer
# ------------------------------------------------------------
# This lightweight Kafka consumer is used to VERIFY that
# messages are successfully being published to the Kafka topic.
#
# Purpose:
# - Debugging and validation
# - Confirm producer → Kafka pipeline is working
# - Print raw messages before downstream processing
#
# NOTE:
# This consumer DOES NOT write to a database.
# It is intentionally simple and used only for inspection.
# ============================================================


# ------------------- Kafka Consumer Import -------------------

from confluent_kafka import Consumer


# ------------------- Kafka Configuration ---------------------

conf = {
    # Replace with your Kafka broker IP / hostname
    # Example: "localhost:9092"
    'bootstrap.servers': '<KAFKA_BROKER_IP>:9092',

    # Consumer group ID
    # Using a separate group ensures this consumer does not
    # interfere with the main TimescaleDB consumer offsets
    'group.id': 'windturbine-consumer-group',

    # If no committed offset exists, start from the beginning
    'auto.offset.reset': 'earliest'
}

# Create Kafka Consumer instance
consumer = Consumer(conf)


# ------------------- Topic Subscription ----------------------

# Subscribe to the wind turbine Kafka topic
consumer.subscribe(['windturbine-data'])

print("Subscribed to topic 'windturbine-data'. Listening for messages...")


# ------------------- Message Consumption Loop ----------------

try:
    # Continuously poll Kafka for new messages
    while True:
        # Poll with timeout to avoid blocking indefinitely
        msg = consumer.poll(1.0)

        if msg is None:
            # No message received during this poll interval
            continue

        if msg.error():
            # Handle Kafka-level errors (broker, network, etc.)
            print(f"Consumer error: {msg.error()}")
            continue

        # Successfully received a message
        # Decode bytes → UTF-8 string and print to console
        print(f"Received message: {msg.value().decode('utf-8')}")


# ------------------- Graceful Shutdown -----------------------

except KeyboardInterrupt:
    # Allows clean shutdown using Ctrl+C
    pass

finally:
    # Close the consumer cleanly
    # Commits final offsets and releases resources
    consumer.close()
    print("Consumer closed.")


# ------------------- End of Verification Script --------------
