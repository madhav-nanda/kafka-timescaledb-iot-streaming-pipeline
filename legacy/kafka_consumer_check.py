# Import the Kafka Consumer from Confluent Kafka library
from confluent_kafka import Consumer

# ---------------- Kafka Configuration ----------------

# Dictionary of Kafka consumer configuration parameters
conf = {
    'bootstrap.servers': '192.168.232.255',       # Kafka broker's IP and port
    'group.id': 'windturbine-consumer-group',          # Consumer group ID for coordinating consumption
    'auto.offset.reset': 'earliest'                    # Start from the beginning of the topic if no previous offset is stored
}

# Create a Kafka Consumer instance with the above config
consumer = Consumer(conf)

# ---------------- Topic Subscription ----------------

# Subscribe the consumer to the topic 'windturbine-data'
consumer.subscribe(['windturbine-data'])

# Notify that the consumer has started listening
print("Subscribed! Listening for messages...")

# ---------------- Message Consumption Loop ----------------

try:
    # Infinite loop to continuously poll messages
    while True:
        # Poll for a new message (waits up to 1.0 second)
        msg = consumer.poll(1.0)

        if msg is None:
            # No message received within timeout, continue polling
            continue

        if msg.error():
            # If there's an error with the message, print and skip
            print("Consumer error: {}".format(msg.error()))
            continue

        # Successfully received a message, decode from bytes to UTF-8 string and print
        print(f"Received message: {msg.value().decode('utf-8')}")

# ---------------- Graceful Shutdown ----------------

# Capture Ctrl+C or interruption to exit gracefully
except KeyboardInterrupt:
    pass

finally:
    # Close the consumer cleanly and commit the final offset
    consumer.close()
    print("Consumer closed.")
    
# END OF CODE
