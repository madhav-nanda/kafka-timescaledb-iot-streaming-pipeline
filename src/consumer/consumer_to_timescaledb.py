# ============================================================
# Kafka Consumer → TimescaleDB (PostgreSQL) Loader
# ------------------------------------------------------------
# This script consumes wind turbine telemetry events from Kafka
# and inserts them into a TimescaleDB table for time-series storage.
#
# Key Showcase Points:
# 1) Uses Confluent Kafka Consumer (industry-grade client)
# 2) Manual offset commits AFTER successful DB insert
#    -> prevents losing messages and avoids acknowledging before persistence
# 3) Uses rollback on failure to avoid partial inserts
#
# NOTE:
# - Install dependencies via terminal (README), not inside code:
#   pip install confluent-kafka psycopg2-binary
# ============================================================


from confluent_kafka import Consumer
import psycopg2       # PostgreSQL/TimescaleDB connector
import json           # For parsing JSON messages


# ---------------- Kafka Consumer Configuration ----------------

# Kafka broker address (replace with your broker IP/hostname)
# Example: "localhost:9092" if Kafka is local
KAFKA_BROKER = "Your_KAFKA_BROKER_IP"

# Kafka topic to subscribe to (must exist on the broker)
TOPIC_NAME = "windturbine-data"

# Consumer Group ID:
# - Kafka tracks offsets per consumer group
# - This allows resuming from last committed message
GROUP_ID = "wind_turbine_consumer_group_v2"

# Consumer configuration:
# - auto.offset.reset='earliest' ensures reading from beginning on first run (no committed offsets)
# - enable.auto.commit=False ensures offsets are committed ONLY after DB insert succeeds
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Create Kafka consumer instance
consumer = Consumer(consumer_conf)

# Subscribe to topic
consumer.subscribe([TOPIC_NAME])


# ---------------- TimescaleDB Configuration ----------------

# Database connection settings
# IMPORTANT: Do NOT commit real passwords to GitHub.
# Keep this as a placeholder in public repos OR mention in README to update it.
TIMESCALE_DB_HOST = "localhost"
TIMESCALE_DB_NAME = "postgres"
TIMESCALE_DB_USER = "postgres"
TIMESCALE_DB_PASSWORD = "360digitmg"  # Replace with your own password (do not expose in public)
TIMESCALE_DB_PORT = 5433              # Adjust if using non-default port

# Establish connection to TimescaleDB/PostgreSQL
conn = psycopg2.connect(
    host=TIMESCALE_DB_HOST,
    database=TIMESCALE_DB_NAME,
    user=TIMESCALE_DB_USER,
    password=TIMESCALE_DB_PASSWORD,
    port=TIMESCALE_DB_PORT
)

# Cursor for executing SQL queries
cur = conn.cursor()

# Target table for inserts
TABLE_NAME = "wind_turbine_streamdata"

print(f"Kafka Consumer started with group {GROUP_ID}... Listening for messages from the beginning...")


# ---------------- Main Message Consumption Loop ----------------

try:
    while True:
        # Poll Kafka for a message (timeout avoids blocking forever)
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            # No message received in this cycle
            continue

        if msg.error():
            # Kafka-level message error (network, broker issues, etc.)
            print(f"Kafka Consumer error: {msg.error()}")
            continue

        try:
            # Decode Kafka payload (bytes → string)
            message_value = msg.value().decode('utf-8')

            # Parse JSON payload into Python dict
            data = json.loads(message_value)

            # SQL insert statement
            # NOTE: Using parameterized values (%s...) prevents SQL injection and handles types safely
            insert_query = f"""
                INSERT INTO {TABLE_NAME} (
                    turbine_id, nacelle_position, wind_direction,
                    ambient_air_temp, bearing_temp, blade_pitch_angle,
                    gearbox_sump_temp, generator_speed, hub_speed,
                    power, wind_speed, gear_temp, generator_temp, timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            # Execute insert with values pulled from the Kafka message
            cur.execute(
                insert_query,
                (
                    data.get('Turbine_ID'),
                    data.get('Nacelle_Position'),
                    data.get('Wind_direction'),
                    data.get('Ambient_Air_temp'),
                    data.get('Bearing_Temp'),
                    data.get('BladePitchAngle'),
                    data.get('GearBoxSumpTemp'),
                    data.get('Generator_Speed'),
                    data.get('Hub_Speed'),
                    data.get('Power'),
                    data.get('Wind_Speed'),
                    data.get('GearTemp'),
                    data.get('GeneratorTemp'),
                    data.get('Timestamp')
                )
            )

            # Commit transaction (persist the insert)
            conn.commit()

            # Proof-of-work logging (useful for demo + debugging)
            print(f"Inserted into TimescaleDB: {data}")

            # Commit Kafka offset ONLY after DB commit succeeds
            # This ensures "at-least-once" behavior (no silent data loss)
            consumer.commit(msg)

        except Exception as e:
            # Any failure: parsing, DB insert, connection issues, etc.
            print(f"Error processing message: {e}")
            conn.rollback()  # Roll back the DB transaction to avoid partial/inconsistent state

except KeyboardInterrupt:
    print("Stopping Kafka Consumer...")

finally:
    # Cleanup resources (good engineering hygiene)
    consumer.close()  # Closes consumer cleanly and releases network resources
    cur.close()       # Releases DB cursor
    conn.close()      # Closes DB connection
    print("All connections closed properly.")
