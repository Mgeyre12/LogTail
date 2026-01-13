import json
import psycopg2
import re
from psycopg2.extras import LogicalReplicationConnection
from kafka import KafkaProducer

# Connection settings for replication
conn_params = {
    'host': 'localhost',
    'port': 5433,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'logtaildb',
    'connection_factory': LogicalReplicationConnection
}

SLOT_NAME = 'my_cdc_slot'
KAFKA_TOPIC = 'postgres.changes'


def parse_logical_message(payload):
    """
    Parse test_decoding output into a clean dictionary.

    Example input: "table public.users: INSERT: id[integer]:5 name[text]:'Gemini'"
    Example output: {'op': 'INSERT', 'table': 'users', 'data': {'id': 5, 'name': 'Gemini'}}
    """
    # Ignore BEGIN/COMMIT messages
    if payload.startswith('BEGIN') or payload.startswith('COMMIT'):
        return None

    # Match: table schema.table_name: OPERATION: columns...
    match = re.match(r"table (\w+)\.(\w+): (INSERT|UPDATE|DELETE):(.*)", payload)
    if not match:
        return None

    schema, table, operation, columns_str = match.groups()

    # Parse columns: column_name[type]:value
    # Pattern handles: name[character varying]:'value with spaces' or id[integer]:5
    # Use [^\]]+ for type to capture "character varying" etc.
    column_pattern = re.compile(r"(\w+)\[([^\]]+)\]:('(?:[^']*(?:'')*[^']*)*'|\S+)")

    data = {}
    for col_match in column_pattern.finditer(columns_str):
        col_name, col_type, raw_value = col_match.groups()

        # Parse the value based on type
        if raw_value.startswith("'") and raw_value.endswith("'"):
            # String value - remove quotes and unescape doubled quotes
            value = raw_value[1:-1].replace("''", "'")
        elif col_type in ('integer', 'bigint', 'smallint'):
            value = int(raw_value)
        elif col_type in ('real', 'double precision', 'numeric'):
            value = float(raw_value)
        elif raw_value == 'null':
            value = None
        else:
            value = raw_value

        data[col_name] = value

    return {
        'op': operation,
        'table': table,
        'data': data
    }


def slot_exists(cursor):
    """Check if the replication slot already exists"""
    cursor.execute(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
        (SLOT_NAME,)
    )
    return cursor.fetchone() is not None


def create_replication_slot(cursor):
    """Create a replication slot if it doesn't exist"""
    if slot_exists(cursor):
        print(f"Replication slot '{SLOT_NAME}' already exists")
    else:
        cursor.create_replication_slot(
            SLOT_NAME,
            output_plugin='test_decoding'
        )
        print(f"Created replication slot: {SLOT_NAME}")


def start_streaming():
    """Main function to start streaming WAL changes"""
    print("Connecting to PostgreSQL for logical replication...")

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    print("Connected successfully!")

    # Create slot if needed
    create_replication_slot(cursor)

    # Initialize Kafka producer
    print("Connecting to Kafka (Redpanda)...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Kafka producer ready. Sending to topic: {KAFKA_TOPIC}")

    # Start replication
    print(f"\nStarting replication from slot '{SLOT_NAME}'...")
    print("Listening for changes... (Press Ctrl+C to stop)\n")

    cursor.start_replication(slot_name=SLOT_NAME, decode=True)

    try:
        while True:
            msg = cursor.read_message()
            if msg:
                parsed = parse_logical_message(msg.payload)
                if parsed:
                    print(parsed)
                    producer.send(KAFKA_TOPIC, parsed)
                # Send feedback to Postgres so WAL logs don't fill up
                msg.cursor.send_feedback(flush_lsn=msg.data_start)
    except KeyboardInterrupt:
        print("\n\nStopping replication...")
    finally:
        producer.close()
        cursor.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    start_streaming()
