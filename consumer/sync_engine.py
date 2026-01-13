import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

KAFKA_TOPIC = 'postgres.changes'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
ES_HOST = 'http://localhost:9200'
ES_INDEX = 'users'


def main():
    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='sync-engine-group'
    )
    print(f"Connected to Kafka topic: {KAFKA_TOPIC}")

    print("Connecting to Elasticsearch...")
    es = Elasticsearch(ES_HOST)
    print(f"Connected to Elasticsearch: {ES_HOST}")

    print(f"\nListening for messages... (Press Ctrl+C to stop)\n")

    try:
        for message in consumer:
            event = message.value
            op = event.get('op')
            table = event.get('table')
            data = event.get('data', {})
            doc_id = data.get('id')

            if doc_id is None:
                print(f"Skipping event without id: {event}")
                continue

            if op in ('INSERT', 'UPDATE'):
                es.index(index=ES_INDEX, id=doc_id, document=data)
                print(f"Indexed User {doc_id}")

            elif op == 'DELETE':
                es.delete(index=ES_INDEX, id=doc_id, ignore=[404])
                print(f"Deleted User {doc_id}")

    except KeyboardInterrupt:
        print("\n\nShutting down...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
