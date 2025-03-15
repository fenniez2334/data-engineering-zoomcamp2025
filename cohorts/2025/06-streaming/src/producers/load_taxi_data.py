import csv
import json
from kafka import KafkaProducer
from time import time

def clean_value(value, data_type):
    """Convert empty strings to None (null) or appropriate default values."""
    if value == "":
        return None  # Send as NULL in JSON
    if data_type == "int":
        return int(value)
    if data_type == "float":
        return float(value)
    return value  # Keep as string if no conversion needed

def main():

    # Define the required columns
    required_columns = {
        'lpep_pickup_datetime': 'str',
        'lpep_dropoff_datetime': 'str',
        'PULocationID': 'int',
        'DOLocationID': 'int',
        'passenger_count': 'int',
        'trip_distance': 'float',
        'tip_amount': 'float'
    }
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = 'green_tripdata_2019-10.csv'  # change to your CSV file path if needed

    t0 = time()

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            filtered_row = {col: clean_value(row[col], dtype) for col, dtype in required_columns.items()}
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-trips"
            producer.send('green-trips', value=filtered_row)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()

    t1 = time()
    took = t1 - t0
    print(took)


if __name__ == "__main__":
    main()