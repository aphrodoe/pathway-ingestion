from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def generate_synthetic_event():
    return {
        "user_id": random.randint(1000, 1100),
        "amount": random.uniform(10, 15000),
        "location": random.choice(["IN", "US", "EU"]),
        "timestamp": time.time()
    }

if __name__ == "__main__":
    while True:
        event = generate_synthetic_event()
        producer.send('synthetic_streams', json.dumps(event).encode())
        print("Sent synthetic event:", event)
        time.sleep(0.5)
