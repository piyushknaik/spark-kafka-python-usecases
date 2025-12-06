import random
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(100):
    sensor_id = random.randint(0, 2)
    data = {
        "sensor_id": f"sensor_{sensor_id}",
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S'),
        "temperature": random.randint(15, 35)
    }
    producer.send('sensor-data', value=data)
    print(f"Sent: {data}")
    time.sleep(1)
    
producer.flush()
producer.close()