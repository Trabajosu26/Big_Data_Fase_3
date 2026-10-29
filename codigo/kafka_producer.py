"""Simula llegada de lecturas cada segundo al topic sensor_data."""
import json
import random
import time
from kafka import KafkaProducer

azar = random.Random()
productor = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda valor: json.dumps(valor).encode('utf-8'),
    retries=3,
)

try:
    while True:
        registro = {
            'sensor_id': azar.randint(1, 10),
            'temperature': round(azar.uniform(20, 30), 2),
            'humidity': round(azar.uniform(30, 70), 2),
            'timestamp': int(time.time()),
        }
        productor.send('sensor_data', value=registro)
        productor.flush()
        print('Enviado:', registro, flush=True)
        time.sleep(1)
except KeyboardInterrupt:
    print('\nProductor detenido.')
finally:
    productor.close()
