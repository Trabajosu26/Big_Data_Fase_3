"""Genera un conjunto propio de lecturas ambientales para análisis batch."""
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

TOTAL_REGISTROS = 200_000
DESTINO = Path(__file__).resolve().parent.parent / 'datos_sensores.csv'
ALEATORIEDAD = random.Random(2026)
INICIO = datetime(2026, 9, 1, tzinfo=timezone.utc)

with DESTINO.open('w', newline='', encoding='utf-8') as archivo:
    escritor = csv.writer(archivo)
    escritor.writerow(['event_id', 'sensor_id', 'event_time', 'temperature', 'humidity'])
    for numero in range(1, TOTAL_REGISTROS + 1):
        sensor = (numero - 1) % 10 + 1
        momento = INICIO + timedelta(seconds=12 * (numero - 1))
        temp = round(ALEATORIEDAD.uniform(20, 30), 2)
        humedad = round(ALEATORIEDAD.uniform(30, 70), 2)
        # Introduce algunos registros incompletos para documentar limpieza.
        if numero % 777 == 0:
            temp = ''
        if numero % 991 == 0:
            humedad = ''
        escritor.writerow([numero, sensor, momento.strftime('%Y-%m-%dT%H:%M:%SZ'), temp, humedad])
print(f'Archivo creado: {DESTINO} ({TOTAL_REGISTROS:,} filas)')
