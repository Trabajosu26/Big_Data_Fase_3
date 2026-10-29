# Tarea 3 · Procesamiento de datos con Apache Spark

**Curso:** Big Data (202016911) · **Modalidad:** Individual  
**Caso de estudio:** monitoreo de temperatura y humedad de 10 sensores ambientales.

Este repositorio contiene el código fuente para dos prácticas: procesamiento por lotes (batch) de 200.000 registros simulados y procesamiento de eventos en tiempo real mediante Apache Kafka y Spark Structured Streaming.

## Estructura

```text
codigo/
  generar_datos.py             # Crea datos_sensores.csv con 200.000 registros
  analisis_batch.py            # Limpieza, estadísticas y gráfica con Spark
  kafka_producer.py            # Emite mediciones al tema sensor_data
  spark_streaming_consumer.py  # Lee Kafka y calcula promedios por minuto
README.md
requirements.txt
.gitignore
```

## Requisitos

- Ubuntu con Java, Apache Spark y Python 3 instalados.
- Apache Kafka 3.9.2 configurado según el Anexo 3 de la guía.
- Spark 3.5.3 para utilizar literalmente el paquete Kafka indicado en dicho anexo. Si la versión instalada es otra, debe corresponderle el conector adecuado.
- Librería `kafka-python` para el productor; `matplotlib` para guardar la gráfica del procesamiento batch.

## 1. Procesamiento batch

Desde la carpeta principal del proyecto:

```bash
python3 codigo/generar_datos.py
spark-submit codigo/analisis_batch.py
```

`generar_datos.py` crea `datos_sensores.csv` con 200.000 registros, cinco columnas y algunos campos deliberadamente vacíos para la fase de limpieza. El programa batch carga el archivo mediante un DataFrame de Spark, excluye registros incompletos, calcula los promedios por sensor y guarda los resultados en `resultados_batch/`.

Se obtendrán carpetas CSV `resumen_por_sensor/` y `alertas_por_sensor/`. Si `matplotlib` está instalado en el Python de Spark, se genera además `temperatura_promedio.png`.

Los datos se generan de forma reproducible con semilla `2026`. El número esperado de registros válidos es **199.542**; hay **458** registros con campos incompletos. Estos valores describen el CSV simulado; las salidas de Spark deben comprobarse al ejecutarlo.

## 2. Procesamiento en tiempo real (Anexo 3)

Para la instalación y creación del tema se siguen las indicaciones del **Anexo 3. Instructivo Spark Streaming y Kafka** de la UNAD. En una máquina con Kafka instalada en `/opt/Kafka`, iniciar los servicios según la guía:

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```

Crear el tema la primera vez:

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
```

En una primera terminal, ejecutar el productor:

```bash
python3 codigo/kafka_producer.py
```

En otra terminal, sin cerrar la anterior, iniciar el consumidor (paquete indicado en la guía para Spark 3.5.3):

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 codigo/spark_streaming_consumer.py
```

Para detener cada proceso, pulsar `Ctrl + C`.

### Nota sobre el consumidor

El productor envía `timestamp` como **entero Unix**, igual que en el Anexo 3. El consumidor incluido en este repositorio lo interpreta como `LongType` y posteriormente lo convierte a fecha. Esta corrección técnica evita interpretar directamente un entero como `TimestampType` al agrupar lecturas en ventanas de un minuto. El resto de la práctica mantiene el objetivo de recibir eventos del tema `sensor_data` y calcular los promedios por sensor.

## Evidencias previstas

- Archivo CSV, código batch, estadísticas y gráfica.
- Tema `sensor_data`, productor enviando eventos y consumidor mostrando resultados.
- Interfaz web de Spark (`http://<ip-de-ubuntu>:4040`, mientras la aplicación está activa).

**Videos del estudiante** (completar una vez publicados):

- Procesamiento batch: [insertar enlace]
- Kafka y Spark Streaming: [insertar enlace]

## Fuente de la actividad

Universidad Nacional Abierta y a Distancia (UNAD). *Guía de aprendizaje – Tarea 3 – Procesamiento de Datos con Apache Spark* y *Anexo 3. Instructivo Spark Streaming y Kafka*.
