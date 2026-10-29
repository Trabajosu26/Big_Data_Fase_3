"""Consume el topic Kafka y calcula métricas por ventana con Spark."""
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName('KafkaSparkSensores')
         .config('spark.sql.session.timeZone', 'UTC')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

esquema = T.StructType([
    T.StructField('sensor_id', T.IntegerType()),
    T.StructField('temperature', T.DoubleType()),
    T.StructField('humidity', T.DoubleType()),
    # El productor envía Unix epoch (entero), no TimestampType.
    T.StructField('timestamp', T.LongType()),
])

entrada = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', 'localhost:9092')
           .option('subscribe', 'sensor_data')
           .option('startingOffsets', 'latest')
           .load())

lecturas = (entrada.select(F.from_json(F.col('value').cast('string'), esquema).alias('d'))
            .select('d.*')
            .dropna(subset=['sensor_id', 'temperature', 'humidity', 'timestamp'])
            .withColumn('fecha_evento', F.to_timestamp(F.from_unixtime('timestamp'))))

ventanas = (lecturas.groupBy(F.window('fecha_evento', '1 minute'), 'sensor_id')
            .agg(F.count('*').alias('lecturas'),
                 F.round(F.avg('temperature'), 2).alias('temperatura_promedio'),
                 F.round(F.avg('humidity'), 2).alias('humedad_promedio')))

consulta = (ventanas.writeStream.outputMode('complete')
            .format('console')
            .option('truncate', 'false')
            .trigger(processingTime='10 seconds')
            .start())
try:
    consulta.awaitTermination()
except KeyboardInterrupt:
    consulta.stop()
finally:
    spark.stop()
