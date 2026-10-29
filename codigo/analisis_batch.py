"""Lectura, limpieza, análisis y visualización batch con PySpark."""
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

RAIZ = Path(__file__).resolve().parent.parent
ARCHIVO = str(RAIZ / 'datos_sensores.csv')
SALIDA = RAIZ / 'resultados_batch'
SALIDA.mkdir(exist_ok=True)

spark = (SparkSession.builder.appName('AnalisisBatchSensores')
         .master('local[2]').config('spark.sql.session.timeZone', 'UTC')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

esquema = T.StructType([
    T.StructField('event_id', T.IntegerType(), True),
    T.StructField('sensor_id', T.IntegerType(), True),
    T.StructField('event_time', T.StringType(), True),
    T.StructField('temperature', T.DoubleType(), True),
    T.StructField('humidity', T.DoubleType(), True),
])

try:
    original = (spark.read.option('header', True)
                .schema(esquema).csv(ARCHIVO))
    registros_originales = original.count()
    depurado = (original
                .withColumn('fecha', F.to_timestamp('event_time', "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                .dropna(subset=['event_id', 'sensor_id', 'fecha', 'temperature', 'humidity'])
                .filter((F.col('sensor_id').between(1, 10)) &
                        (F.col('temperature').between(-20, 60)) &
                        (F.col('humidity').between(0, 100)))
                .dropDuplicates(['event_id']).cache())
    registros_validos = depurado.count()
    print(f'REGISTROS ORIGINALES: {registros_originales}')
    print(f'REGISTROS VALIDOS: {registros_validos}')
    print(f'ELIMINADOS POR LIMPIEZA: {registros_originales - registros_validos}')

    print('\nESTADISTICA GENERAL')
    depurado.select('temperature', 'humidity').describe().show(truncate=False)

    resumen = (depurado.groupBy('sensor_id')
               .agg(F.count('*').alias('registros'),
                    F.round(F.avg('temperature'), 2).alias('temperatura_promedio'),
                    F.round(F.avg('humidity'), 2).alias('humedad_promedio'),
                    F.round(F.min('temperature'), 2).alias('temperatura_minima'),
                    F.round(F.max('temperature'), 2).alias('temperatura_maxima'))
               .orderBy('sensor_id'))
    print('\nRESUMEN POR SENSOR')
    resumen.show(20, truncate=False)

    alertas = (depurado.withColumn(
        'alerta', (F.col('temperature') > 28.5) | (F.col('humidity') < 35))
        .groupBy('sensor_id')
        .agg(F.sum(F.col('alerta').cast('int')).alias('eventos_alerta'))
        .orderBy('sensor_id'))
    print('\nEVENTOS FUERA DE UMBRAL (criterios demostrativos)')
    alertas.show(20, truncate=False)

    resumen.coalesce(1).write.mode('overwrite').option('header', True).csv(str(SALIDA / 'resumen_por_sensor'))
    alertas.coalesce(1).write.mode('overwrite').option('header', True).csv(str(SALIDA / 'alertas_por_sensor'))

    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        datos = resumen.select('sensor_id', 'temperatura_promedio').collect()
        fig, eje = plt.subplots(figsize=(8, 4.5))
        eje.bar([str(x['sensor_id']) for x in datos], [x['temperatura_promedio'] for x in datos])
        eje.set_title('Temperatura promedio por sensor')
        eje.set_xlabel('Sensor')
        eje.set_ylabel('Temperatura (°C)')
        eje.grid(axis='y', alpha=0.2)
        fig.tight_layout()
        fig.savefig(SALIDA / 'temperatura_promedio.png', dpi=160)
        plt.close(fig)
        print('GRAFICA GENERADA:', SALIDA / 'temperatura_promedio.png')
    except ImportError:
        print('No se encontró matplotlib; CSV y resultados de consola sí se generaron.')
finally:
    spark.stop()
