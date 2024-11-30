from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, avg, count

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "DUMMYIDEXAMPLE") \
    .config("spark.hadoop.fs.s3a.secret.key", "DUMMYEXAMPLEKEY") \
    .config("spark.executor.memory", "4g") \
    .appName("Hello World PySpark") \
    .getOrCreate()

df_silver = spark.read \
    .format('delta') \
    .load('s3a://lakehouse/silver/transactions/')

valor_medio_por_faixa_etaria = df_silver.groupBy("faixa_etaria").agg(
    avg(col("valor")).alias("valor_medio_transacoes")
)

total_transacoes_por_comerciante = df_silver.groupBy("comerciante").agg(
    count("*").alias("total_transacoes")
)

taxa_fraude_por_categoria = df_silver.groupBy("categoria").agg(
    (100 * (count(when(col("fraude") == 1, True)) / count("*"))).alias("taxa_fraude_percentual")
)

valor_medio_por_faixa_etaria.write.mode("overwrite").format('delta').save("s3a://lakehouse/gold/valor_medio_por_faixa_etaria")
total_transacoes_por_comerciante.write.mode("overwrite").format('delta').save("s3a://lakehouse/gold/total_transacoes_por_comerciante")
taxa_fraude_por_categoria.write.mode("overwrite").format('delta').save("s3a://lakehouse/gold/taxa_fraude_por_categoria")