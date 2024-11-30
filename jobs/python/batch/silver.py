from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff

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

df_bronze = spark.read \
    .format('delta') \
    .load('s3a://lakehouse/bronze/transactions/')

df_silver = df_bronze.dropDuplicates()

df_silver = df_silver.withColumnRenamed("trans_date_trans_time", "data_transacao") \
    .withColumnRenamed("cc_num", "numero_cartao") \
    .withColumnRenamed("merchant", "comerciante") \
    .withColumnRenamed("category", "categoria") \
    .withColumnRenamed("amt", "valor") \
    .withColumnRenamed("first", "primeiro_nome") \
    .withColumnRenamed("last", "sobrenome") \
    .withColumnRenamed("gender", "genero") \
    .withColumnRenamed("street", "rua") \
    .withColumnRenamed("city", "cidade") \
    .withColumnRenamed("state", "estado") \
    .withColumnRenamed("zip", "cep") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("long", "longitude") \
    .withColumnRenamed("city_pop", "populacao_cidade") \
    .withColumnRenamed("job", "profissao") \
    .withColumnRenamed("dob", "data_nascimento") \
    .withColumnRenamed("trans_num", "id_transacao") \
    .withColumnRenamed("merch_lat", "latitude_comerciante") \
    .withColumnRenamed("merch_long", "longitude_comerciante") \
    .withColumnRenamed("is_fraud", "fraude") \
    .withColumnRenamed("merch_zipcode", "cep_comerciante")

df_silver = df_silver.withColumn("idade", (datediff(col("data_transacao"),
                                                    col("data_nascimento")) / 365).cast("int"))
df_silver = df_silver.withColumn(
    "faixa_etaria",
    when(col("idade") < 18, "Menor de 18")
    .when((col("idade") >= 18) & (col("idade") < 30), "18-29")
    .when((col("idade") >= 30) & (col("idade") < 50), "30-49")
    .when((col("idade") >= 50) & (col("idade") < 65), "50-64")
    .otherwise("65+")
)


df_silver.write.mode("overwrite").format('delta').save("s3a://lakehouse/silver/transactions/")