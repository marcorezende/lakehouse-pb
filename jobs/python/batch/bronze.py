from pyspark.sql import SparkSession


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

schema= """
Unnamed INT,
trans_date_trans_time TIMESTAMP,
cc_num STRING,
merchant STRING,
category STRING,
amt DOUBLE,
first STRING,
last STRING,
gender STRING,
street STRING,
city STRING,
state STRING,
zip STRING,
lat DOUBLE,
long DOUBLE,
city_pop INT,
job STRING,
dob DATE,
trans_num STRING,
unix_time BIGINT,
merch_lat DOUBLE,
merch_long DOUBLE,
is_fraud INT,
merch_zipcode STRING
"""


data_frame = spark.readStream \
    .schema(schema) \
    .csv('s3a://lakehouse/landing/transactions/*')

data_frame.writeStream.format("delta") \
    .option("checkpointLocation",'s3a://lakehouse/_checkpoint') \
    .trigger(availableNow=True) \
    .start('s3a://lakehouse/bronze/transactions') \
    .awaitTermination()

spark.stop()
