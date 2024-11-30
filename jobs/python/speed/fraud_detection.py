import subprocess
import sys

import pkg_resources
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, OneHotEncoder
from pyspark.sql import SparkSession
from pyspark.sql.functions import sqrt, pow, col, from_unixtime, hour, when, lit, udf
from pyspark.sql.types import DoubleType

try:
    import joblib

    pkg_resources.require("joblib")
except Exception as e:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "joblib==1.4.2"])
    print(e)
finally:
    import joblib

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

schema = """
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

# data_frame = spark.readStream \
#     .schema(schema) \
#     .csv('s3a://lakehouse/landing/transactions/*')

data_frame = spark.read \
    .schema(schema) \
    .option('header', 'true')\
    .csv('s3a://lakehouse/landing/transactions/*')

data_frame = data_frame.withColumn('unix_time', from_unixtime('unix_time'))

data_frame = data_frame.withColumn('hour', hour('unix_time'))

data_frame = data_frame.withColumn('time_category',
                                   when((col('hour') >= 0) & (col('hour') < 6), lit('Madrugada'))
                                   .when((col('hour') >= 6) & (col('hour') < 12), lit('Manhã'))
                                   .when((col('hour') >= 12) & (col('hour') < 18), lit('Tarde'))
                                   .otherwise(lit('Noite'))
                                   )

data_frame = data_frame.withColumn(
    "distancia_cliente_comerciante",
    sqrt(pow(col("merch_lat") - col("lat"), 2) + pow(col("merch_long") - col("long"), 2))
)

indexer = StringIndexer(inputCol="category", outputCol="category_index")
encoder = OneHotEncoder(inputCol="category_index", outputCol="category_encoded")

time_category_indexer = StringIndexer(inputCol="time_category", outputCol="time_category_index")
time_category_encoder = OneHotEncoder(inputCol="time_category_index", outputCol="time_category_encoded")

assembler = VectorAssembler(inputCols=["amt", "distancia_cliente_comerciante"], outputCol="features_unscaled")
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=False)

pipeline = Pipeline(stages=[indexer, encoder, time_category_indexer, time_category_encoder, assembler, scaler])
data_frame.show()
model = pipeline.fit(data_frame)
processed_data = model.transform(data_frame)

model = joblib.load("/opt/bitnami/spark/fraud_model.pkl")


def predict_fraud(amt, distancia_cliente_comerciante, time_category, category):
    input_data = [[amt, distancia_cliente_comerciante, time_category, category]]
    return float(model.predict(input_data)[0])


predict_udf = udf(predict_fraud, DoubleType())

stream_df = data_frame.withColumn(
    "fraud_prediction",
    predict_udf(
        col("amt"),
        col("distancia_cliente_comerciante"),
        col("time_category"),
        col("category")
    )
)

# stream_df.writeStream.format("delta") \
#     .option("checkpointLocation", 's3a://lakehouse/speed_checkpoint') \
#     .trigger(processingTime='2 seconds') \
#     .start('s3a://lakehouse/gold/live_deteccao_fraude') \
#     .awaitTermination()

# stream_df.write.format('delta').mode('overwrite').save('s3a://lakehouse/gold/live_deteccao_fraude')
