import subprocess
import sys

import pkg_resources
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import sqrt, pow, col, from_unixtime, hour, when, lit, udf

try:
    import joblib

    pkg_resources.require("joblib")
except Exception as e:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "joblib==1.4.2"])
    print(e)
finally:
    import joblib

try:
    import sklearn

    pkg_resources.require("scikit-learn")
except Exception as e:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn==1.5.2"])
    print(e)
finally:
    import sklearn

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

data_frame = spark.readStream \
    .schema(schema) \
    .option('header', 'true') \
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

model = joblib.load("jobs/fraud_model.pkl")


def prediction(df: DataFrame, model_ml, spark_context: SparkContext) -> Column:
    model_ml = spark_context.broadcast(model_ml)

    @udf('float')
    def predict_data(*cols):
        return float(model_ml.value.predict((cols,))[0])

    return predict_data(*df.columns)


stream_df = data_frame.withColumn(
    "fraud_prediction",
    prediction(df=data_frame.select('amt', 'distancia_cliente_comerciante', 'category', 'time_category'),
               model_ml=model, spark_context=spark.sparkContext)
)

stream_df.writeStream.format("delta") \
    .option("checkpointLocation", 's3a://lakehouse/speed_checkpoint') \
    .trigger(processingTime='2 seconds') \
    .start('s3a://lakehouse/gold/live_deteccao_fraude') \
    .awaitTermination()

