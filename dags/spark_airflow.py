import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Yusuf Ganiyu",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

bronze = SparkSubmitOperator(
    task_id="bronze",
    conn_id="spark-conn",
    conf={
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "DUMMYIDEXAMPLE",
        "spark.hadoop.fs.s3a.secret.key": "DUMMYEXAMPLEKEY",
        "spark.executor.memory": "4g"

    },
    packages=(
        "io.delta:delta-spark_2.12:3.2.0,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.hadoop:hadoop-common:3.3.4"
    ),

    application="jobs/python/bronze.py",
    dag=dag
)

silver = SparkSubmitOperator(
    task_id="silver",
    conn_id="spark-conn",
    conf={
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "DUMMYIDEXAMPLE",
        "spark.hadoop.fs.s3a.secret.key": "DUMMYEXAMPLEKEY",
        "spark.executor.memory": "4g"
    },
    packages=(
        "io.delta:delta-spark_2.12:3.2.0,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.hadoop:hadoop-common:3.3.4"
    ),

    application="jobs/python/silver.py",
    dag=dag
)

gold = SparkSubmitOperator(
    task_id="gold",
    conn_id="spark-conn",
    conf={
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "DUMMYIDEXAMPLE",
        "spark.hadoop.fs.s3a.secret.key": "DUMMYEXAMPLEKEY"
    },
    packages=(
        "io.delta:delta-spark_2.12:3.2.0,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.hadoop:hadoop-common:3.3.4"
    ),

    application="jobs/python/gold.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> bronze >> silver >> gold >> end
