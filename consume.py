import duckdb

duckdb.sql("""
CREATE OR REPLACE SECRET my_secret (
    TYPE S3,
    REGION 'us-east-1',
    KEY_ID 'DUMMYIDEXAMPLE',
    SECRET 'DUMMYEXAMPLEKEY',
    ENDPOINT 'localhost:9000',
    USE_SSL 'false',
    URL_STYLE 'path');
""")

print(duckdb.sql("""
SELECT *
FROM delta_scan('s3://lakehouse/gold/live_deteccao_fraude')
;
"""))

print(duckdb.sql("""
SELECT *
FROM delta_scan('s3://lakehouse/gold/valor_medio_por_faixa_etaria')
;
"""))
