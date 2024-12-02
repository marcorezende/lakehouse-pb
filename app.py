import time

import duckdb
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Charts App", layout="wide")

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

def get_taxa_fraude_por_categoria():

    return duckdb.sql("SELECT * FROM delta_scan('s3://lakehouse/gold/taxa_fraude_por_categoria')").df()


def get_total_transacoes_por_comerciante():

    return duckdb.sql("SELECT * FROM delta_scan('s3://lakehouse/gold/total_transacoes_por_comerciante') ORDER BY total_transacoes DESC LIMIT 10").df()


def get_valor_medio_por_faixa_etaria():

    return duckdb.sql("SELECT * FROM delta_scan('s3://lakehouse/gold/valor_medio_por_faixa_etaria')").df()


st.title("Financeiro")

df_taxa_fraude_por_categoria = get_taxa_fraude_por_categoria()
df_valor_medio_por_faixa_etaria = get_valor_medio_por_faixa_etaria()
df_total_transacoes_por_comerciante = get_total_transacoes_por_comerciante()


col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Valor medio por faixa etaria")
    fig1 = px.bar(df_valor_medio_por_faixa_etaria, x="faixa_etaria", y="valor_medio_transacoes", title="Bar Chart")
    st.plotly_chart(fig1, use_container_width=False)

with col2:
    st.subheader("Total transacoes por comerciante")
    fig2 = px.bar(df_total_transacoes_por_comerciante, x="comerciante", y="total_transacoes", title="Line Chart")
    st.plotly_chart(fig2, use_container_width=False)

with col3:
    st.subheader("Taxa de fraude por categoria")
    fig3 = px.pie(df_taxa_fraude_por_categoria, names="categoria", values="taxa_fraude_percentual", title="Pie Chart")
    st.plotly_chart(fig3, use_container_width=False)

st.divider()

st.subheader("Fraude em tempo real")
placeholder = st.empty()

i = 0

while True:
    query = "SELECT COUNT(*) as total, fraud_prediction FROM delta_scan('s3://lakehouse/gold/live_deteccao_fraude') GROUP BY fraud_prediction"
    data = duckdb.sql(query).df()
    fig = px.bar(data, x="fraud_prediction", y="total", title="Valores em Tempo Real")

    placeholder.plotly_chart(fig, use_container_width=True, key=f"realtime_chart-{i}")

    time.sleep(5)
    i = i + 1


