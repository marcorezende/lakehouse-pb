CREATE TABLE IF NOT EXISTS delta.default.live_deteccao_fraude (dummy bigint) WITH ( location = 's3a://lakehouse/gold/live_deteccao_fraude');
CREATE TABLE IF NOT EXISTS delta.default.taxa_fraude_por_categoria (dummy bigint) WITH ( location = 's3a://lakehouse/gold/taxa_fraude_por_categoria');
CREATE TABLE IF NOT EXISTS delta.default.total_transacoes_por_comerciante (dummy bigint) WITH ( location = 's3a://lakehouse/gold/total_transacoes_por_comerciante');
CREATE TABLE IF NOT EXISTS delta.default.valor_medio_por_faixa_etaria (dummy bigint) WITH ( location = 's3a://lakehouse/gold/valor_medio_por_faixa_etaria');
CREATE TABLE IF NOT EXISTS delta.default.bronze_transactions (dummy bigint) WITH ( location = 's3a://lakehouse/bronze/transactions');
CREATE TABLE IF NOT EXISTS delta.default.silver_transactions (dummy bigint) WITH ( location = 's3a://lakehouse/silver/transactions');