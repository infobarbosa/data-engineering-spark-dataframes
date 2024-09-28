# Exemplo de script modulo7.py

import logging
from pyspark.sql import SparkSession

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Função modular para carregar dados
def load_data(spark, path):
    logger.info(f"Carregando dados de {path}")
    return spark.read.parquet(path)

# Função modular para processar dados
def process_data(df):
    logger.info("Processando dados...")
    return df.filter(df['value'] > 100)

# Função modular para salvar dados
def save_data(df, path):
    logger.info(f"Salvando dados em {path}")
    df.write.parquet(path)

# Pipeline principal
if __name__ == "__main__":
    spark = SparkSession.builder.appName("dataeng-modulo-7").getOrCreate()
    
    df = load_data(spark, "s3://bucket_name/data/parquet/")
    df_processed = process_data(df)
    save_data(df_processed, "s3://bucket_name/data/processed/")
    
    logger.info("Pipeline concluído com sucesso.")
    spark.stop()
