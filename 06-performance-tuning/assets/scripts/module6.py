# Exemplo de script modulo6.py

from pyspark.sql import SparkSession

# Inicializando a SparkSession com ajustes de performance
spark = SparkSession.builder \
    .appName("dataeng-modulo-6") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Leitura de dados e operação de groupBy para análise de performance
df = spark.read.parquet("s3://bucket_name/data/parquet/")
df_grouped = df.groupBy("column").sum()
df_grouped.show()

# Analisando o plano de execução da operação
df_grouped.explain(True)

# Finalizando a sessão Spark
spark.stop()
