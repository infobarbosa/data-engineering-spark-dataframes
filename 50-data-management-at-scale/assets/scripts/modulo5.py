# Exemplo de script modulo5.py

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-5").getOrCreate()

# Carregando dados em formato Parquet
df_parquet = spark.read.parquet("s3://bucket_name/data/parquet/")
df_parquet.show()

# Carregando dados em formato ORC
df_orc = spark.read.orc("s3://bucket_name/data/orc/")
df_orc.show()

# Adicionando novos dados com esquema evoluído
new_data = [Row(id=1, name="Alice", age=30, country="US"),
            Row(id=2, name="Bob", age=25, country="UK")]
df_new = spark.createDataFrame(new_data)

# Salvando o DataFrame com o novo esquema
df_new.write.mode("append").parquet("s3://bucket_name/data/parquet/")

# Leitura otimizada com pushdown de filtros e seleção de colunas
df_optimized_read = spark.read.parquet("s3://bucket_name/data/parquet/") \
    .select("id", "name") \
    .filter("age > 20")

df_optimized_read.show()

# Escrita otimizada com compressão e particionamento
df_optimized_read.write.mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("country") \
    .parquet("s3://bucket_name/output/parquet/")

# Encerrando a SparkSession
spark.stop()
