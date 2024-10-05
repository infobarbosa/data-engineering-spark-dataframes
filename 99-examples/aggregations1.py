from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-aggregations") \
    .getOrCreate()

# Carregando o dataset de pedidos
df = spark.read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .option("sep", ";") \
    .load("./datasets-csv-pedidos/*.csv.gz", inferSchema=True)

# Mostrando o schema para verificar o carregamento correto dos dados
df.printSchema()

# Desafio AGREGAÇAO 1: 
#   Agrupando pelos campo UF, calcule a soma das QUANTIDADES dos produtos nos pedidos
df_agg1 = df.groupBy(col("UF")) \
            .agg(sum(col("quantidade")).alias("qtt")) 

print("Resultado do desafio de agregacao 1")
df_agg1.show(truncate=False)