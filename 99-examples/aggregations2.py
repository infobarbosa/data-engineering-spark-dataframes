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

# Desafio AGREGAÇAO 2: 
#   Agrupe pelo atributo PRODUTO, calcule a soma do valor total dos pedidos
#   Atenção! 
#   O dataset nao possui valor total do pedido, apenas quantidade e valor unitario dos produtos. 
#   Dessa forma, sera necessario criar uma nova coluna de valor total calculado.

# Incluindo a nova coluna de valor total do pedido
df = df.withColumn("VALOR_TOTAL_PEDIDO", col("quantidade") * col("valor_unitario"))

df_agg2 = df.groupBy("produto") \
            .agg(sum("valor_total_pedido").alias("total")) \
            .orderBy(col("total").desc())

print("Resultado do desafio de agregacao 2")
df_agg2.show(truncate=False)
