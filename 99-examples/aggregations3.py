from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc, col, count, sum, avg, max, min

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

# Desafio AGREGAÇAO 3: Agrupe pela DATA DO PEDIDO e calcule a soma do valor total dos pedidos
# Atenção! 
#   O dataset nao possui valor total do pedido, apenas quantidade e valor unitario dos produtos. 
#   Dessa forma, sera necessario criar uma nova coluna de valor total calculado.
#   O atributo DATA_CRIACAO possui hora, minuto e segundo. Utilize a funcao date_trunc para truncar o valor.

# Incluindo a nova coluna de data truncada
df = df.withColumn("DATA_PEDIDO", date_trunc("day", col("data_criacao")))

# Incluindo a nova coluna de valor total do pedido
df = df.withColumn("VALOR_TOTAL_PEDIDO", col("quantidade") * col("valor_unitario"))

df_agg3 = df.groupBy(col("data_pedido")) \
            .agg(sum("valor_total_pedido").alias("total")) \
            .orderBy(col("data_pedido").asc()) 

print("Resultado do desafio de agregacao 3")
df_agg3.show(40, truncate=False)

# Parar a Spark Session
spark.stop()