from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, row_number, rank, dense_rank, lag
from pyspark.sql.window import Window

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-3").getOrCreate()

# Criando DataFrames de exemplo
df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "valor"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "desc"])

# Broadcast join
df_broadcast_join = df1.join(broadcast(df2), "id")
df_broadcast_join.show()

# Shuffle join
df_shuffle_join = df1.join(df2, "id")
df_shuffle_join.show()

# GroupBy com agregação
df_grouped = df1.groupBy("valor").count()
df_grouped.show()

# Definindo uma janela de dados
window_spec = Window.partitionBy("valor").orderBy("id")

# Aplicando funções de janela
df_window = df1.withColumn("row_number", row_number().over(window_spec))
df_window = df_window.withColumn("rank", rank().over(window_spec))
df_window = df_window.withColumn("dense_rank", dense_rank().over(window_spec))
df_window = df_window.withColumn("lag", lag("id", 1).over(window_spec))
df_window.show()

# Encerrando a SparkSession
spark.stop()