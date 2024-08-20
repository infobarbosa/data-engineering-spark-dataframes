# Exemplo de script modulo4.py

from pyspark.sql import SparkSession

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-4").getOrCreate()

# Carregando um grande conjunto de dados
df = spark.read.csv("path/to/large_data.csv", header=True, inferSchema=True)

# Otimizações com Catalyst (filtros, projeções)
df_optimized = df.filter(df['column1'] > 100).select("column2", "column3")
df_optimized.explain(True)

# Cacheando o DataFrame para otimizar performance
df_cached = df_optimized.cache()
df_cached.show()

# Persistindo o DataFrame em memória e disco
df_persisted = df_optimized.persist()
df_persisted.show()

# Particionamento para paralelismo eficiente
df_repartitioned = df.repartition(10, "column2")
df_repartitioned.explain(True)

# Análise final do plano de execução
df_final = df_repartitioned.filter(df_repartitioned['column3'] < 50)
df_final.explain(True)

# Encerrando a SparkSession
spark.stop()
