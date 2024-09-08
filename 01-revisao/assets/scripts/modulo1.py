from pyspark.sql import SparkSession

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("My Spark Application") \
    .getOrCreate()

df_json = spark.read.json("s3://bucket/data.json")

# Transformações e ações
df_result = df_json.filter(df_json["idade"] > 25).groupBy("cidade").count()

# Mostrando o resultado
df_result.show()

# Esquema personalizado
schema_custom = StructType([
    StructField("cidade", StringType(), True),
    StructField("total", IntegerType(), True)
])

df_custom = spark.createDataFrame(df_result.rdd, schema=schema_custom)
df_custom.printSchema()
