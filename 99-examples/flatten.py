from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, ArrayType

print("inicializando a sessao spark")
spark = SparkSession.builder.appName("dataeng-flatten").getOrCreate();

#help(StructField)

df1 = spark.read.json("./data.json", multiLine=True)
print("O Schema: ")
df1.printSchema()

print("Os dados")
df1.show()

schema2 = StructType([
		StructField("nome", StringType(), True),
		StructField("idade", IntegerType(), True),
		StructField("notas", StringType(), True),
		StructField("contatos", StringType(), True),
		StructField("interesses", StringType(), True)
])

print(schema2)

print("abrindo novamente o dataframe")
df2 = spark.read.json("./data.json", multiLine=True, schema=schema2)

print("O schema 2: ")
df2.printSchema()

print("imprimindo df2")
df2.show(truncate=False)

schema3 = StructType([
                StructField("nome", StringType() ),
                StructField("idade", IntegerType() ),
                StructField("notas", StructType([
                    StructField("matematica", IntegerType()),
			        StructField("portugues", IntegerType()),
			        StructField("ciencias", IntegerType())
		]))
])

df3 = spark.read.json("./data.json", multiLine=True, schema=schema3)
df3.printSchema()
df3.show()

print("Explodindo NOTAS")
df_exploded = df3.select(
    			col("nome"), 
                col("idade"), 
                col("notas.matematica").alias("matematica"),
                col("notas.portugues").alias("portugues"),
                col("notas.ciencias").alias("ciencias")
				)

print("dataframe explodido: ")
df_exploded.show(truncate=False)

print("Explodindo CONTATOS")
schema4 = StructType([
                StructField("nome", StringType() ),
                StructField("idade", IntegerType() ),
                StructField("contatos", 
                    ArrayType(
                        StructType([
							StructField("tipo", StringType()),
							StructField("valor", StringType())
						])
					))
		])

df4 = spark.read.json("./data.json", multiLine=True, schema=schema4)
df4.show(truncate=False)

print("selecionando os dados")
df_exploded = df4.select(col("nome"), col("idade"), explode(col("contatos")).alias("c"))
df_exploded.printSchema()
df_exploded.show()

df5 = df_exploded.select(col("nome"), col("idade"), col("c.tipo"), col("c.valor"))
df5.printSchema()
df5.show(truncate=False)


print("Explodindo INTERESSES")
schema6 = StructType([
			StructField("nome", StringType()),
            StructField("idade", IntegerType()),
            StructField("interesses", ArrayType(StringType()))
])

df6 = spark.read.json("./data.json", multiLine=True, schema=schema6)
df6.printSchema()
df6.show()

df_exploded = df6.select(col("nome"), col("idade"), explode(col("interesses")).alias("interesse"))
df_exploded.show()
