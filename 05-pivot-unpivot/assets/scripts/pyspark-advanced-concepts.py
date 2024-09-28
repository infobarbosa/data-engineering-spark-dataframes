from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, ArrayType

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-2").getOrCreate()

# 1. Criação de DataFrame com Arrays e Structs
data = [
    ("João", [{"curso": "Matemática", "nota": 85}, {"curso": "História", "nota": 90}]),
    ("Maria", [{"curso": "Matemática", "nota": 95}, {"curso": "História", "nota": 80}])
]
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("cursos", ArrayType(StructType([
        StructField("curso", StringType(), True),
        StructField("nota", IntegerType(), True)
    ])), True)
])
df = spark.createDataFrame(data, schema)

# 2. Explodindo o Array para Linhas Individuais
df_exploded = df.withColumn("curso", explode(df["cursos"]))
df_exploded = df_exploded.select("nome", col("curso.curso"), col("curso.nota"))
df_exploded.show()

# 3. Definindo uma UDF para Calcular um Bônus na Nota
@udf(IntegerType())
def calcular_bonus(nota):
    return nota + 5

# Aplicando a UDF na Coluna 'nota'
df_bonus = df_exploded.withColumn("nota_bonus", calcular_bonus(df_exploded["nota"]))
df_bonus.show()

# 4. Aplicação de Pivot
df_pivot_bonus = df_bonus.groupBy("nome").pivot("curso").agg({"nota_bonus": "max"})
df_pivot_bonus.show()

# 5. Rollup para Agregações Hierárquicas
df_rollup = df_exploded.rollup("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_rollup.show()

# 6. Cube para Agregações Multidimensionais
df_cube = df_exploded.cube("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_cube.show()

# Encerrando a SparkSession
spark.stop()
