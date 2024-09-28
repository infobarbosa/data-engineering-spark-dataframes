# Rolloups e Cubes

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Rollups e cubes são usados para criar agregações hierárquicas em grupos de dados, sendo especialmente úteis para relatórios multidimensionais.

## 2. Rollups

A função `rollup` é usada para gerar uma hierarquia de agregações. Ela permite agregar dados em diferentes níveis de granularidade. O `rollup` cria uma série de subtotais e um total geral, permitindo ver a evolução dos dados em níveis agregados.

**Exemplo de uso do `rollup`**:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Inicializando a sessão do Spark
spark = SparkSession.builder.appName("Exemplo ROLLUP").getOrCreate()

# Exemplo de dados de vendas
data = [
    ("Eletrônicos", 2022, 1000),
    ("Eletrônicos", 2023, 1500),
    ("Eletrônicos", 2024, 400),
    ("Móveis", 2022, 700),
    ("Móveis", 2023, 800),
    ("Móveis", 2024, 200),
    ("Vestuário", 2022, 500),
    ("Vestuário", 2023, 600),
    ("Vestuário", 2024, 300)
]

# Definir o esquema do DataFrame
columns = ["categoria_produto", "ano", "vendas"]
df = spark.createDataFrame(data, columns)

print("Dataframe original")
df.show()

# Aplicando a agregação com ROLLUP
df_rollup = df.groupBy("categoria_produto", "ano") \
              .agg(sum("vendas").alias("total_vendas")) \
              .rollup("categoria_produto", "ano") \
              .sum("total_vendas") \
              .orderBy("categoria_produto", "ano")

print("Resultado agregado com rollup")
df_rollup.show()

```

Output esperado:
```
+-----------------+----+-----------------+                                      
|categoria_produto| ano|sum(total_vendas)|
+-----------------+----+-----------------+
|             NULL|NULL|             6000| # Total geral
|      Eletrônicos|NULL|             2900| # Subtotal para Eletrônicos
|      Eletrônicos|2022|             1000|
|      Eletrônicos|2023|             1500|
|      Eletrônicos|2024|              400|
|           Móveis|NULL|             1700| # Subtotal para Móveis
|           Móveis|2022|              700|
|           Móveis|2023|              800|
|           Móveis|2024|              200|
|        Vestuário|NULL|             1400| # Subtotal para Vestuário
|        Vestuário|2022|              500|
|        Vestuário|2023|              600|
|        Vestuário|2024|              300|
+-----------------+----+-----------------+
```


## 3. Cubes

A função `cube` é usada para criar uma agregação em todas as combinações possíveis dos grupos especificados. Isso é útil para obter um resumo completo dos dados para todas as combinações dos grupos fornecidos.

**Exemplo de uso do `cube`**:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Inicializando a sessão do Spark
spark = SparkSession.builder.appName("Exemplo CUBE").getOrCreate()

data = [
    ("Eletrônicos", 2022, 1000),
    ("Eletrônicos", 2023, 1500),
    ("Eletrônicos", 2024, 400),
    ("Móveis", 2022, 700),
    ("Móveis", 2023, 800),
    ("Móveis", 2024, 200),
    ("Vestuário", 2022, 500),
    ("Vestuário", 2023, 600),
    ("Vestuário", 2024, 300)
]

# Definir as colunas do DataFrame
columns = ["categoria_produto", "ano", "vendas"]

# Criando o DataFrame com os dados fornecidos
df = spark.createDataFrame(data, columns)

# Aplicando a função CUBE para todas as combinações de categoria e ano
df_cube = df.groupBy("categoria_produto", "ano") \
            .agg(sum("vendas").alias("total_vendas")) \
            .cube("categoria_produto", "ano") \
            .sum("total_vendas") \
            .orderBy("categoria_produto", "ano")

# Mostrar o resultado
df_cube.show()

```

Output esperado:
```
+-----------------+----+-----------------+                                      
|categoria_produto| ano|sum(total_vendas)|
+-----------------+----+-----------------+
|             NULL|NULL|             6000| # Total geral
|             NULL|2022|             2200| # subtotal para todos os produtos em 2022
|             NULL|2023|             2900| # subtotal para todos os produtos em 2023
|             NULL|2024|              900| # subtotal para todos os produtos em 2024
|      Eletrônicos|NULL|             2900| # subtotal para Eletrônicos
|      Eletrônicos|2022|             1000|
|      Eletrônicos|2023|             1500|
|      Eletrônicos|2024|              400|
|           Móveis|NULL|             1700| # subtotal para Móveis
|           Móveis|2022|              700|
|           Móveis|2023|              800|
|           Móveis|2024|              200|
|        Vestuário|NULL|             1400| # subtotal para Vestuário
|        Vestuário|2022|              500|
|        Vestuário|2023|              600|
|        Vestuário|2024|              300|
+-----------------+----+-----------------+
```

## 4. Resumo

- **`rollup`**: Cria agregações hierárquicas e totais gerais. Útil para relatórios que exigem subtotais em diferentes níveis.
- **`cube`**: Cria todas as combinações possíveis dos grupos, permitindo análises mais detalhadas em várias dimensões.

Ambos são ferramentas poderosas para análise e sumarização de dados em PySpark.

## 5. Exemplo com Arrays e Structs
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("dataeng-rollup-cube").getOrCreate()

from pyspark.sql.functions import explode, col

# Exemplo de DataFrame com arrays e structs
data = [
    ("João", [{"curso": "MATEMATICA", "nota": 85}, {"curso": "HISTORIA", "nota": 90}]),
    ("Maria", [{"curso": "MATEMATICA", "nota": 95}, {"curso": "HISTORIA", "nota": 80}])
]
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("cursos", ArrayType(StructType([
        StructField("curso", StringType(), True),
        StructField("nota", IntegerType(), True)
    ])), True)
])
df = spark.createDataFrame(data, schema)

df.show( truncate=False)

# Explodindo o array para linhas individuais
df_exploded = df.withColumn("curso", explode(df["cursos"]))
df = df_exploded.select("nome", col("curso.curso"), col("curso.nota"))

df.show()

print('Exemplo de Rollup')
df_rollup = df.rollup("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_rollup.show()

print('Exemplo de Cube')
df_cube = df.cube("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_cube.show()

```

Output esperado:
```
Exemplo de Rollup
+-----+----------+---------+
| nome|     curso|avg(nota)|
+-----+----------+---------+
| NULL|      NULL|     87.5|
| João|      NULL|     87.5|
| João|  HISTORIA|     90.0|
| João|MATEMATICA|     85.0|
|Maria|      NULL|     87.5|
|Maria|  HISTORIA|     80.0|
|Maria|MATEMATICA|     95.0|
+-----+----------+---------+

Exemplo de Cube
+-----+----------+---------+
| nome|     curso|avg(nota)|
+-----+----------+---------+
| NULL|      NULL|     87.5|
| NULL|  HISTORIA|     85.0|
| NULL|MATEMATICA|     90.0|
| João|      NULL|     87.5|
| João|  HISTORIA|     90.0|
| João|MATEMATICA|     85.0|
|Maria|      NULL|     87.5|
|Maria|  HISTORIA|     80.0|
|Maria|MATEMATICA|     95.0|
+-----+----------+---------+
```

## 6. Desafio

Faça o clone do repositório abaixo:
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos
```

Examine o script a seguir e faça as alterações necessárias onde requisitado
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, hour

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-rollup-cube") \
    .getOrCreate()

# Carregando o dataset pedidos-2024-01-01.csv.gz
df = spark.read.csv("./datasets-csv-pedidos/pedidos-2024-01-01.csv.gz", sep=";", header=True, inferSchema=True)

# Mostrando o schema para verificar o carregamento correto dos dados
df.printSchema()

# Desafio ROLLUP: Agrupando pelos campos UF e PRODUTO, calcule a soma das QUANTIDADES dos produtos nos pedidos
df_rollup = df.groupBy(_______) \
                .agg(sum(_______).alias(_______)) \
                .rollup(_______) \
                .sum(_______).alias(_______) \
                .orderBy(_______)

# Mostrando os resultados parciais do rollup
df_rollup.show(truncate=False)

# Desafio CUBE: Agrupe pela HORA e UF do pedido e calcule a soma do valor total dos pedidos
# Atenção! 
# 1. O dataset nao possui valor total do pedido, apenas quantidade e valor unitario dos produtos. Sera necessario criar uma nova coluna de valor total calculado.
# 2. A coluna DATA_CRIACAO possui hora. Utilize a funcao "hour" do pacote pyspark.sql.functions.
# 3. Filtre apenas os estados SP, RJ e MG.

# Incluindo a nova coluna de data
df = df.withColumn("VALOR_TOTAL_PEDIDO", _______)
df = df.withColumn("HORA_PEDIDO", _______)

df_cube = df.filter( (_______) | (_______) | (_______)) \
            .groupBy(_______) \
            .agg(sum("_______").alias("_______")) \
            .cube("_______", "_______") \
            .sum("_______") \
            .orderBy("_______", "_______")

# Mostrando os resultados parciais do cube
df_cube.show(truncate=False)

# Parar a Spark Session
spark.stop()

```

## 7. Parabéns!
Você concluiu com sucesso os exemplos de uso das funções `rollup` e `cube` no PySpark. Agora, você está apto a aplicar essas técnicas em seus próprios conjuntos de dados para realizar análises avançadas e obter insights valiosos. Continue praticando e explorando outras funcionalidades do PySpark para aprimorar ainda mais suas habilidades em engenharia de dados.

## 8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

