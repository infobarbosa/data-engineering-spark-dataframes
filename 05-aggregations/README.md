# Agregações

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Agregações de dados são operações fundamentais na análise e processamento de grandes volumes de informação, permitindo transformar conjuntos de dados brutos em resumos significativos. No Apache Spark, as agregações são amplamente utilizadas para consolidar informações, calcular métricas e extrair insights, sendo essenciais para tarefas como a geração de relatórios, a construção de dashboards e a preparação de dados para modelos de Machine Learning. O Spark oferece diversas funções e métodos para realizar agregações de forma eficiente e escalável, desde operações básicas como contagem e soma até técnicas mais avançadas como `rollup` e `cube`.

---

## 2. Técnicas de Agregação
### 2.1. groupBy
A operação `groupBy` permite agrupar os dados com base em uma ou mais colunas e aplicar funções agregadas.

```python
    df_agregado = df.groupBy("coluna1", "coluna2") ... 

```

#### Exemplo 1
```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, MapType, DoubleType, FloatType, IntegerType, DateType
from datetime import date

spark = SparkSession.builder \
    .appName("data-eng-joins") \
    .getOrCreate()

schema_pedidos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", FloatType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("data_criacao", DateType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True)
])

df_pedidos = spark.read. \
    schema(schema_pedidos). \
    option("header", "true") \
    .option("sep", ";") \
    .csv("./datasets-csv-pedidos/data/pedidos/")

df_pedidos.select("id_pedido", "id_cliente", "produto", "valor_unitario", "quantidade", "data_criacao", "uf").show(10, truncate=False)
df_pedidos.printSchema()
print("count: ", df_pedidos.count())

```

#### 2.2. `count`
A função `count` é utilizada para contar o número de elementos em um DataFrame ou RDD no Apache Spark. Ela retorna um valor inteiro que representa a quantidade total de registros presentes na estrutura de dados. Esta função é frequentemente usada em operações de agregação para obter o tamanho de um conjunto de dados.

```python

# Usando a buil-in function count()
print("### COUNT ###")
df_pedidos_por_uf = df_pedidos.groupBy("uf").count()
df_pedidos_por_uf.show()

# Usando agg
print("### AGG ###")
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(F.count("id_pedido").alias("total_pedidos"))
df_pedidos_por_uf.show()

```


#### 2.3. `sum`

A função `sum` é utilizada para calcular a soma dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para obter o total de valores numéricos em um conjunto de dados. Por exemplo, ao trabalhar com dados financeiros, você pode usar `sum` para calcular o total de vendas ou receitas.

**Exemplo:**


```python

print("### SUM ###")
# Acrescentando uma coluna de valor total
df_pedidos = df_pedidos.withColumn("valor_total", df_pedidos["valor_unitario"] * df_pedidos["quantidade"])
# Usando built-in functions
df_pedidos_por_uf = df_pedidos.groupBy("uf").sum("valor_total")
df_pedidos_por_uf.show()

# Usando agg
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(F.sum("valor_total").alias("total_valor"))
df_pedidos_por_uf.show()

```

#### 2.4. `avg()`

A função `avg` é utilizada para calcular a média dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para obter o valor médio de um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `avg` para calcular o valor médio das vendas por departamento.

**Exemplo:**

```python

print("### AVG ###")
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(F.avg("valor_total").alias("media_valor"))
df_pedidos_por_uf.show()

```

#### 2.5. `max()`

A função `max` é utilizada para encontrar o valor máximo de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para identificar o maior valor em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `max` para encontrar o valor máximo das vendas por departamento.

**Exemplo:**

```python

print("### MAX ###")
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(F.max("valor_total").alias("max_valor"))
df_pedidos_por_uf.show()

```

#### 2.6. `min()`

A função `min` é utilizada para encontrar o valor mínimo de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para identificar o menor valor em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `min` para encontrar o valor mínimo das vendas por departamento.

**Exemplo:**

```python

print("### MIN ###")
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(F.min("valor_total").alias("min_valor"))
df_pedidos_por_uf.show()

```


#### 2.7. `stddev()`

A função `stddev` é utilizada para calcular o desvio padrão dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para medir a dispersão ou variabilidade dos dados em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `stddev` para calcular a variabilidade dos valores das vendas por departamento.

**Exemplo de uso:**

```python

print("### STDDEV ###")
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(F.stddev("valor_total").alias("stddev_valor"))
df_pedidos_por_uf.show()

```

#### 2.8. `variance()`

A função `variance` é utilizada para calcular a variância dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para medir a dispersão dos dados em um conjunto de dados numéricos. A variância é uma medida que indica o quão longe os valores de um conjunto de dados estão do valor médio.

**Exemplo de uso:**

```python

print("### VAR ###")
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(F.variance("valor_total").alias("var_valor"))
df_pedidos_por_uf.show()

```

#### 2.9. Agrupando múltiplas agregações

É possível realizar múltiplas agregações em um único comando no Apache Spark. Isso é útil quando você precisa calcular várias estatísticas de uma vez, como contagem, soma, média, valor máximo e valor mínimo, agrupadas por uma ou mais colunas. O exemplo a seguir mostra como agrupar os dados pelo campo `departamento_produto` e aplicar todas essas funções de agregação em uma única operação.

```python
print("Múltiplas agregações")
df_pedidos_por_uf = df_pedidos.groupBy("uf").agg(
    F.count("id_pedido").alias("total_pedidos"), 
    F.sum("valor_total").alias("total_valor"), 
    F.avg("valor_total").alias("media_valor"), 
    F.max("valor_total").alias("max_valor"), 
    F.min("valor_total").alias("min_valor"), 
    F.stddev("valor_total").alias("stddev_valor"), 
    F.variance("valor_total").alias("var_valor")
)
df_pedidos_por_uf.show()
```

---

#### 2.10. Agregando por mais de uma coluna
```python
print("### GROUPBY MULTIPLAS COLUNAS ###")
df_pedidos = df_pedidos.withColumn("ano_mes_criacao", F.date_format("data_criacao", "yyyy-MM"))
df_vendas = df_pedidos.filter(F.col("uf") == "SP")
df_vendas.show()
df_pedidos_por_uf_mes = df_vendas.groupBy("uf", "ano_mes_criacao").agg(
    F.count("id_pedido").alias("total_pedidos"), 
    F.sum("valor_total").alias("total_valor"), 
    F.avg("valor_total").alias("media_valor"), 
    F.max("valor_total").alias("max_valor"), 
    F.min("valor_total").alias("min_valor"), 
    F.stddev("valor_total").alias("stddev_valor"), 
    F.variance("valor_total").alias("var_valor")
).orderBy("ano_mes_criacao", ascending=False)

df_pedidos_por_uf_mes.show(truncate=False)

```
---

## 3. Desafio 1
Utilizando a base pedidos dos exemplos anteriores, responda: qual foi o produto que mais vendeu em quantidade no mês de Novembro/25?

## 4. Desafio 2
Utilizando a base de pedidos dos exemplos anteriores, responda: qual foi o produto com **maior receita** em 2025? E qual foi o produto com **menor receita** nesse mesmo ano?

## 5. Desafio 3
Utilizando a base de pedidos do Bolsa Família dos laboratórios anteriores, responda: 

---

## 4. Rollups

A função `rollup` é usada para gerar uma hierarquia de agregações. Ela permite agregar dados em diferentes níveis de granularidade. O `rollup` cria uma série de subtotais e um total geral, permitindo ver a evolução dos dados em níveis agregados.

**Exemplo de uso do `rollup`**:
```python

print("### ROLLUP ###")
# inclui uma coluna Ano no formato yyyy
df_pedidos = df_pedidos.withColumn("ano_criacao", F.date_format("data_criacao", "yyyy"))
df_vendas = df_pedidos.filter((F.col("uf") == "RJ") & (F.col("produto") == "TABLET"))
df_vendas.show()
df_pedidos_por_uf_mes_rollup = df_vendas.rollup("ano_criacao").agg(
    F.count("id_pedido").alias("total_pedidos"), 
    F.sum("valor_total").alias("total_valor"), 
    F.avg("valor_total").alias("media_valor"), 
    F.max("valor_total").alias("max_valor"), 
    F.min("valor_total").alias("min_valor"), 
    F.stddev("valor_total").alias("stddev_valor"), 
    F.variance("valor_total").alias("var_valor")
).orderBy("ano_criacao", ascending=False)

print("Total de linhas: ", df_pedidos_por_uf_mes_rollup.count())
df_pedidos_por_uf_mes_rollup.show(50, truncate=False)

```

---

## 5. Cubes

A função `cube` é usada para criar uma agregação em todas as combinações possíveis dos grupos especificados. Isso é útil para obter um resumo completo dos dados para todas as combinações dos grupos fornecidos.

**Exemplo de uso do `cube`**:
```python

print("### CUBE ###")
# inclui uma coluna Ano no formato yyyy
df_vendas = df_pedidos.filter(F.col("uf") == "MG")
df_vendas.show()
df_pedidos_por_uf_mes_cube = df_vendas.cube("ano_criacao", "produto").agg(
    F.count("id_pedido").alias("total_pedidos"), 
    F.sum("valor_total").alias("total_valor"), 
    F.avg("valor_total").alias("media_valor"), 
    F.max("valor_total").alias("max_valor"), 
    F.min("valor_total").alias("min_valor"), 
    F.stddev("valor_total").alias("stddev_valor"), 
    F.variance("valor_total").alias("var_valor")
).orderBy("ano_criacao", "produto", ascending=False)

print("Total de linhas: ", df_pedidos_por_uf_mes_cube.count())
df_pedidos_por_uf_mes_cube.show(50, truncate=False)

```

## 6. Resumo

- **`rollup`**: Cria agregações hierárquicas e totais gerais. Útil para relatórios que exigem subtotais em diferentes níveis.
- **`cube`**: Cria todas as combinações possíveis dos grupos, permitindo análises mais detalhadas em várias dimensões.

Ambos são ferramentas poderosas para análise e sumarização de dados em PySpark.

---

## 7. Parabéns!
Você concluiu com sucesso os exemplos de uso das funções `rollup` e `cube` no PySpark. Agora, você está apto a aplicar essas técnicas em seus próprios conjuntos de dados para realizar análises avançadas e obter insights valiosos. Continue praticando e explorando outras funcionalidades do PySpark para aprimorar ainda mais suas habilidades em engenharia de dados.

## 8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.
