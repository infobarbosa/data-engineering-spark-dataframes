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

**Exemplo de código:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import count, sum, avg, max, min, variance, stddev

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-aggregations").getOrCreate()

print("DataFrame de pedidos")
df_pedido = spark.createDataFrame([
    # Cliente MARIVALDA (dois pedidos)
    ('2b162060', 2, 'Celular', 1500.00, 3000.00, '2024-09-01', 'ELETRONICOS'),
    ('2b162060', 1, 'Notebook', 3500.00, 3500.00, '2024-09-05', 'ELETRONICOS'),
    
    # Cliente JUCILENE (um pedido de um item)
    ('2b16242a', 1, 'Geladeira', 2000.00, 2000.00, '2024-09-03', 'ELETRODOMESTICOS'),
    
    # Cliente IVONE (um pedido de um item)
    ('2b16396a', 1, 'Smart TV', 2500.00, 2500.00, '2024-09-08', 'ELETRONICOS'),
    
    # Cliente ALDENORA (um pedido de dez itens)
    ('2b16353c', 10, 'Teclado', 150.00, 1500.00, '2024-09-10', 'INFORMATICA'),
    
    # Cliente GRACIMAR (cinco pedidos de um item cada)
    ('2b16256a', 1, 'Fogão', 1200.00, 1200.00, '2024-09-02', 'ELETRODOMESTICOS'),
    ('2b16256a', 1, 'Microondas', 800.00, 800.00, '2024-09-04', 'ELETRODOMESTICOS'),
    ('2b16256a', 1, 'Máquina de Lavar', 1800.00, 1800.00, '2024-09-06', 'ELETRODOMESTICOS'),
    ('2b16256a', 1, 'Ventilador', 200.00, 200.00, '2024-09-09', 'ELETRODOMESTICOS'),
    ('2b16256a', 1, 'Aspirador de Pó', 600.00, 600.00, '2024-09-11', 'ELETRODOMESTICOS'),
    
    # Dez pedidos aleatórios
    ('2b162345', 1, 'Cafeteira', 150.00, 150.00, '2024-09-12', 'ELETRODOMESTICOS'),
    ('2b162346', 2, 'Impressora', 600.00, 1200.00, '2024-09-13', 'INFORMATICA'),
    ('2b162347', 3, 'Monitor', 750.00, 2250.00, '2024-09-14', 'INFORMATICA'),
    ('2b162348', 4, 'Bicicleta', 1000.00, 4000.00, '2024-09-15', 'ESPORTE'),
    ('2b162349', 1, 'Chuveiro', 200.00, 200.00, '2024-09-16', 'CASA E CONSTRUCAO'),
    ('2b162350', 1, 'Mesa de Jantar', 1800.00, 1800.00, '2024-09-17', 'MOVEIS'),
    ('2b162351', 1, 'Cama Box', 2500.00, 2500.00, '2024-09-18', 'MOVEIS'),
    ('2b162352', 2, 'Notebook', 3500.00, 7000.00, '2024-09-19', 'ELETRONICOS'),
    ('2b162353', 5, 'Caneta', 2.00, 10.00, '2024-09-20', 'PAPELARIA'),
    ('2b162354', 2, 'Ar Condicionado', 1500.00, 3000.00, '2024-09-21', 'ELETRODOMESTICOS')
], ["id_cliente", "quantidade", "descricao_produto", "valor_produto", "valor_total_pedido", "data_pedido", "departamento_produto"])

df_pedido.show(truncate=False)

```

### 2.2. `count`
A função `count` é utilizada para contar o número de elementos em um DataFrame ou RDD no Apache Spark. Ela retorna um valor inteiro que representa a quantidade total de registros presentes na estrutura de dados. Esta função é frequentemente usada em operações de agregação para obter o tamanho de um conjunto de dados.

```python
print('Exemplo 4.2: Contagem de registros por departamento')
count_by_desc = df_pedido.groupBy("departamento_produto").agg(count("id_cliente").alias("count"))
count_by_desc.show()

```

```python
print('Exemplo 4.2: Contagem de registros por departamento usando função embutida')
count_by_desc_builtin = df_pedido.groupBy("departamento_produto").count()
count_by_desc_builtin.show()

```

### 2.3. `sum`

A função `sum` é utilizada para calcular a soma dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para obter o total de valores numéricos em um conjunto de dados. Por exemplo, ao trabalhar com dados financeiros, você pode usar `sum` para calcular o total de vendas ou receitas.

**Exemplo:**


```python
print('Exemplo 4.3: Soma dos valores por departamento')
sum_by_desc = df_pedido.groupBy("departamento_produto").agg(sum("valor_total_pedido").alias("sum_valor"))
sum_by_desc.show()

```

```python
print('Exemplo 4.3: Soma dos valores por departamento usando função embutida')
sum_by_desc_builtin = df_pedido.groupBy("departamento_produto").sum("valor_total_pedido").alias("sum_valor")
sum_by_desc_builtin.show()

```

### 2.4. `avg()`

A função `avg` é utilizada para calcular a média dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para obter o valor médio de um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `avg` para calcular o valor médio das vendas por departamento.

**Exemplo:**

```python
print('Exemplo 4.4: Média dos valores por departamento')
avg_by_depart = df_pedido.groupBy("departamento_produto").agg(avg("valor_total_pedido").alias("avg_valor"))
avg_by_depart.show()
```

### 2.5. `max()`

A função `max` é utilizada para encontrar o valor máximo de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para identificar o maior valor em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `max` para encontrar o valor máximo das vendas por departamento.

**Exemplo:**

```python
print('Exemplo 4.5: Valor máximo por departamento')
max_by_depart = df_pedido.groupBy("departamento_produto").agg(max("valor_total_pedido").alias("max_valor"))
max_by_depart.show()
```

### 2.6. `min()`

A função `min` é utilizada para encontrar o valor mínimo de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para identificar o menor valor em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `min` para encontrar o valor mínimo das vendas por departamento.

**Exemplo:**

```python
print('Exemplo 4.6: Valor mínimo por departamento')
min_by_depart = df_pedido.groupBy("departamento_produto").agg(min("valor_total_pedido").alias("min_valor"))
min_by_depart.show()
```


### 2.7. `stddev()`

A função `stddev` é utilizada para calcular o desvio padrão dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para medir a dispersão ou variabilidade dos dados em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `stddev` para calcular a variabilidade dos valores das vendas por departamento.

**Exemplo de uso:**

```python
print('Exemplo 4.7: Desvio padrão dos valores por departamento')
stddev_by_depart = df_pedido.groupBy("departamento_produto").agg(stddev("valor_total_pedido").alias("stddev_valor"))
stddev_by_depart.show()
```

### 2.8. `variance()`

A função `variance` é utilizada para calcular a variância dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para medir a dispersão dos dados em um conjunto de dados numéricos. A variância é uma medida que indica o quão longe os valores de um conjunto de dados estão do valor médio.

**Exemplo de uso:**

```python
print('Exemplo 4.8: Variância dos valores por departamento')
variance_by_depart = df_pedido.groupBy("departamento_produto").agg(variance("valor_total_pedido").alias("variance_valor"))
variance_by_depart.show()
```

### 2.9. Agrupando múltiplas agregações

É possível realizar múltiplas agregações em um único comando no Apache Spark. Isso é útil quando você precisa calcular várias estatísticas de uma vez, como contagem, soma, média, valor máximo e valor mínimo, agrupadas por uma ou mais colunas. O exemplo a seguir mostra como agrupar os dados pelo campo `departamento_produto` e aplicar todas essas funções de agregação em uma única operação.

```python
print('Exemplo 4.9: Todas as agregações em um único comando')
all_aggregations = df_pedido.groupBy("departamento_produto").agg(
    count("id_cliente").alias("count"),
    sum("valor_total_pedido").alias("sum_valor"),
    avg("valor_total_pedido").alias("avg_valor"),
    max("valor_total_pedido").alias("max_valor"),
    min("valor_total_pedido").alias("min_valor"),
    variance("valor_total_pedido").alias("var_valor"),
    stddev("valor_total_pedido").alias("stdev_valor")
)
all_aggregations.show()
```

---

## 3. Desafio

Faça o clone do repositório abaixo:
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos

```

Examine os scripts a seguir e faça as alterações necessárias onde requisitado
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, date_trunc

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
```

#### Desafio 1 
Agrupando pelos campo UF, calcule a soma das QUANTIDADES dos produtos nos pedidos

```python
# Desafio AGREGAÇAO 1: 
#   Agrupando pelos campo UF, calcule a soma das QUANTIDADES dos produtos nos pedidos
df_agg1 = df.groupBy(_______) \
            .agg(sum(_______).alias(_______)) 

print("Resultado do desafio de agregacao 1")
df_agg1.show(truncate=False)
```

Output esperado:
```
+---+----+
|UF |qtt |
+---+----+
|SC |2287|
|RO |2246|
|PI |2306|
|AM |2316|
|RR |2295|
|GO |2357|
|TO |2320|
|MT |2348|
|SP |2157|
|PB |2174|
|ES |2376|
|RS |2252|
|MS |2227|
|AL |2267|
|MG |2226|
|PA |2414|
|BA |2287|
|SE |2301|
|PE |2384|
|CE |2383|
+---+----+

```

#### Desafio 2
Agrupe pelo atributo PRODUTO, calcule a soma do valor total dos pedidos

```python
# Desafio AGREGAÇAO 2: 
#   Agrupe pelo atributo PRODUTO, calcule a soma do valor total dos pedidos
#   Atenção! 
#   O dataset nao possui valor total do pedido, apenas quantidade e valor unitario dos produtos. 
#   Dessa forma, sera necessario criar uma nova coluna de valor total calculado.

# Incluindo a nova coluna de valor total do pedido
df = df.withColumn("VALOR_TOTAL_PEDIDO", _______)

df_agg2 = df.groupBy(_______) \
            .agg(sum("_______").alias("_______")) \
            .orderBy(col("______").desc())

print("Resultado do desafio de agregacao 2")
df_agg2.show(truncate=False)

```

Output esperado:
```
+-----------+--------+
|produto    |total   |
+-----------+--------+
|TV         |17475000|
|GELADEIRA  |13532000|
|NOTEBOOK   |10557000|
|TABLET     |7719800 |
|CELULAR    |7080000 |
|SOUNDBAR   |5878800 |
|COMPUTADOR |4831400 |
|MONITOR    |4109400 |
|HOMETHEATER|3392000 |
+-----------+--------+
```

#### Desafio 3
Agrupe pela DATA DO PEDIDO, calcule a soma do valor total dos pedidos

```python
# Desafio AGREGAÇAO 3: Agrupe pela DATA DO PEDIDO e calcule a soma do valor total dos pedidos
# Atenção! 
#   O dataset nao possui valor total do pedido, apenas quantidade e valor unitario dos produtos. 
#   Dessa forma, sera necessario criar uma nova coluna de valor total calculado.
#   O atributo DATA_CRIACAO possui hora, minuto e segundo. Utilize a funcao date_trunc para truncar o valor.

# Incluindo a nova coluna de data truncada
df = df.withColumn("DATA_PEDIDO", _______)

# Incluindo a nova coluna de valor total do pedido
df = df.withColumn("VALOR_TOTAL_PEDIDO", _______)

df_agg3 = df.groupBy(_______) \
            .agg(sum("_______").alias("_______")) \
            .orderBy(col("_______").asc()) 

print("Resultado do desafio de agregacao 3")
df_agg3.show(truncate=False)

# Parar a Spark Session
spark.stop()

```

Output esperado:
```
+-------------------+-------+
|data_pedido        |total  |
+-------------------+-------+
|2024-01-01 00:00:00|2367100|
|2024-01-02 00:00:00|2437000|
|2024-01-03 00:00:00|2356600|
|2024-01-04 00:00:00|2374200|
|2024-01-05 00:00:00|2338400|
|2024-01-06 00:00:00|2391400|
|2024-01-07 00:00:00|2404800|
|2024-01-08 00:00:00|2370200|
|2024-01-09 00:00:00|2417000|
|2024-01-10 00:00:00|2416700|
|2024-01-11 00:00:00|2429000|
|2024-01-12 00:00:00|2424500|
|2024-01-13 00:00:00|2381100|
|2024-01-14 00:00:00|2438000|
|2024-01-15 00:00:00|2465400|
|2024-01-16 00:00:00|2361300|
|2024-01-17 00:00:00|2447200|
|2024-01-18 00:00:00|2398700|
|2024-01-19 00:00:00|2434800|
|2024-01-20 00:00:00|2512200|
|2024-01-21 00:00:00|2435300|
|2024-01-22 00:00:00|2439800|
|2024-01-23 00:00:00|2314600|
|2024-01-24 00:00:00|2356500|
|2024-01-25 00:00:00|2341800|
|2024-01-26 00:00:00|2332500|
|2024-01-27 00:00:00|2435700|
|2024-01-28 00:00:00|2435600|
|2024-01-29 00:00:00|2463400|
|2024-01-30 00:00:00|2484300|
|2024-01-31 00:00:00|2370300|
+-------------------+-------+

```




---

## 4. Rollups

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
df_rollup = df.rollup("categoria_produto", "ano") \
              .agg(sum("vendas").alias("total_vendas")) \
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


## 5. Cubes

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
df_cube = df.cube("categoria_produto", "ano") \
            .agg(sum("vendas").alias("total_vendas")) \
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

## 6. Resumo

- **`rollup`**: Cria agregações hierárquicas e totais gerais. Útil para relatórios que exigem subtotais em diferentes níveis.
- **`cube`**: Cria todas as combinações possíveis dos grupos, permitindo análises mais detalhadas em várias dimensões.

Ambos são ferramentas poderosas para análise e sumarização de dados em PySpark.

## 7. Exemplo com Arrays e Structs
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

## 8. Desafio

Faça o clone do repositório abaixo:
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos

```

#### Desafio 1 

Desafio **ROLLUP**: Agrupando pelos campos UF e PRODUTO, calcule a soma das QUANTIDADES dos produtos nos pedidos

Examine o script a seguir e faça as alterações necessárias onde requisitado
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, hour

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-rollup-cube") \
    .getOrCreate()

# Carregando o dataset pedidos
df = spark.read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .option("sep", ";") \
    .load("./datasets-csv-pedidos/*.csv.gz", inferSchema=True)

# Mostrando o schema para verificar o carregamento correto dos dados
df.printSchema()

# Implemente sua lógica de agregação com ROLLUP aqui
df_rollup = df.rollup...

# Mostrando os resultados parciais do rollup
df_rollup.show(truncate=False)

# Parar a Spark Session
spark.stop()

```

#### Desafio 2 

Desafio **CUBE**: Agrupe pela HORA e UF do pedido e calcule a soma do valor total dos pedidos.<br>
Atenção! 
  - O dataset nao possui valor total do pedido, apenas quantidade e valor unitario dos produtos. Será necessário criar uma nova coluna de valor total calculado.
  - A coluna DATA_CRIACAO possui hora. Utilize a funcao "hour" do pacote pyspark.sql.functions.
  - Filtre apenas os estados SP, RJ e MG.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, hour

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-rollup-cube") \
    .getOrCreate()

# Carregando o dataset pedidos
df = spark.read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .option("sep", ";") \
    .load("./datasets-csv-pedidos/*.csv.gz", inferSchema=True)

# Mostrando o schema para verificar o carregamento correto dos dados
df.printSchema()

# Incluindo a nova coluna VALOR_TOTAL_PEDIDO
df = df.withColumn...
# Incluindo a nova coluna HORA_PEDIDO
df = df.withColumn...

# Filtrando apenas os estados SP, RJ e MG
df_filtrado = df.filter...

# Implemente sua lógica de agregação com CUBE aqui
df_cube = df.cube...

# Mostrando os resultados parciais do cube
df_cube.show(truncate=False)

# Parar a Spark Session
spark.stop()

```

## 9. Parabéns!
Você concluiu com sucesso os exemplos de uso das funções `rollup` e `cube` no PySpark. Agora, você está apto a aplicar essas técnicas em seus próprios conjuntos de dados para realizar análises avançadas e obter insights valiosos. Continue praticando e explorando outras funcionalidades do PySpark para aprimorar ainda mais suas habilidades em engenharia de dados.

## 10. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.
