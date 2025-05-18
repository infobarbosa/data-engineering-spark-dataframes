# Operações de Junção e Agregação

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Neste módulo, vamos aprofundar nosso conhecimento em operações de junção e agregação no Apache Spark, explorando tipos de joins, técnicas de agregação, e o uso de funções analíticas para cálculos mais sofisticados.

## 2. Joins
Os joins no Apache Spark são operações fundamentais que permitem combinar dados de diferentes DataFrames com base em uma chave comum. Eles são essenciais para a integração de dados provenientes de diversas fontes e para a realização de análises complexas. <br>
O Spark oferece vários tipos de joins, como **inner join**, **left join**, **right join**, **full join** e **cross join**, cada um com suas características e casos de uso específicos. <br>
A eficiência dessas operações pode ser aprimorada com técnicas como o Broadcast Join, especialmente quando se trabalha com grandes volumes de dados.

**Exemplo 1**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-joins").getOrCreate()

print("Dataframe de UF")
df_uf = spark.createDataFrame([
    ("RO", "Rondônia"),
    ("AC", "Acre"),
    ("AM", "Amazonas"),
    ("RR", "Roraima"),
    ("PA", "Pará"),
    ("AP", "Amapá"),
    ("TO", "Tocantins"),
    ("MA", "Maranhão"),
    ("PI", "Piauí"),
    ("CE", "Ceará"),
    ("RN", "Rio Grande do Norte"),
    ("PB", "Paraíba"),
    ("PE", "Pernambuco"),
    ("AL", "Alagoas"),
    ("SE", "Sergipe"),
    ("BA", "Bahia"),
    ("MG", "Minas Gerais"),
    ("ES", "Espírito Santo"),
    ("RJ", "Rio de Janeiro"),
    ("SP", "São Paulo"),
    ("PR", "Paraná"),
    ("SC", "Santa Catarina"),
    ("RS", "Rio Grande do Sul"),
    ("MS", "Mato Grosso do Sul"),
    ("MT", "Mato Grosso"),
    ("GO", "Goiás"),
    ("DF", "Distrito Federal")
], ["uf", "nome"])

df_uf.show()

print("DataFrame de clientes")
df_cliente = spark.createDataFrame([
    ('2b162060', 'MARIVALDA', 'SP'),
    ('2b16242a', 'JUCILENE',  'ES'),
    ('2b16256a', 'GRACIMAR',  'MG'),
    ('2b16353c', 'ALDENORA',  'SP'),
    ('2b1636ae', 'VERA',      'RJ'),
    ('2b16396a', 'IVONE',     'RJ'),
    ('2b163bcc', 'LUCILIA',   'RS'),
    ('2b163bff', 'MARTINS',   ''),
    ('2b163bdd', 'GENARO',    ''),
], ["id_cliente", "nome", "uf"])

df_cliente.show()

print("DataFrame de pedidos")
df_pedido = spark.createDataFrame([
    # Cliente MARIVALDA (dois pedidos)
    ('2b162060', 2, 'Celular', 1500.00, 3000.00, '2024-09-01'),
    ('2b162060', 1, 'Notebook', 3500.00, 3500.00, '2024-09-05'),
    
    # Cliente JUCILENE (um pedido de um item)
    ('2b16242a', 1, 'Geladeira', 2000.00, 2000.00, '2024-09-03'),
    
    # Cliente IVONE (um pedido de um item)
    ('2b16396a', 1, 'Smart TV', 2500.00, 2500.00, '2024-09-08'),
    
    # Cliente ALDENORA (um pedido de dez itens)
    ('2b16353c', 10, 'Teclado', 150.00, 1500.00, '2024-09-10'),
    
    # Cliente GRACIMAR (cinco pedidos de um item cada)
    ('2b16256a', 1, 'Fogão', 1200.00, 1200.00, '2024-09-02'),
    ('2b16256a', 1, 'Microondas', 800.00, 800.00, '2024-09-04'),
    ('2b16256a', 1, 'Máquina de Lavar', 1800.00, 1800.00, '2024-09-06'),
    ('2b16256a', 1, 'Ventilador', 200.00, 200.00, '2024-09-09'),
    ('2b16256a', 1, 'Aspirador de Pó', 600.00, 600.00, '2024-09-11')
], ["id_cliente", "quantidade", "descricao_produto", "valor_produto", "valor_total_pedido", "data_pedido"])

df_pedido.show()
```

### 2.1 Inner Joins
Um **Inner Join** é uma operação de junção entre dois DataFrames que retorna apenas as linhas que têm correspondências em ambas as tabelas. No contexto do código fornecido, o `df_cliente` e o `df_pedido` são unidos usando a coluna `id_cliente` como chave. Isso significa que o resultado do Inner Join incluirá apenas os registros onde o valor de id_cliente está presente em ambos os DataFrames. Essa operação é útil para combinar dados relacionados de diferentes fontes, garantindo que apenas as correspondências exatas sejam incluídas no resultado final.

```python
# Inner Join entre df_cliente e df_pedido usando a coluna id_cliente como chave
print("Inner Join:")
df_inner_join = df_cliente.join(df_pedido, on='id_cliente', how='inner')
df_inner_join.show()

```

### 2.2 Left Joins
Um **Left Join** (ou Left Outer Join) combina dois DataFrames mantendo todas as linhas do DataFrame da esquerda e adicionando as linhas correspondentes do DataFrame da direita. Se não houver correspondência, os valores do DataFrame da direita serão preenchidos com null.

```python
# Left Join (ou Left Outer Join) - Mantém todos os registros do DataFrame da esquerda (df1) e adiciona os registros correspondentes do DataFrame da direita (df2)
print("Left Join:")
df_inner_join = df_cliente.join(df_pedido, on='id_cliente', how='left')
df_inner_join.show()

```

### 2.3 Right Joins
**Right Joins** (ou Right Outer Joins) mantêm todos os registros do DataFrame da direita e adicionam os registros correspondentes do DataFrame da esquerda. Se não houver correspondência, os valores do DataFrame da esquerda serão preenchidos com null.

```python
# Right Join (ou Right Outer Join) - Mantém todos os registros do DataFrame da direita (df2) e adiciona os registros correspondentes do DataFrame da esquerda (df1)
print("Right Join:")
df_right_join = df_cliente.join(df_uf, "uf", "right")
df_right_join.show()

```

### 2.4 Full Joins
Um **Full Join** (ou Full Outer Join) combina todos os registros de ambos os DataFrames, preenchendo com valores nulos onde não há correspondência entre as chaves. Isso garante que nenhum dado seja perdido, mesmo que não haja correspondência entre os DataFrames.

```python
# Full Join (ou Full Outer Join) - Mantém todos os registros de ambos os DataFrames (df1 e df2), preenchendo com nulls onde não há correspondência
print("Full Join:")
df_full_join = df_cliente.join(df_uf, "uf", "full")
df_full_join.show()

```

### 2.5 Cross Joins
Um **Cross Join** (ou Cartesian Join) é uma operação que combina cada linha de um DataFrame com cada linha de outro DataFrame, resultando em um produto cartesiano. No Apache Spark, essa operação pode ser útil em situações onde é necessário comparar todas as combinações possíveis de registros entre dois DataFrames. No entanto, deve-se ter cuidado ao usar Cross Joins, pois o número de linhas no resultado pode crescer exponencialmente, levando a um alto consumo de memória e tempo de processamento.

```python
# Cross Join (ou Cartesian Join) - Faz o produto cartesiano entre os DataFrames, ou seja, combina cada linha de df1 com cada linha de df2
print("Cross Join:")
df_cross_join = df_cliente.crossJoin(df_uf)
df_cross_join.show()

```

## 3. Exemplo 2
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("joins-chaves-compostas").getOrCreate()

print("### Centro de Distribuição")
centro_distribuição_data = [
    ("SP", "Centro de Distribuição SP"),
    ("RJ", "Centro de Distribuição RJ"),
    ("MG", "Centro de Distribuição MG")
]
centro_distribuição_columns = ["id_centro_dist", "ds_centro_dist"]
centro_distribuição_df = spark.createDataFrame(centro_distribuição_data, centro_distribuição_columns)
centro_distribuição_df.show(truncate=False)

print("### Produtos")
produtos_data = [
    (10, "Notebook", "Eletrônicos", 1800.0),
    (20, "Geladeira", "Eletrodomésticos", 2000.0),
    (30, "Smartphone", "Eletrônicos", 1500.0),
    (40, "Fogão", "Eletrodomésticos", 800.0),
    (50, "Tablet", "Eletrônicos", 1200.0),
]
produtos_df = spark.createDataFrame(produtos_data, ["id_produto", "ds_produto", "ds_categoria", "vl_unitario"])
produtos_df.printSchema()
produtos_df.show(truncate=False)

print("### Clientes")
clientes_data = [
    (1, "SP", "2020-01-01", "Barbosa"),
    (1, "RJ", "2019-06-01", "Carlos"),
    (2, "RJ", "2018-02-15", "Renato"),
    (2, "SP", "2016-09-01", "Roberto"),
    (3, "MG", "2014-03-10", "Marcelo"),
    (3, "RJ", "2018-07-01", "Fernanda"),
    (4, "SP", "2023-04-05", "Guilherme"),
    (4, "MG", "2023-08-01", "Juliana"),
    (5, "SP", "2023-05-01", "Ana"),
    (5, "MG", "2023-10-01", "Patricia")
]
clientes_columns = ["id_cliente", "uf_cliente", "dt_cadastro", "nome"]
clientes_df = spark.createDataFrame(clientes_data, clientes_columns)
clientes_df.printSchema()
clientes_df.show(truncate=False)

print("### Pedidos")
pedidos_data = [
    (101, 1, "SP", "2025-01-01", "ativo"),
    (102, 2, "RJ", "2025-02-03", "cancelado"),
    (103, 3, "MG", "2025-03-05", "ativo"),
    (104, 4, "SP", "2025-04-07", "ativo"),
    (105, 1, "SP", "2025-05-09", "ativo"),
    (106, 5, "MG", "2025-05-10", "ativo"),
    (107, 3, "RJ", "2025-05-11", "ativo"),

]
pedidos_schema = StructType([
    StructField("id_pedido", IntegerType(), True),
    StructField("id_cliente", IntegerType(), True),
    StructField("uf_cliente", StringType(), True),
    StructField("dt_pedido", StringType(), True),
    StructField("status", StringType(), True)
])

pedidos_df = spark.createDataFrame(pedidos_data, pedidos_schema)
pedidos_df.printSchema()
pedidos_df.show(truncate=False)

print("### Pagamentos")
pagamentos_data = [
    (1001, 101, "2025-05-02", "cartao", 1800.0),
    (1002, 102, "2025-05-04", "boleto", 2000.0),
    (1003, 103, "2025-05-06", "pix", 1500.0),
    (1004, 104, "2025-05-10", "cartao", 800.0),
    (1005, 105, "2025-05-12", "cartao", 2400.0),
    (1006, 106, "2025-05-14", "cartao", 1200.0),
    (1007, 107, "2025-05-16", "pix", 1500.0)
]
pagamentos_columns = ["id_pagamento", "id_pedido", "dt_pagamento", "ds_metodo", "valor"]
pagamentos_df = spark.createDataFrame(pagamentos_data, pagamentos_columns)
pagamentos_df.printSchema()
pagamentos_df.show(truncate=False)

print("### Itens de Pedidos")
itens_pedidos_data = [
    (101, 10, 1, "SP"),
    (102, 20, 1, "RJ"),
    (103, 30, 1, "MG"),
    (104, 40, 1, "SP"),
    (105, 50, 2, "SP"),
    (106, 10, 1, "MG"),
    (107, 20, 1, "RJ")
]
itens_pedidos_columns = ["id_pedido", "id_produto", "quantidade", "id_centro_dist"]
itens_pedidos_df = spark.createDataFrame(itens_pedidos_data, itens_pedidos_columns)
itens_pedidos_df.printSchema()
itens_pedidos_df.show(truncate=False)

# OBTER UM RELATÓRIO DE ENTREGAS DO CENTRO DE DISTRIBUIÇÃO DE MG MAIO DE 2025
# O relatório deve conter o centro de distribuição, o produto e quantidade
# 1. Filtrar os pedidos do centro de distribuição de MG
# 2. Filtrar os pedidos do mês de maio de 2025
# 3. Juntar as tabelas pedidos, itens_pedidos, pagamentos e clientes
# 4. Exibir as colunas: id_pedido, id_cliente, dt_pedido, dt_pagamento, ds_metodo, valor, nome
# 5. Ordenar pelo id_pedido
# 6. Exibir o resultado

df_entregas_mg = (
    pedidos_df
    .filter((col("uf_cliente") == "MG") & (col("dt_pedido").between("2025-05-01", "2025-05-31")))
    .join(itens_pedidos_df, ["id_pedido"], "inner")
    .join(produtos_df, ["id_produto"], "inner")
    .join(clientes_df, ["id_cliente", "uf_cliente"], "inner")
    .join(centro_distribuição_df, ["id_centro_dist"], "inner")
    .select("ds_centro_dist", "ds_produto", "quantidade", "id_pedido", "id_cliente", "dt_pedido", "nome")
    .orderBy("id_pedido")
)   
df_entregas_mg.show(truncate=False)

```

## 4. Broadcast Join
O Broadcast Join é uma técnica eficiente para realizar joins quando uma das tabelas é pequena o suficiente para ser copiada para todos os nós de processamento.

**Exemplo:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-broadcast-join").getOrCreate()

# Exemplo de DataFrames para join
df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "valor"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "desc"])

# Broadcast join
df_join = df1.join(broadcast(df2), "id")
df_join.show()
```
---

## 5. Técnicas de Agregação
### 5.1. groupBy
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

### 5.2. `count`
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

### 5.3. `sum`

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

### 5.4. `avg()`

A função `avg` é utilizada para calcular a média dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para obter o valor médio de um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `avg` para calcular o valor médio das vendas por departamento.

**Exemplo:**

```python
print('Exemplo 4.4: Média dos valores por departamento')
avg_by_depart = df_pedido.groupBy("departamento_produto").agg(avg("valor_total_pedido").alias("avg_valor"))
avg_by_depart.show()
```

### 5.5. `max()`

A função `max` é utilizada para encontrar o valor máximo de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para identificar o maior valor em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `max` para encontrar o valor máximo das vendas por departamento.

**Exemplo:**

```python
print('Exemplo 4.5: Valor máximo por departamento')
max_by_depart = df_pedido.groupBy("departamento_produto").agg(max("valor_total_pedido").alias("max_valor"))
max_by_depart.show()
```

### 5.6. `min()`

A função `min` é utilizada para encontrar o valor mínimo de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para identificar o menor valor em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `min` para encontrar o valor mínimo das vendas por departamento.

**Exemplo:**

```python
print('Exemplo 4.6: Valor mínimo por departamento')
min_by_depart = df_pedido.groupBy("departamento_produto").agg(min("valor_total_pedido").alias("min_valor"))
min_by_depart.show()
```


### 5.7. `stddev()`

A função `stddev` é utilizada para calcular o desvio padrão dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para medir a dispersão ou variabilidade dos dados em um conjunto de dados numéricos. Por exemplo, ao trabalhar com dados de vendas, você pode usar `stddev` para calcular a variabilidade dos valores das vendas por departamento.

**Exemplo de uso:**

```python
print('Exemplo 4.7: Desvio padrão dos valores por departamento')
stddev_by_depart = df_pedido.groupBy("departamento_produto").agg(stddev("valor_total_pedido").alias("stddev_valor"))
stddev_by_depart.show()
```

### 5.8. `variance()`

A função `variance` é utilizada para calcular a variância dos valores de uma coluna específica em um DataFrame. Esta função é frequentemente usada em operações de agregação para medir a dispersão dos dados em um conjunto de dados numéricos. A variância é uma medida que indica o quão longe os valores de um conjunto de dados estão do valor médio.

**Exemplo de uso:**

```python
print('Exemplo 4.8: Variância dos valores por departamento')
variance_by_depart = df_pedido.groupBy("departamento_produto").agg(variance("valor_total_pedido").alias("variance_valor"))
variance_by_depart.show()
```

### 5.9. Agrupando múltiplas agregações

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

## 6. Desafio

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
## 7. Parabéns!
Parabéns por concluir o módulo! Agora você domina as operações de junção e agregação mais básicas no Apache Spark.

## 8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

