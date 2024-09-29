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

**Exemplo:**
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

## 3. Broadcast Join
O Broadcast Join é uma técnica eficiente para realizar joins quando uma das tabelas é pequena o suficiente para ser copiada para todos os nós de processamento.

**Exemplo de código:**
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

## 4. Técnicas de Agregação
### 4.1. groupBy
A operação `groupBy` permite agrupar os dados com base em uma ou mais colunas e aplicar funções agregadas.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import count, sum, avg, max, min

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-aggregations").getOrCreate()

# Exemplo de DataFrames para join
df1 = spark.createDataFrame([(1, 3.0), (2, 5.0), (1, 10.0), (1, 7.0), (2, 8.0), ], ["id", "valor"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "desc"])

# Realizando um join entre os DataFrames
joined_df = df1.join(df2, "id")

print('Exemplo 1: Contagem de registros por descrição')
count_by_desc = joined_df.groupBy("desc").agg(count("id").alias("count"))
count_by_desc.show()

print('Exemplo 1: Contagem de registros por descrição usando função embutida')
count_by_desc_builtin = joined_df.groupBy("desc").count()
count_by_desc_builtin.show()

print('Exemplo 2: Soma dos valores por descrição')
sum_by_desc = joined_df.groupBy("desc").agg(sum("valor").alias("sum_valor"))
sum_by_desc.show()

print('Exemplo 2: Soma dos valores por descrição usando função embutida')
sum_by_desc_builtin = joined_df.groupBy("desc").sum("valor").alias("sum_valor")
sum_by_desc_builtin.show()

print('Exemplo 3: Média dos valores por descrição')
avg_by_desc = joined_df.groupBy("desc").agg(avg("valor").alias("avg_valor"))
avg_by_desc.show()

print('Exemplo 3: Média dos valores por descrição usando função embutida')
avg_by_desc_builtin = joined_df.groupBy("desc").avg("valor").alias("avg_valor")
avg_by_desc_builtin.show()

print('Exemplo 4: Valor máximo por descrição')
max_by_desc = joined_df.groupBy("desc").agg(max("valor").alias("max_valor"))
max_by_desc.show()

print('Exemplo 5: Valor mínimo por descrição')
min_by_desc = joined_df.groupBy("desc").agg(min("valor").alias("min_valor"))
min_by_desc.show()

print('Exemplo 6: Todas as agregações em um único comando')
all_aggregations = joined_df.groupBy("desc").agg(
    count("id").alias("count"),
    sum("valor").alias("sum_valor"),
    avg("valor").alias("avg_valor"),
    max("valor").alias("max_valor"),
    min("valor").alias("min_valor")
)
all_aggregations.show()

```


---
## 5. Parabéns!
Parabéns por concluir o módulo! Agora você domina as operações de junção e agregação mais básicas no Apache Spark.

## 6. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

