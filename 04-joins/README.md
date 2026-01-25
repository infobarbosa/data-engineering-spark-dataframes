# Operações de Junção e Agregação

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Neste módulo, vamos aprofundar nosso conhecimento em operações de junção no Apache Spark explorando os diferentes tipos de joins disponíveis.

### Datasets
Esse laboratório utiliza dois conjuntos de dados:
#### Clientes
```sh
git clone https://github.com/infobarbosa/dataset-json-clientes.git

```

```sh
zcat dataset-json-clientes/data/clientes.json.gz | wc -l

````

```sh
zcat dataset-json-clientes/data/clientes.json.gz | head -10

```

#### Pedidos

```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git

```

```sh
zcat datasets-csv-pedidos/data/pedidos/pedidos-2025-12.csv.gz | wc -l 

```

```sh
zcat datasets-csv-pedidos/data/pedidos/pedidos-2025-12.csv.gz | head -10

```



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

---

## 3. Broadcast Join
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

## 4. Desafio

Elaborar um relatório de entregas do centro de distribuição de MG no mês de maio de 2025.<br>
Critérios:
  - Filtrar os pedidos do centro de distribuição de **Minas Gerais**.
  - Filtrar os pedidos do mês de **Maio de 2025**
  - Filtrar os pedidos com status diferente de **cancelado**
  - Juntar as tabelas pedidos, itens_pedidos, pagamentos e clientes
  - Selecionar apenas as colunas: 
    * Centro de distribuição
    * Produto
    * Método de pagamento
    * Identificador do pedido
    * Data do pedido
    * Identficador do cliente
    * Nome do cliente
  - Ordenar pelo **Centro de distribuição** e pelo **Identificador do pedido**
  - Exibir o resultado

**Atenção!** O cadastro de clientes possui chave composta: id_cliente e uf_cliente.


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("joins-chaves-compostas").getOrCreate()

################################
### Centro de Distribuição
################################
print("### Centro de Distribuição")
centro_distribuição_data = [
    ("SP", "Centro de Distribuição Cajamar/SP"),
    ("RJ", "Centro de Distribuição Duque de Caxias/RJ"),
    ("MG", "Centro de Distribuição Contagem/MG")
]
centro_distribuição_columns = ["id_centro_dist", "ds_centro_dist"]
centro_distribuição_df = spark.createDataFrame(centro_distribuição_data, centro_distribuição_columns)
centro_distribuição_df.show(truncate=False)

################################
### Produtos
################################
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

################################
### Clientes
################################
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
    (5, "MG", "2023-10-01", "Patricia"),
    (6, "MG", "2024-11-17", "Gustavo"),
]
clientes_columns = ["id_cliente", "uf_cliente", "dt_cadastro", "nome"]
clientes_df = spark.createDataFrame(clientes_data, clientes_columns)
clientes_df.printSchema()
clientes_df.show(truncate=False)

################################
### Pedidos
################################
print("### Pedidos")
pedidos_data = [
    (101, 1, "SP", "2025-01-01", "concluido"),
    (102, 2, "RJ", "2025-02-03", "cancelado"),
    (103, 3, "MG", "2025-03-05", "ativo"),
    (104, 4, "SP", "2025-04-07", "ativo"),
    (105, 1, "SP", "2025-05-09", "concluido"),
    (106, 5, "MG", "2025-05-10", "ativo"),
    (107, 3, "RJ", "2025-05-11", "concluido"),
    (108, 6, "MG", "2025-05-12", "cancelado"),

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

################################
### Pagamentos
################################
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

################################
### Itens de pedidos
################################
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


#########################################
# Implemente a sua lógica a partir daqui
#########################################


```

---
## 5. Parabéns!
Parabéns por concluir o módulo! Agora você conhece os fundamentos de operações de junção no Apache Spark.

## 6. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

