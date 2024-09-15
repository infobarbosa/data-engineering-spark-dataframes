# Módulo 3: Operações de Junção e Agregação

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Neste módulo, vamos aprofundar nosso conhecimento em operações de junção e agregação no Apache Spark, explorando tipos de joins, técnicas de agregação, e o uso de funções analíticas para cálculos mais sofisticados.

## 2. Tipos de Join
### 2.1. Shuffle Join
O Shuffle Join é a junção padrão do Spark. Neste caso as tabelas são redistribuídas (shuffle) entre os nós para executar o join.

**Exemplo:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-shuffle-join").getOrCreate()

# Exemplo de DataFrames para join
df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "valor"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y"), (4, "Z")], ["id", "desc"])

# Shuffle join (join padrão, equivalente a inner join)
df_shuffle_join = df1.join(df2, "id")
print("Inner Join (shuffle join):")
df_shuffle_join.show()

# Left Join (ou Left Outer Join) - Mantém todos os registros do DataFrame da esquerda (df1) e adiciona os registros correspondentes do DataFrame da direita (df2)
df_left_join = df1.join(df2, "id", "left")
print("Left Join:")
df_left_join.show()

# Right Join (ou Right Outer Join) - Mantém todos os registros do DataFrame da direita (df2) e adiciona os registros correspondentes do DataFrame da esquerda (df1)
df_right_join = df1.join(df2, "id", "right")
print("Right Join:")
df_right_join.show()

# Full Join (ou Full Outer Join) - Mantém todos os registros de ambos os DataFrames (df1 e df2), preenchendo com nulls onde não há correspondência
df_full_join = df1.join(df2, "id", "full")
print("Full Join:")
df_full_join.show()

# Cross Join (ou Cartesian Join) - Faz o produto cartesiano entre os DataFrames, ou seja, combina cada linha de df1 com cada linha de df2
df_cross_join = df1.crossJoin(df2)
print("Cross Join:")
df_cross_join.show()

```

### 2.2. Broadcast Join
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

## 3. Técnicas de Agregação
### 3.1. groupBy
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

### 3.2. Window Functions
As Window Functions permitem a execução de cálculos complexos que envolvem particionamento e ordenação de dados.

**Exemplo de código:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Definindo a janela de dados
window_spec = Window.partitionBy("valor").orderBy("id")

# Aplicando uma função de janela
df_window = df1.withColumn("row_number", row_number().over(window_spec))
df_window.show()
```

## 4. Exploração de Funções Analíticas e Agregações Complexas
As funções analíticas permitem a execução de cálculos que envolvem operações mais sofisticadas, como rank, dense_rank, lead e lag.

**Exemplo de código:**
```python
from pyspark.sql.functions import rank, dense_rank, lag

# Funções analíticas: rank, dense_rank, lag
df_analytic = df1.withColumn("rank", rank().over(window_spec))
df_analytic = df_analytic.withColumn("dense_rank", dense_rank().over(window_spec))
df_analytic = df_analytic.withColumn("lag", lag("id", 1).over(window_spec))
df_analytic.show()
```

## 5. Exercício Prático Avançado
**Objetivo:** Implementar operações de junção e agregação utilizando técnicas avançadas de joins, funções de janela e agregações complexas.

**Instruções:**
1. Clone o repositório do curso:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-3.git
   ```
2. Navegue até a pasta do módulo 3:
   ```bash
   cd dataeng-modulo-3
   ```
3. Execute o script `modulo3.py`, que realizará as seguintes etapas:
   - Implementação de Broadcast Join e Shuffle Join.
   - Aplicação de funções de janela (window functions).
   - Uso de funções analíticas como rank, dense_rank, e lag.

**Código do laboratório:**
```python
# Exemplo de script modulo3.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, row_number, rank, dense_rank, lag
from pyspark.sql.window import Window

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-3").getOrCreate()

# Criando DataFrames de exemplo
df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "valor"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "desc"])

# Broadcast join
df_broadcast_join = df1.join(broadcast(df2), "id")
df_broadcast_join.show()

# Shuffle join
df_shuffle_join = df1.join(df2, "id")
df_shuffle_join.show()

# GroupBy com agregação
df_grouped = df1.groupBy("valor").count()
df_grouped.show()

# Definindo uma janela de dados
window_spec = Window.partitionBy("valor").orderBy("id")

# Aplicando funções de janela
df_window = df1.withColumn("row_number", row_number().over(window_spec))
df_window = df_window.withColumn("rank", rank().over(window_spec))
df_window = df_window.withColumn("dense_rank", dense_rank().over(window_spec))
df_window = df_window.withColumn("lag", lag("id", 1).over(window_spec))
df_window.show()

# Encerrando a SparkSession
spark.stop()
```

### 6. Parabéns!
Parabéns por concluir o módulo 3! Agora você domina operações de junção e agregação avançadas no Apache Spark, incluindo o uso de funções analíticas e agregações complexas.

### 7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-3/
│
├── README.md
├── modulo3.py
```

Este é o conteúdo do módulo 3. Podemos seguir para o próximo módulo quando você estiver pronto!