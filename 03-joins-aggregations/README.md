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
---
## 5. Um exemplo completo

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("dataeng-window-functions") \
    .getOrCreate()

# Criando o dataframe de salarios por departamento
dados = [
    ("João", "Vendas", 5000),
    ("Maria", "Vendas", 6000),
    ("Pedro", "Vendas", 7000),
    ("Lucas", "Vendas", 7000),
    ("Ana", "RH", 3000),
    ("Paula", "RH", 4000),
    ("Carlos", "RH", 4000),
    ("Fernanda", "TI", 8000),
    ("Ricardo", "TI", 9000),
    ("Bianca", "TI", 9000),
    ("Marcos", "TI", 8500),
    ("Sofia", "Marketing", 5000),
    ("Bruno", "Marketing", 5500),
    ("Letícia", "Marketing", 5000)
]

colunas = ["nome", "departamento", "salario"]

df = spark.createDataFrame(dados, colunas)

# Definindo a janela de partição por departamento e ordenação por salário descendente
janela_departamento = Window.partitionBy("departamento").orderBy(col("salario").desc())

# Aplicando funções de janela
df_com_funcoes_janela = df \
    .withColumn("numero_linha", row_number().over(janela_departamento)) \
    .withColumn("rank", rank().over(janela_departamento)) \
    .withColumn("dense_rank", dense_rank().over(janela_departamento))

# Exibindo o resultado
df_com_funcoes_janela.show()

# Parando a sessão Spark
spark.stop()
```

### Explicação das funções de janela utilizadas

1. **row_number()**: Atribui um número de linha único e sequencial a cada registro dentro da partição, baseado na ordenação especificada. Não considera empates; cada linha recebe um número distinto.

2. **rank()**: Atribui uma classificação aos registros dentro da partição, permitindo empates. Se dois registros tiverem o mesmo valor na ordenação, eles receberão o mesmo rank, e o próximo rank será pulado. Por exemplo, se dois registros estão em primeiro lugar, o próximo será o terceiro.

3. **dense_rank()**: Semelhante ao `rank()`, mas não pula ranks após empates. Se dois registros compartilham o primeiro lugar, o próximo receberá o rank dois.

### Objetivo do dataset mais complexo

Ao introduzir salários iguais dentro dos departamentos, podemos observar como cada função lida com empates:

- **Vendas**: Dois funcionários com salário de 7000.
- **RH**: Dois funcionários com salário de 4000.
- **TI**: Dois funcionários com salário de 9000.
- **Marketing**: Dois funcionários com salário de 5000.

Isso permite visualizar claramente as diferenças entre `row_number()`, `rank()` e `dense_rank()`.

### Resultado esperado

Ao executar o código, o output será semelhante a:

```
+--------+------------+-------+------------+----+----------+
|    nome|departamento|salario|numero_linha|rank|dense_rank|
+--------+------------+-------+------------+----+----------+
|   Pedro|      Vendas|   7000|           1|   1|         1|
|   Lucas|      Vendas|   7000|           2|   1|         1|
|   Maria|      Vendas|   6000|           3|   3|         2|
|    João|      Vendas|   5000|           4|   4|         3|
| Ricardo|          TI|   9000|           1|   1|         1|
|  Bianca|          TI|   9000|           2|   1|         1|
|  Marcos|          TI|   8500|           3|   3|         2|
|Fernanda|          TI|   8000|           4|   4|         3|
|   Bruno|   Marketing|   5500|           1|   1|         1|
|   Sofia|   Marketing|   5000|           2|   2|         2|
| Letícia|   Marketing|   5000|           3|   2|         2|
|   Paula|          RH|   4000|           1|   1|         1|
|  Carlos|          RH|   4000|           2|   1|         1|
|     Ana|          RH|   3000|           3|   3|         2|
+--------+------------+-------+------------+----+----------+
```

Observe como:

- `row_number()` incrementa sequencialmente, sem considerar empates.
- `rank()` atribui o mesmo rank para empates e pula ranks subsequentes.
- `dense_rank()` atribui o mesmo rank para empates, mas não pula ranks.

### Conclusão

Este exemplo demonstra claramente como as diferentes funções de janela se comportam em relação à ordenação e empates dentro de partições, tornando mais fácil entender e escolher a função adequada para suas necessidades analíticas.

---

## 6. `lag`

A função `lag` é uma função de janela que permite acessar o valor de uma coluna em uma linha anterior, baseada na ordenação definida na janela. 

A sintaxe básica é:

```python
lag(coluna, deslocamento, valor_padrão)
```

- **coluna**: A coluna da qual queremos obter o valor.
- **deslocamento**: O número de linhas para olhar para trás (por padrão é 1).
- **valor_padrão**: O valor a ser usado se não houver linha anterior (por exemplo, na primeira linha).

### Exemplo:

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("dataeng-lag-function") \
    .getOrCreate()

# Criando um DataFrame com dados mais complexos
dados = [
    ("2021-01-01", "Produto A", 100),
    ("2021-01-02", "Produto A", 150),
    ("2021-01-03", "Produto A", 200),
    ("2021-01-04", "Produto A", 250),
    ("2021-01-01", "Produto B", 80),
    ("2021-01-02", "Produto B", 120),
    ("2021-01-03", "Produto B", 160),
    ("2021-01-04", "Produto B", 200),
    ("2021-01-01", "Produto C", 50),
    ("2021-01-02", "Produto C", 70),
    ("2021-01-03", "Produto C", 90),
    ("2021-01-04", "Produto C", 110)
]

colunas = ["data", "produto", "vendas"]

df = spark.createDataFrame(dados, colunas)

# Convertendo a coluna 'data' para o tipo Date
from pyspark.sql.functions import to_date
df = df.withColumn("data", to_date(col("data"), "yyyy-MM-dd"))

# Definindo a janela para a função lag
janela = Window.partitionBy("produto").orderBy("data")

# Aplicando a função lag para obter as vendas do dia anterior
df_com_lag = df.withColumn("vendas_dia_anterior", lag("vendas", 1).over(janela))

# Calculando a diferença das vendas em relação ao dia anterior
df_com_lag = df_com_lag.withColumn("dif_vendas", col("vendas") - col("vendas_dia_anterior"))

# Exibindo o resultado
df_com_lag.orderBy("produto", "data").show()

# Parando a sessão Spark
spark.stop()
```

### Explicando o código

O objetivo deste exemplo é mostrar como a função `lag` pode ser usada para acessar valores de linhas anteriores dentro de uma janela especificada. Neste caso, queremos comparar as vendas diárias de cada produto com as vendas do dia anterior.

#### Descrição dos Dados

Temos um DataFrame que contém as vendas diárias de três produtos (`Produto A`, `Produto B` e `Produto C`) ao longo de quatro dias.

#### Passos Principais

1. **Conversão da Coluna de Data**: Utilizamos a função `to_date` para converter a coluna `data` de string para o tipo de data apropriado.

2. **Definição da Janela**: Criamos uma janela (`janela`) que partitiona os dados por `produto` e ordena por `data` ascendente.

3. **Aplicação da Função `lag`**:
   - Usamos `lag("vendas", 1).over(janela)` para obter o valor das vendas na linha anterior dentro da mesma partição (mesmo produto).
   - O número `1` indica que queremos olhar uma linha atrás. Podemos ajustar esse número para obter valores de linhas mais distantes.

4. **Cálculo da Diferença de Vendas**:
   - Calculamos a diferença entre as vendas atuais e as vendas do dia anterior usando `col("vendas") - col("vendas_dia_anterior")`.
   - Isso nos dá a variação diária das vendas para cada produto.

#### Resultado Esperado

Ao executar o código, o output será semelhante a:

```
+----------+---------+------+-------------------+----------+
|      data|  produto|vendas|vendas_dia_anterior|dif_vendas|
+----------+---------+------+-------------------+----------+
|2021-01-01|Produto A|   100|               null|      null|
|2021-01-02|Produto A|   150|                100|        50|
|2021-01-03|Produto A|   200|                150|        50|
|2021-01-04|Produto A|   250|                200|        50|
|2021-01-01|Produto B|    80|               null|      null|
|2021-01-02|Produto B|   120|                 80|        40|
|2021-01-03|Produto B|   160|                120|        40|
|2021-01-04|Produto B|   200|                160|        40|
|2021-01-01|Produto C|    50|               null|      null|
|2021-01-02|Produto C|    70|                 50|        20|
|2021-01-03|Produto C|    90|                 70|        20|
|2021-01-04|Produto C|   110|                 90|        20|
+----------+---------+------+-------------------+----------+
```

Observe que para cada produto:

- No primeiro dia (`2021-01-01`), não há vendas do dia anterior, portanto `vendas_dia_anterior` é `null` e `dif_vendas` também é `null`.

- Nos dias subsequentes, `vendas_dia_anterior` mostra as vendas do dia anterior, e `dif_vendas` mostra a diferença em relação ao dia anterior.

No nosso exemplo, usamos `lag("vendas", 1).over(janela)` para obter o valor das vendas da linha anterior dentro da mesma partição de `produto`.

#### Conclusão

Este exemplo ilustra como a função `lag` pode ser usada em conjunto com funções de janela para comparar valores entre linhas em um DataFrame ordenado. Essa funcionalidade é particularmente útil para calcular diferenças, taxas de crescimento ou qualquer análise que dependa de valores sequenciais.

---
## 7. Parabéns!
Parabéns por concluir o módulo! Agora você domina operações de junção e agregação avançadas no Apache Spark, incluindo o uso de funções analíticas e agregações complexas.

## 8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

