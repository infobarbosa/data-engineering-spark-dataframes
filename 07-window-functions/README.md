# Manipulação Avançada de DataFrame - Funções de Janela

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução

As **Window Functions** (Funções de Janela) permitem realizar cálculos avançados em um conjunto de linhas relacionadas à linha atual.

A principal diferença entre uma Window Function e um `groupBy` tradicional é a **preservação da granularidade**:

* **groupBy**: Agrupa as linhas e retorna apenas uma linha por grupo (reduzindo o dataset).
* **Window**: Mantém todas as linhas originais e adiciona uma nova coluna com o resultado do cálculo (como um ranking, uma média móvel ou um valor anterior).

Para definir essa "janela" de atuação, utilizamos a classe `Window` com dois métodos principais:

1. **`partitionBy`**: Divide os dados em grupos lógicos (ex: por cliente ou departamento). O cálculo reinicia a cada novo grupo.
2. **`orderBy`**: Define a ordem lógica das linhas dentro da partição (ex: ordenar por data para criar um ranking cronológico).

### 1.1. Laboratório
- Faça o clone do repositório abaixo:
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos

```

- Crie um script `window01.py` e inclua o código abaixo:
```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("data-eng-window-functions") \
    .getOrCreate()

# Definição do Schema
schema = "id_pedido STRING, produto STRING, valor_unitario FLOAT, quantidade INT, data_criacao DATE, uf STRING, id_cliente LONG"

# Leitura dos dados
df_pedidos = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .option("sep", ";") \
    .csv("./datasets-csv-pedidos/data/pedidos/")

# Pré-processamento: Criando coluna de valor total para análises monetárias
df_pedidos = df_pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))

print("--- Dataset Base ---")
df_pedidos.show(5, truncate=False)

```

---

## 2. row_number(): Deduplicação e Recência

**Cenário de Negócio:** "Queremos identificar apenas a compra mais recente de cada cliente para enviar uma pesquisa de satisfação."

**Conceito:** `row_number` cria um índice sequencial exclusivo. É a ferramenta padrão para deduplicação.

```python

print("### row_number(): Deduplicação e Recência ###")
# 1. Definindo a janela: Particionar por Cliente, Ordenar pela data (Decrescente)
w_cliente_recente = Window.partitionBy("id_cliente").orderBy(F.col("data_criacao").desc())

# 2. Criando o índice
df_recente = df_pedidos.withColumn("rn", F.row_number().over(w_cliente_recente))

# 3. Filtrando apenas a última compra (rn = 1)
print("--- Última compra de cada cliente ---")
df_recente.filter(F.col("rn") == 1) \
    .select("id_cliente", "data_criacao", "produto", "valor_total") \
    .show(5)

```

---

## 3. rank() vs dense_rank(): Ranqueamento

**Cenário de Negócio:** "Quais são os 3 pedidos de maior valor em cada Estado (UF)? Se houver empate no valor, como lidamos com a classificação?"

**Conceito:** Diferença no tratamento de empates.

* `rank`: Pula posições (1, 2, 2, 4).
* `dense_rank`: Não pula posições (1, 2, 2, 3).

```python

print("### rank() vs dense_rank(): Ranqueamento ###")
# 1. Definindo a janela: Particionar por Estado (UF), Ordenar por Valor Total (Decrescente)
w_uf_valor = Window.partitionBy("uf").orderBy(F.col("valor_total").desc())

# 2. Aplicando as funções
df_ranking = df_pedidos \
    .filter(F.col("data_criacao").between("2025-12-01", "2025-12-02")) \
    .withColumn("rank", F.rank().over(w_uf_valor)) \
    .withColumn("dense_rank", F.dense_rank().over(w_uf_valor))

print("--- Top Vendas por Estado (Comparativo) ---")
df_ranking \
    .filter(F.col("dense_rank") <= 3) \
    .select("uf", "produto", "valor_total", "rank", "dense_rank") \
    .show(10)

df_ranking \
    .filter(F.col("dense_rank") <= 3) \
    .filter(F.col("uf") == "SP") \
    .select("uf", "produto", "valor_total", "rank", "dense_rank", "id_cliente") \
    .show(20)

```

---

## 4. lag() e lead(): Análise Temporal (Time Travel)

**Cenário de Negócio:** "Qual é a frequência de compra dos nossos clientes? Quantos dias se passam, em média, entre um pedido e o próximo?"

**Conceito:** Acessar linhas anteriores (`lag`) ou posteriores (`lead`) sem fazer joins complexos.

```python

print("### lag() e lead(): Análise Temporal (Time Travel) ###")

# 1. Definindo a janela: Particionar por Cliente, Ordenar Cronologicamente
w_cliente_cronologico = Window.partitionBy("id_cliente").orderBy("data_criacao")

# 2. Buscando a data do pedido ANTERIOR (lag) e do PRÓXIMO pedido (lead)
df_temporal = df_pedidos \
    .withColumn("data_anterior", F.lag("data_criacao").over(w_cliente_cronologico)) \
    .withColumn("data_proxima", F.lead("data_criacao").over(w_cliente_cronologico))

# 3. Calculando a diferença em dias (Recorrência passada e futura)
df_temporal = df_temporal.withColumn(
    "dias_desde_ultimo", 
    F.datediff(F.col("data_criacao"), F.col("data_anterior"))
).withColumn(
    "dias_ate_proximo", 
    F.datediff(F.col("data_proxima"), F.col("data_criacao"))
)

print("--- Análise Temporal ---")
print("Colunas de Negócio:")
print(" - dias_desde_ultimo: Indica a FREQUÊNCIA atual de compra.")
print(" - dias_ate_proximo: Indica o RISCO DE CHURN (se NULL, o cliente parou).")

df_temporal \
    .select("id_cliente", "data_criacao", "data_anterior", "dias_desde_ultimo", "data_proxima", "dias_ate_proximo") \
    .orderBy("id_cliente", "data_criacao") \
    .show(10)

```

---

## 5. Agregações de Janela (Running Total / Média Móvel)

**Cenário de Negócio:** "Queremos ver a evolução do gasto acumulado (Lifetime Value) de cada cliente ao longo do tempo."

**Conceito:** Diferente do `groupBy` que colapsa as linhas, aqui mantemos as linhas e acumulamos o valor linha a linha.

*Nota didática:* Aqui introduzimos o conceito de `rowsBetween`, fundamental para definir o escopo da agregação (do início até a linha atual).

```python

print("### Agregações de Janela (Running Total / Média Móvel) ###")
# 1. Definindo a janela com Frame: Do início (unboundedPreceding) até a linha atual (currentRow)
w_acumulado = Window.partitionBy("id_cliente") \
                    .orderBy("data_criacao") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# 2. Calculando a soma acumulada
df_ltv = df_pedidos.withColumn("gasto_acumulado", F.sum("valor_total").over(w_acumulado))

print("--- Evolução do Gasto do Cliente (Running Total) ---")
df_ltv \
    .select("id_cliente", "data_criacao", "valor_total", "gasto_acumulado") \
    .orderBy("id_cliente", "data_criacao") \
    .show(10)

```

---

## 6. Desafio

Faça o clone do repositório abaixo (caso ainda não tenha feito):
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos

```

### Contexto

Você é o Engenheiro de Dados responsável por gerar inteligência para o time de CRM e Vendas. Eles precisam de 3 relatórios estratégicos baseados no histórico de pedidos.

**Dica:** Lembre-se de criar a coluna `valor_total` (`valor_unitario` * `quantidade`) antes de começar.

---

### Questão 1: Ranking de Produtos por Estado (Ranking)

O time de vendas quer saber quais são os produtos "campeões" de venda em cada estado para repor o estoque.

* **Objetivo:** Liste os 3 produtos que mais geraram receita (valor total) em cada estado (UF).
* **Requisito:** Utilize `dense_rank()` para lidar com empates de valor.
* **Output esperado:** Colunas `uf`, `produto`, `receita_total`, `posicao`.

### Questão 2: Análise de Recorrência (Lag)

O time de CRM quer identificar o comportamento de compra dos clientes.

* **Objetivo:** Para cada pedido, calcule quantos dias se passaram desde o pedido **anterior** do mesmo cliente.
* **Requisito:** Utilize a função `lag()`.
* **Output esperado:** Colunas `id_cliente`, `data_criacao`, `data_anterior`, `dias_desde_ultimo`.

### Questão 3: Faturamento Acumulado por Estado (Running Total)

A diretoria financeira quer visualizar a evolução da receita ao longo do tempo, mas separada por estado.

* **Objetivo:** Calcule a soma acumulada (Running Total) do valor dos pedidos dia a dia, particionado por estado (UF).
* **Requisito:** Utilize `sum()` com `rowsBetween(Window.unboundedPreceding, Window.currentRow)`.
* **Output esperado:** Colunas `uf`, `data_criacao`, `valor_pedido`, `receita_acumulada_uf`.

---

### Script Inicial

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-window-functions") \
    .getOrCreate()

# Carregando o dataset
schema = "id_pedido STRING, produto STRING, valor_unitario FLOAT, quantidade INT, data_criacao DATE, uf STRING, id_cliente LONG"

df_pedidos = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("sep", ";") \
    .schema(schema) \
    .load("./datasets-csv-pedidos/data/pedidos/")

# Criando a coluna de valor total
df_pedidos = df_pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))

# -----------------------------
# DESENVOLVA SUA SOLUÇÃO ABAIXO
# -----------------------------

# 1. Ranking de Produtos por Estado
# ...

# 2. Análise de Recorrência
# ...

# 3. Faturamento Acumulado
# ...

```

---

### Soluções dos desafios
**Não desista!** Tente ao menos uma ou duas vezes antes de checar a solução. ;)

<details>
  <summary>Ranking de Produtos por Estado (Ranking)</summary>

```python
# ---------------------------------------------------------
# Solução 1: Ranking de Produtos por Estado (Com Agregação)
# ---------------------------------------------------------
print("--- Questão 1: Top 3 Produtos por Receita em cada Estado ---")

# Passo A: Calcular a receita total de cada produto em cada estado
df_receita_produto = df_pedidos \
    .groupBy("uf", "produto") \
    .agg(F.sum("valor_total").alias("receita_total"))

# Passo B: Criar a janela para ranquear os produtos dentro de cada estado
w_ranking = Window.partitionBy("uf").orderBy(F.col("receita_total").desc())

# Passo C: Ranquear e filtrar
df_top3 = df_receita_produto \
    .withColumn("posicao", F.dense_rank().over(w_ranking)) \
    .filter(F.col("posicao") <= 3) \
    .select("uf", "produto", "receita_total", "posicao") \
    .orderBy("uf", "posicao")

df_top3.show(10)

```
</details>

<details>
  <summary>Análise de Recorrência (Lag)</summary>

```python
# ---------------------------------------------------------
# Solução 2: Análise de Recorrência (Lag)
# ---------------------------------------------------------
print("--- Questão 2: Dias desde o último pedido (Recorrência) ---")

# Passo A: Janela por cliente ordenada cronologicamente
w_cliente = Window.partitionBy("id_cliente").orderBy("data_criacao")

# Passo B: Calcular Lag e Diferença
df_recorrencia = df_pedidos \
    .withColumn("data_anterior", F.lag("data_criacao").over(w_cliente)) \
    .withColumn("dias_desde_ultimo", F.datediff(F.col("data_criacao"), F.col("data_anterior"))) \
    .select("id_cliente", "data_criacao", "data_anterior", "dias_desde_ultimo") \
    .orderBy("id_cliente", "data_criacao")

# Mostrando apenas casos onde houve recompra (data_anterior não nula) para facilitar visualização
df_recorrencia.filter(F.col("data_anterior").isNotNull()).show(10)

```
</details>

<details>
  <summary>Faturamento Acumulado por Estado (Running Total)</summary>

```python
# ---------------------------------------------------------
# Solução 3: Faturamento Acumulado por Estado (Running Total)
# ---------------------------------------------------------
print("--- Questão 3: Evolução da Receita por Estado (Dia a Dia) ---")

# Passo A: Janela por Estado, ordenada por data, acumulando do início até a linha atual
w_acumulado = Window.partitionBy("uf") \
    .orderBy("data_criacao") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Passo B: Calcular Soma Acumulada
df_acumulado = df_pedidos \
    .withColumn("receita_acumulada_uf", F.sum("valor_total").over(w_acumulado)) \
    .select("uf", "data_criacao", "valor_total", "receita_acumulada_uf") \
    .orderBy("uf", "data_criacao")

df_acumulado.show(10)

```

</details

---

## 7. Parabéns!
Você concluiu com sucesso o módulo sobre funções de janela no Spark! Esperamos que você tenha aprendido como utilizar as funções `row_number`, `rank`, `dense_rank` e `lag` para realizar análises avançadas em seus dados. Continue praticando e explorando novas possibilidades com o Spark para aprimorar suas habilidades em engenharia de dados. Bom trabalho e até a próxima!

---

## 8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

