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

---

## 2. Funções de Janela

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

### 2.1. row_number(): Deduplicação e Recência

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

### 2.2. rank() vs dense_rank(): Ranqueamento

**Cenário de Negócio:** "Quais são os 3 pedidos de maior valor em cada Estado (UF)? Se houver empate no valor, como lidamos com a classificação?"

**Conceito:** Diferença no tratamento de empates.

* `rank`: Pula posições (1, 2, 2, 4).
* `dense_rank`: Não pula posições (1, 2, 2, 3).

```python
print("### 2. rank() vs dense_rank(): Ranqueamento ###")
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

### 2.3. lag() e lead(): Análise Temporal (Time Travel)

**Cenário de Negócio:** "Qual é a frequência de compra dos nossos clientes? Quantos dias se passam, em média, entre um pedido e o próximo?"

**Conceito:** Acessar linhas anteriores (`lag`) ou posteriores (`lead`) sem fazer joins complexos.

```python
# 1. Definindo a janela: Particionar por Cliente, Ordenar Cronologicamente
w_cliente_cronologico = Window.partitionBy("id_cliente").orderBy("data_criacao")

# 2. Buscando a data do pedido ANTERIOR
df_frequencia = df_pedidos.withColumn("data_anterior", F.lag("data_criacao").over(w_cliente_cronologico))

# 3. Calculando a diferença em dias
df_frequencia = df_frequencia.withColumn(
    "dias_desde_ultimo_pedido", 
    F.datediff(F.col("data_criacao"), F.col("data_anterior"))
)

print("--- Análise de Recorrência ---")
df_frequencia \
    .filter(F.col("data_anterior").isNotNull()) \
    .select("id_cliente", "data_criacao", "data_anterior", "dias_desde_ultimo_pedido") \
    .orderBy("id_cliente", "data_criacao") \
    .show(5)

```

---

### 2.4. Agregações de Janela (Running Total / Média Móvel)

**Cenário de Negócio:** "Queremos ver a evolução do gasto acumulado (Lifetime Value) de cada cliente ao longo do tempo."

**Conceito:** Diferente do `groupBy` que colapsa as linhas, aqui mantemos as linhas e acumulamos o valor linha a linha.

*Nota didática:* Aqui introduzimos o conceito de `rowsBetween`, fundamental para definir o escopo da agregação (do início até a linha atual).

```python
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

## 3. Desafio

Faça o clone do repositório abaixo:
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos
```

### Leiaute dos arquivos

- **Separador**: ";"
- **Header**: True
- **Compressão**: gzip

##### Atributos 
| Atributo        | Tipo      | Obs                                               | 
| ---             | ---       | ---                                               |
| ID_PEDIDO       | UUID      | O identificador da pessoa                         | 
| PRODUTO         | string    | O nome do produto no pedido                       | 
| VALOR_UNITARIO  | float     | O valor unitário do produto no pedido             | 
| QUANTIDADE      | long      | A quantidade do produto no pedido                 | 
| DATA_CRIACAO    | date      | A data da criação do pedido                       | 
| UF              | string    | A sigla da unidade federativa (estado) no Brasil  | 
| ID_CLIENTE      | long      | O identificador do cliente                        | 

##### Sample
id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente

fdd7933e-ce3a-4475-b29d-f239f491a0e7;MONITOR;600;3;2024-01-01T22:26:32;RO;12414<br>
fe0f547a-69f3-4514-adee-8f4299f152af;MONITOR;600;2;2024-01-01T16:01:26;SP;11750<br>
fe4f2b05-1150-43d8-b86a-606bd55bc72c;NOTEBOOK;1500;1;2024-01-01T06:49:21;RR;1195<br>
fe8f5b08-160b-490b-bcb3-c86df6d2e53b;GELADEIRA;2000;1;2024-01-01T04:14:54;AC;8433<br>
feaf3652-e1bd-4150-957e-ee6c3f62e11e;HOMETHEATER;500;2;2024-01-01T10:33:09;SP;12231<br>
feb1efc5-9dd7-49a5-a9c7-626c2de3e029;CELULAR;1000;2;2024-01-01T13:48:39;SC;2340<br>
ff181456-d587-4abd-a0ac-a8e6e33b87d5;TABLET;1100;1;2024-01-01T21:28:47;RS;12121<br>
ff3bc5e0-c49a-46c5-a874-3eb6c8289fd1;HOMETHEATER;500;1;2024-01-01T22:31:35;SC;6907<br>
ff4fcf5f-ca8a-4bc4-8d17-995ecaab3110;SOUNDBAR;900;3;2024-01-01T19:33:08;RJ;9773<br>
ff703483-e564-4883-bdb5-0e25d8d9a006;NOTEBOOK;1500;3;2024-01-01T00:22:32;RN;2044<br>
ffe4d6ad-3830-45af-a599-d09daaeb5f75;HOMETHEATER;500;3;2024-01-01T02:55:59;MS;3846<br>


Considerando os datasets do repositório clonado acima, elabore os scripts dos seguintes relatórios:
1. Relatório de ranking de vendas de PRODUTOS por QUANTIDADE.
    - Quais os produtos que mais vendem em quantidade?
    - Utilize `rank()` e `dense_rank()`
2. Relatório de ranking de vendas de PRODUTOS por VALOR TOTAL VENDIDO.
    - Quais os produtos que mais vendem em valor total vendido?
    - Utilize `rank()` e `dense_rank()`
3. Relatório de ranking de vendas por UF (estado) e VALOR TOTAL VENDIDO.
    - Quais os estados que com maior valor total faturado?
    - Utilize `rank()` e `dense_rank()`
4. Relatório de ranking de vendas por DIA DA SEMANA (Segunda, Terça, etc) e VALOR TOTAL VENDIDO.
    - Quais os dias da semana com maior valor total faturado?
    - Utilize `rank()` e `dense_rank()`

A seguir está um script inicial
```python
from pyspark.sql import SparkSession

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-desafio-window-functions") \
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

# DESENVOLVA SUA LOGICA AQUI

```

---
## 7. Parabéns!
Você concluiu com sucesso o módulo sobre funções de janela no Spark! Esperamos que você tenha aprendido como utilizar as funções `row_number`, `rank`, `dense_rank` e `lag` para realizar análises avançadas em seus dados. Continue praticando e explorando novas possibilidades com o Spark para aprimorar suas habilidades em engenharia de dados. Bom trabalho e até a próxima!

---

## 8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

