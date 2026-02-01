# PySpark com SQL

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução

O módulo **Spark SQL** permite consultar dados estruturados dentro de programas Spark usando SQL padrão (ANSI) ou a API de DataFrame.

Para muitos Engenheiros de Dados, o SQL é a "língua franca". O Spark permite que você alterne fluidamente entre código Python (para lógica imperativa e I/O) e SQL (para lógica relacional e analítica), aproveitando o melhor dos dois mundos.

**Neste módulo, você aprenderá:**
* Como expor DataFrames como Tabelas Temporárias.
* Criar regras de negócio complexas com `CASE WHEN`.
* Organizar queries longas com `CTEs`.
* Usar Window Functions (Ranking e Lag) diretamente no SQL.
* Misturar Python e SQL na mesma pipeline.

---

## 2. Setup do Laboratório

Para usar SQL, primeiro precisamos carregar nossos dados brutos e "registrá-los" como tabelas (Views) na memória do Spark.

Utilizaremos três fontes de dados distintas:
1.  **Clientes:** Dados cadastrais (CSV).
2.  **Pedidos:** Transações de venda (CSV).
3.  **Pagamentos:** Dados financeiros/bancários (JSON).

### 2.1. Preparação

Faça o clone dos repositórios:

* Clientes
```sh
git clone https://github.com/infobarbosa/datasets-csv-clientes

```

```sh
zcat datasets-csv-clientes/clientes.csv.gz | head -n 5

```

Output esperado:
```
id;nome;data_nasc;cpf;email;cidade;uf
1;Calebe Pinto;1988-03-01;645.278.301-71;calebe.pinto@hotmail.com;Vila Velha;ES
2;Lorenzo Silveira;1958-06-04;167.259.048-58;lorenzo.silveira@hotmail.com;Picos;PI
3;Henry da Conceição;2003-11-13;685.402.197-94;henry.da.conceicao@live.com;Parnamirim;RN
4;João Pedro Duarte;1995-10-26;354.806.172-90;joao.pedro.duarte@hotmail.com;Maracanaú;CE
```

* Pedidos
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos

```

```sh
zcat datasets-csv-pedidos/data/pedidos/pedidos-2026-01.csv.gz | head -5

```

Output esperado:
```
ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE
f198e8f7-033d-414d-b032-20975e84edde;LIQUIDIFICADOR;300.0;1;2026-01-05T18:36:28;MG;8409
97969db5-9304-4b80-b19e-3a9d60ce6520;CELULAR;1000.0;3;2026-01-01T11:58:48;DF;934
f1db6c7e-0701-42fd-90b2-638b57cefe38;NOTEBOOK;1500.0;2;2026-01-17T15:28:57;MG;5872
3994d9fa-6609-4818-8efa-c3a570a6116a;GELADEIRA;2000.0;1;2026-01-27T13:37:31;MA;174
```

* Pagamentos
```sh
git clone https://github.com/infobarbosa/dataset-json-pagamentos

```

```sh
zcat dataset-json-pagamentos/data/pagamentos/pagamentos-2026-01.json.gz | head -5

```

Output esperado:
```
{"id_pedido": "f198e8f7-033d-414d-b032-20975e84edde", "forma_pagamento": "PIX", "valor_pagamento": 285.0, "status": true, "data_processamento": "2026-01-06T02:29:21.830930", "avaliacao_fraude": {"fraude": false, "score": 0.12}}
{"id_pedido": "97969db5-9304-4b80-b19e-3a9d60ce6520", "forma_pagamento": "PIX", "valor_pagamento": 2850.0, "status": true, "data_processamento": "2026-01-01T22:26:07.151965", "avaliacao_fraude": {"fraude": false, "score": 0.11}}
{"id_pedido": "f1db6c7e-0701-42fd-90b2-638b57cefe38", "forma_pagamento": "PIX", "valor_pagamento": 2850.0, "status": true, "data_processamento": "2026-01-17T15:48:54.507491", "avaliacao_fraude": {"fraude": false, "score": 0.83}}
{"id_pedido": "3994d9fa-6609-4818-8efa-c3a570a6116a", "forma_pagamento": "CARTAO_CREDITO", "valor_pagamento": 2000.0, "status": true, "data_processamento": "2026-01-27T20:50:41.884628", "avaliacao_fraude": {"fraude": false, "score": 0.56}}
{"id_pedido": "04065285-5a0b-4631-af25-ea318f389b83", "forma_pagamento": "CARTAO_CREDITO", "valor_pagamento": 900.0, "status": true, "data_processamento": "2026-01-23T15:30:16.761626", "avaliacao_fraude": {"fraude": false, "score": 0.02}}
```

### 2.2. Script inicial

Crie um arquivo chamado `spark-sql-01.py`:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, FloatType, IntegerType, BooleanType, TimestampType, DoubleType

spark = SparkSession.builder \
    .appName("dataeng-spark-sql") \
    .getOrCreate()

# ---------------------------------------------------------
# 1. Carregando Clientes (CSV)
# ---------------------------------------------------------
# Definindo o schema
schema_cliente = StructType([
    StructField("ID", LongType(), True),
    StructField("NOME", StringType(), True),
    StructField("DATA_NASC", DateType(), True),
    StructField("CPF", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("CIDADE", StringType(), True),
    StructField("UF", StringType(), True)
])

# Criando o dataframe utilizando o schema definido acima
df_clientes = spark.read \
        .format("csv") \
        .option("compression", "gzip") \
        .option("sep", ";") \
        .option("header", True) \
        .load("./datasets-csv-clientes/clientes.csv.gz", schema=schema_cliente)

# Mostrando o schema
df_clientes.show(5, truncate=False)
df_clientes.printSchema()


# ---------------------------------------------------------
# 2. Carregando PEDIDOS (CSV particionado) e Criando valor_total
# ---------------------------------------------------------
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

df_pedidos = df_pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))

df_pedidos.show(5, truncate=False)
df_pedidos.printSchema()

# ---------------------------------------------------------
# 3. Carregando PAGAMENTOS (JSON)
# ---------------------------------------------------------
from pyspark.sql.types import BooleanType, TimestampType

schema_pagamentos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", FloatType(), True),
    StructField("status", BooleanType(), True),
    StructField("data_processamento", TimestampType(), True),
    StructField("avaliacao_fraude", StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score", DoubleType(), True)
    ]), True)
])

df_pagamentos = spark.read \
    .format("json") \
    .load("./dataset-json-pagamentos/data/pagamentos/", schema=schema_pagamentos)

df_pagamentos.show(5, truncate=False)
df_pagamentos.printSchema()

# ---------------------------------------------------------
# 4. REGISTRANDO AS TABELAS TEMPORÁRIAS (TempViews)
# ---------------------------------------------------------
# É aqui que a mágica acontece. Damos um "nome SQL" para o DataFrame.
df_clientes.createOrReplaceTempView("tb_clientes")
df_pedidos.createOrReplaceTempView("tb_pedidos")
df_pagamentos.createOrReplaceTempView("tb_pagamentos")

print("Tabelas registradas! Pronto para executar SQL.")


```

---

## 3. Consultas Básicas e Regras de Negócio (`CASE WHEN`)

Uma das maiores vantagens do SQL é a legibilidade para regras de negócio condicionais. Em vez de encadear múltiplos `.when().otherwise()` no Python, usamos `CASE WHEN`.

**Cenário:** Classificar os pedidos em categorias (Bronze, Prata, Ouro) baseadas no valor total.

```python
# Adicione ao seu script
print("--- Classificação de Pedidos (CASE WHEN) ---")

spark.sql("""
    SELECT 
        id_pedido,
        produto,
        valor_total,
        CASE 
            WHEN valor_total > 5000 THEN 'OURO'
            WHEN valor_total BETWEEN 2000 AND 5000 THEN 'PRATA'
            ELSE 'BRONZE'
        END as categoria_cliente
    FROM tb_pedidos
    WHERE uf = 'SP'
    LIMIT 10
""").show()

```

---

## 4. Organizando a Lógica com CTEs (Common Table Expressions)

Em Engenharia de Dados, queries "monstruosas" são comuns. As CTEs (cláusula `WITH`) permitem quebrar uma query complexa em blocos lógicos menores e reutilizáveis.

**Cenário:** Calcular o Ticket Médio por estado e listar apenas os estados que estão acima da média geral do Brasil.

```python
print("--- Estados com Ticket Médio acima da Média Nacional (CTE) ---")

query_cte = """
WITH cte_vendas_por_estado AS (
    -- Bloco 1: Calcula métricas por estado (Granularidade: UF)
    SELECT 
        uf,
        SUM(valor_total) as receita_estado,
        COUNT(id_pedido) as qtd_pedidos,
        (SUM(valor_total) / COUNT(id_pedido)) as ticket_medio_uf
    FROM tb_pedidos
    GROUP BY uf
),
cte_metricas_nacionais AS (
    -- Bloco 2: Calcula a média global (Granularidade: País)
    -- Note que aqui não fazemos GROUP BY, gerando uma única linha com a média do Brasil
    SELECT 
        AVG(ticket_medio_uf) as ticket_medio_brasil
    FROM cte_vendas_por_estado
)
-- Bloco Final: Cruza os estados com a média nacional
SELECT 
    e.uf,
    ROUND(e.ticket_medio_uf, 2) as ticket_medio_uf,
    ROUND(n.ticket_medio_brasil, 2) as media_nacional,
    ROUND(e.ticket_medio_uf - n.ticket_medio_brasil, 2) as diferenca
FROM cte_vendas_por_estado e
CROSS JOIN cte_metricas_nacionais n -- Une a linha única da média com todos os estados
WHERE e.ticket_medio_uf > n.ticket_medio_brasil
ORDER BY diferenca DESC
"""

spark.sql(query_cte).show()

```

### Padrões de Nomenclatura para CTEs
Ao escrever queries complexas com múltiplas CTEs, siga estas diretrizes para manter a legibilidade:
- Use snake case ou pascal case para nomes de CTEs.
    * Ex: `CalculoImposto` (menos verboso e redundante) ou `cte_calculo_imposto` (na opinião deste professor, mas legível embora redundante).

- Seja Descritivo: O nome deve explicar o que o conjunto de dados contém.
    * Ex: Se a CTE filtra clientes inativos, chame-a de `ClientesAtivos` ou `cte_clientes_ativos` em vez de `Filtro1` ou `cte_filtro_1`.

- Fluxo Lógico: Se uma CTE depende da anterior, tente manter nomes que contem uma história 
    * ex: `PedidosBrutos` -> `PedidosFiltrados` -> `MetricasFinais`.

---

---
## 5. Rollup e Cube
```python
print("### Rollup ###")
query_rollup = """
    SELECT 
        YEAR(data_criacao) as ano,
        SUM(valor_total) as receita,
        COUNT(id_pedido) as qtd_pedidos
    FROM tb_pedidos
    WHERE uf = 'RJ'
    AND produto = 'TABLET'
    GROUP BY ROLLUP(ano)
    ORDER BY ano DESC
"""

df_rollup = spark.sql(query_rollup)
df_rollup.printSchema()
df_rollup.show(10, truncate=False)

```

```python

print("### Cube ###")
query_cube = """
    SELECT 
        uf,
        YEAR(data_criacao) as ano,
        SUM(valor_total) as receita,
        COUNT(id_pedido) as qtd_pedidos
    FROM tb_pedidos
    WHERE uf = 'RJ'
    AND produto = 'TABLET'
    GROUP BY CUBE(uf, ano)
    ORDER BY uf, ano DESC
"""

df_cube = spark.sql(query_cube)
df_cube.printSchema()
df_cube.show(10, truncate=False)

```

---

## 6. Window Functions no SQL

No módulo anterior, usamos a classe `Window` no Python. No SQL, a sintaxe é padrão ANSI (`OVER PARTITION BY`), o que facilita muito para quem vem de bancos como Oracle, SQL Server ou PostgreSQL.

**Cenário:** Identificar os 2 pedidos mais caros de cada cliente (Ranking) e calcular a diferença de valor para o pedido anterior (Lag).

```python
print("--- Ranking e Lag via SQL ---")

spark.sql("""
    SELECT 
        id_cliente,
        data_criacao,
        valor_total,
        -- Ranking: O nº 1 é o pedido mais caro do cliente
        RANK() OVER (PARTITION BY id_cliente ORDER BY valor_total DESC) as rank_valor,
        
        -- Lag: Valor do pedido ANTERIOR (cronologicamente) desse mesmo cliente
        LAG(valor_total) OVER (PARTITION BY id_cliente ORDER BY data_criacao) as valor_pedido_anterior,
        
        -- Diferença: Quanto aumentou/diminuiu em relação à compra passada
        valor_total - LAG(valor_total) OVER (PARTITION BY id_cliente ORDER BY data_criacao) as diferenca
    FROM tb_pedidos
    ORDER BY id_cliente, data_criacao
""").show(10)

```

---

## 7. Abordagem Híbrida (Best of Both Worlds)

Você pode usar SQL para filtrar e juntar dados (onde o SQL é mais legível) e Python para transformações complexas ou I/O (onde o Python é melhor).

**Exemplo:** Usar SQL para juntar tabelas e Python para aplicar uma máscara de segurança (hash) no email.

```python
from pyspark.sql.functions import sha2

# 1. SQL para o Join (Mais legível que df.join(df, cond, how)...)
df_join = spark.sql("""
    SELECT 
        c.nome,
        c.email,
        p.produto,
        p.valor_total
    FROM tb_clientes c
    INNER JOIN tb_pedidos p ON c.id = p.id_cliente
    WHERE p.uf = 'RJ'
""")

# 2. Python para transformação de segurança
df_seguro = df_join.withColumn("email_hash", sha2("email", 256)).drop("email")

print("--- DataFrame Híbrido (SQL + Python) ---")
df_seguro.show(5, truncate=False)

```

---

## 8. Boas Práticas e Pitfalls

1. **TempView vs GlobalTempView:**
    * `createOrReplaceTempView`: A tabela só existe nesta sessão do Spark. Se o script terminar, a tabela "some".
    * `createGlobalTempView`: A tabela é compartilhada entre sessões na mesma aplicação Spark (útil em Notebooks compartilhados). Para acessar, use `SELECT * FROM global_temp.minha_tabela`.

    * Exemplo:
    ```python

    from pyspark.sql import SparkSession

    # 1. Criar a primeira SparkSession (Representando o Processo A - QG do Chapolin)
    spark1 = SparkSession.builder.appName("dataeng-spark-sql").getOrCreate()

    # Dados baseados na nossa simulação de maldade
    # Note que os valores numéricos são passados sem aspas para serem inferidos como Long/Integer
    data = [
        ("Alma Negra", "Ramón Valdés", 98),
        ("Poucas Trancas", "Rubén Aguirre", 92),
        ("Racha Cuca", "Ramón Valdés", 85),
        ("Tripa Seca", "Ramón Valdés", 78),
        ("Bruxa Baratuxa", "Maria Antonieta de las Nieves", 75),
        ("Cuajinais", "Carlos Villagrán", 70),
        ("Shory (Nenê)", "Rubén Aguirre", 65),
        ("Matadouro", "Rubén Aguirre", 60),
        ("Rosa a Rumorosa", "Florinda Meza", 40)
    ]

    # Criando o DataFrame com a coluna atualizada
    df_viloes = spark1.createDataFrame(data, ["vilao", "ator", "coeficiente_de_maldade"])

    # 2. Criar uma View LOCAL e uma View GLOBAL
    # A view local só "vive" dentro da spark1
    df_viloes.createOrReplaceTempView("viloes_locais")

    # A view global fica no catálogo global_temp, visível para outras sessões
    df_viloes.createOrReplaceGlobalTempView("viloes_globais_compartilhados")

    print("--- No Processo A (Sessão 1) ---")
    print("Acessando View Local:")
    spark1.sql("SELECT * FROM viloes_locais WHERE coeficiente_de_maldade > 80").show()

    # 3. Criar a segunda SparkSession (Representando o Processo B - Vilões em outro esconderijo)
    # O método .newSession() cria um ambiente isolado mas compartilha o mesmo SparkContext
    spark2 = spark1.newSession()

    print("--- No Processo B (Sessão 2) ---")
    try:
        # Isso vai FALHAR porque a view local 'viloes_locais' pertence apenas à spark1
        spark2.sql("SELECT * FROM viloes_locais").show()
    except Exception as e:
        print("ERRO ESPERADO: O Processo B não tem astúcia suficiente para enxergar a view local da Session A.")

    # Isso vai FUNCIONAR porque a global_temp atravessa as sessões dentro da mesma aplicação
    print("\nSUCESSO: Processo B acessando os Vilões via View Global:")
    spark2.sql("""
        SELECT vilao, coeficiente_de_maldade 
        FROM global_temp.viloes_globais_compartilhados 
        ORDER BY coeficiente_de_maldade DESC
    """).show()


    ```

2. **SQL Injection:**
    * **Nunca** faça isso: `spark.sql(f"SELECT * FROM table WHERE id = {user_input}")`.
    * Embora menos crítico em pipelines batch do que em web apps, é uma prática ruim.


3. **Performance (Catalyst Optimizer):**
    * *"O SQL é mais lento que o PySpark?"*
    * **Não!** O Spark converte tanto o SQL quanto o código DataFrame Python para o mesmo Plano de Execução Lógico e Físico (via Catalyst Optimizer). Use o que for mais legível para sua equipe.

---

## 9. Desafio: Relatório de Inadimplência e VIPs

Você precisa entregar um relatório financeiro cruzando as três bases de dados.

**Objetivo:**
Gerar uma lista de clientes que possuem pedidos realizados, mas cujo pagamento consta como **"Fraude"** ou não foi encontrado, e ao mesmo tempo listar os clientes **VIPs** (pagamentos confirmados acima de um certo valor).

**Requisitos do Desafio:**

1. Criar uma Query SQL que faça o JOIN entre `tb_clientes`, `tb_pedidos` e `tb_pagamentos`.
2. Utilizar `CASE WHEN` para criar uma coluna `status_final`:
* Se o pagamento for confirmado (`status = true`), marcar como **"Venda Confirmada"**.
* Se o pagamento for fraude (`fraude = true`), marcar como **"Fraude Detectada"**.
* Se não houver registro de pagamento (NULL no join), marcar como **"Pagamento Pendente"**.


3. Utilizar uma Window Function (`SUM OVER`) para calcular o `total_gasto_cliente` (considerando apenas vendas confirmadas).
4. Exibir: Nome do Cliente, UF, Produto, Status Final e Total Gasto pelo Cliente.

**Dica:** Você vai precisar de `LEFT JOIN` para encontrar os pagamentos pendentes.

**Template de Solução (`desafio_sql.py`):**

```python
# ... (Setup inicial igual ao script lab_sql_intro.py)

print("--- Relatório Financeiro (Desafio) ---")

query_desafio = """
    -- Escreva sua Query Aqui
    SELECT ...
"""

spark.sql(query_desafio).show(20, truncate=False)

```

---

## 10. Parabéns!
Você agora domina a integração PySpark + SQL! Essa habilidade é essencial para migrar cargas de trabalho de Data Warehouses tradicionais para o Spark e para colaborar com analistas de dados que preferem SQL.

---

## 11. Destruição de recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.
