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

## 2. Setup do Laboratório (ETL Inicial)

Para usar SQL, primeiro precisamos carregar nossos dados brutos e "registrá-los" como tabelas (Views) na memória do Spark.

Utilizaremos três fontes de dados distintas:
1.  **Clientes:** Dados cadastrais (CSV).
2.  **Pedidos:** Transações de venda (CSV).
3.  **Pagamentos:** Dados financeiros/bancários (JSON).

### 2.1. Preparação

Faça o clone dos repositórios:

```sh
git clone https://github.com/infobarbosa/datasets-csv-clientes
git clone https://github.com/infobarbosa/datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos

```

### 2.2. Script de Carga

Crie um arquivo chamado `lab_sql_intro.py`:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("dataeng-spark-sql") \
    .getOrCreate()

# ---------------------------------------------------------
# 1. Carregando CLientES (CSV)
# ---------------------------------------------------------
# Schema: id;nome;data_nasc;cpf;email;cidade;uf
df_clientes = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("sep", ";") \
    .load("./datasets-csv-clientes/clientes.csv.gz")

# ---------------------------------------------------------
# 2. Carregando PEDIDOS (CSV particionado) e Criando valor_total
# ---------------------------------------------------------
df_pedidos = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("sep", ";") \
    .load("./datasets-csv-pedidos/data/pedidos/") \
    .withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))

# ---------------------------------------------------------
# 3. Carregando PAGAMENTOS (JSON)
# ---------------------------------------------------------
df_pagamentos = spark.read \
    .format("json") \
    .load("./dataset-json-pagamentos/pagamentos.json")

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
print("--- Estados acima da Média (CTE) ---")

query_cte = """
WITH VendasPorEstado AS (
    -- Bloco 1: Calcula totais por estado
    SELECT 
        uf,
        SUM(valor_total) as receita_estado,
        COUNT(id_pedido) as qtd_pedidos
    FROM tb_pedidos
    GROUP BY uf
),
Indicadores AS (
    -- Bloco 2: Calcula o ticket médio
    SELECT 
        uf,
        receita_estado,
        (receita_estado / qtd_pedidos) as ticket_medio
    FROM VendasPorEstado
)
-- Bloco Final: Filtra e Ordena
SELECT * FROM Indicadores
WHERE ticket_medio > 2500 -- Apenas tickets altos
ORDER BY ticket_medio DESC
"""

spark.sql(query_cte).show()

```

---

## 5. Window Functions no SQL (O Poder Analítico)

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

## 6. Abordagem Híbrida (Best of Both Worlds)

O "superpoder" do Spark é misturar as linguagens. Você pode usar SQL para filtrar e juntar dados (onde o SQL é mais legível) e Python para transformações complexas ou I/O (onde o Python é melhor).

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

## 7. Boas Práticas e Pitfalls

1. **TempView vs GlobalTempView:**
* `createOrReplaceTempView`: A tabela só existe nesta sessão do Spark. Se o script terminar, a tabela "some".
* `createGlobalTempView`: A tabela é compartilhada entre sessões na mesma aplicação Spark (útil em Notebooks compartilhados). Para acessar, use `SELECT * FROM global_temp.minha_tabela`.


2. **SQL Injection:**
* **Nunca** faça isso: `spark.sql(f"SELECT * FROM table WHERE id = {user_input}")`.
* Embora menos crítico em pipelines batch do que em web apps, é uma prática ruim.


3. **Performance (Catalyst Optimizer):**
* Muitos perguntam: *"O SQL é mais lento que o PySpark?"*
* **Resposta:** Não. O Spark converte tanto o SQL quanto o código DataFrame Python para o mesmo Plano de Execução Lógico e Físico (via Catalyst Optimizer). Use o que for mais legível para sua equipe.

---

## 8. Desafio: Relatório de Inadimplência e VIPs

Você precisa entregar um relatório financeiro cruzando as três bases de dados.

**Objetivo:**
Gerar uma lista de clientes que possuem pedidos realizados, mas cujo pagamento consta como **"Fraude"** ou não foi encontrado, e ao mesmo tempo listar os clientes **VIPs** (pagamentos confirmados acima de um certo valor).

**Requisitos do Desafio:**

1. Criar uma Query SQL que faça o JOIN entre `tb_clientes`, `tb_pedidos` e `tb_pagamentos`.
2. Utilizar `CASE WHEN` para criar uma coluna `status_final`:
* Se o pagamento for confirmado (`status = 'aprovado'`), marcar como **"Venda Confirmada"**.
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

## 9. Parabéns!

Você agora domina a integração PySpark + SQL! Essa habilidade é essencial para migrar cargas de trabalho de Data Warehouses tradicionais para o Spark e para colaborar com analistas de dados que preferem SQL.

**Próximos Passos:**
Não se esqueça de destruir seus recursos (cluster ou ambiente Cloud9) para evitar custos extras.

