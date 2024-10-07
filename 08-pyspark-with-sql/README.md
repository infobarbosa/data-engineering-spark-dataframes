
# PySpark com SQL

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

## 1. Introdução

Este módulo demonstra a integração da linguagem SQL com o PySpark, utilizando os datasets de clientes, pedidos e pagamentos como exemplos. O foco é apresentar os conceitos fundamentais dessa integração, e não oferecer um curso completo de SQL.

## 2. Visualizações temporárias

No PySpark, você pode criar visualizações temporárias a partir de DataFrames usando o método `createOrReplaceTempView`. Essas visualizações permitem que você execute consultas SQL diretamente nos dados carregados. A seguir, um exemplo de como criar uma visualização temporária para um dataset de clientes:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-joins").getOrCreate()

print("DataFrame de clientes")

# Definindo o schema dos dados de clientes
schema_clientes = StructType([
    StructField("id_cliente", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("uf", StringType(), True)
])

# Dados dos clientes
dados_clientes = [
    ('2b162060', 'MARIVALDA', 'SP'),
    ('2b16242a', 'JUCILENE',  'ES'),
    ('2b16256a', 'GRACIMAR',  'MG'),
    ('2b16353c', 'ALDENORA',  'SP'),
    ('2b1636ae', 'VERA',      'RJ'),
    ('2b16396a', 'IVONE',     'RJ'),
    ('2b163bcc', 'LUCILIA',   'RS'),
    ('2b163bff', 'MARTINS',   ''),
    ('2b163bdd', 'GENARO',    '')
]

# Criando o DataFrame de clientes com o schema definido
df_clientes = spark.createDataFrame(dados_clientes, schema=schema_clientes)

df_clientes.show()
# Cria uma visualização temporária de CLIENTES
df_clientes.createOrReplaceTempView("clientes")

print("DataFrame de pedidos")

schema_pedidos = StructType([
    StructField("id_cliente", StringType(), True),
    StructField("id_pedido", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("descricao_produto", StringType(), True),
    StructField("valor_produto", FloatType(), True),
    StructField("valor_total_pedido", FloatType(), True),
    StructField("data_pedido", StringType(), True)
])

# Criando o DataFrame de pedidos com o schema definido
dados_pedidos = [
    ('2b162060', 10, 2, 'Celular', 1500.00, 3000.00, '2024-09-01'),
    ('2b162060', 20, 1, 'Notebook', 3500.00, 3500.00, '2024-09-05'),
    ('2b16242a', 30, 1, 'Geladeira', 2000.00, 2000.00, '2024-09-03'),
    ('2b16396a', 40, 1, 'Smart TV', 2500.00, 2500.00, '2024-09-08'),
    ('2b16353c', 50, 10, 'Teclado', 150.00, 1500.00, '2024-09-10'),
    ('2b16256a', 60, 1, 'Fogão', 1200.00, 1200.00, '2024-09-02'),
    ('2b16256a', 70, 1, 'Microondas', 800.00, 800.00, '2024-09-04'),
    ('2b16256a', 80, 1, 'Máquina de Lavar', 1800.00, 1800.00, '2024-09-06'),
    ('2b16256a', 90, 1, 'Ventilador', 200.00, 200.00, '2024-09-09'),
    ('2b16256a', 99, 1, 'Aspirador de Pó', 600.00, 600.00, '2024-09-11')
]

df_pedidos = spark.createDataFrame(dados_pedidos, schema=schema_pedidos)

df_pedidos.show()
# Cria uma visualização temporária de PEDIDOS
df_pedidos.createOrReplaceTempView("pedidos")  

```

Depois de criar a visualização temporária, você pode executar consultas SQL sobre ela como se fosse uma tabela SQL tradicional:

```python
# Exemplo de consulta SQL em uma visualização temporária
clientes_df = spark.sql("SELECT * FROM clientes WHERE id_cliente = '2b162060'")
clientes_df.show()

```

**Nota:** As visualizações temporárias são específicas para a sessão Spark em que foram criadas e não persistem após o término da sessão.


## 3. Junções

```python
spark.sql("""
        SELECT c.id_cliente, c.nome, p.id_pedido, p.valor_total_pedido
        FROM clientes c
        JOIN pedidos p ON c.id_cliente = p.id_cliente
    """).show()
```


## 4. Agregações 

As agregações são operações fundamentais em SQL e PySpark, permitindo resumir e analisar grandes volumes de dados. No PySpark, você pode realizar agregações usando funções SQL diretamente nas visualizações temporárias criadas a partir de DataFrames.

### 4.1 Funções de Agregação Comuns

Algumas das funções de agregação mais comuns incluem:

- `COUNT`: Conta o número de linhas.
- `SUM`: Soma os valores de uma coluna.
- `AVG`: Calcula a média dos valores de uma coluna.
- `MIN`: Encontra o valor mínimo de uma coluna.
- `MAX`: Encontra o valor máximo de uma coluna.

### 4.2 Exemplos de Agregações

A seguir, alguns exemplos de como realizar agregações em visualizações temporárias no PySpark.

#### 4.2.1 Contagem de Pedidos por Cliente

```python
# Contar o número de pedidos por cliente
contagem_pedidos = spark.sql("""
    SELECT id_cliente, COUNT(*) as total_pedidos
    FROM pedidos
    GROUP BY id_cliente
""")
contagem_pedidos.show()
```

#### 4.2.2 Soma do Valor Total dos Pedidos por Cliente

```python
# Somar o valor total dos pedidos por cliente
soma_valor_pedidos = spark.sql("""
    SELECT id_cliente, SUM(valor_total_pedido) as valor_total
    FROM pedidos
    GROUP BY id_cliente
""")
soma_valor_pedidos.show()
```

#### 4.2.3 Média do Valor dos Produtos por Cliente

```python
# Calcular a média do valor dos produtos por cliente
media_valor_produtos = spark.sql("""
    SELECT id_cliente, AVG(valor_produto) as valor_medio_produto
    FROM pedidos
    GROUP BY id_cliente
""")
media_valor_produtos.show()
```

### 4.3 Agregações com Condições

Você também pode aplicar condições nas agregações para obter resultados mais específicos.

#### 4.3.1 Soma do Valor dos Pedidos Acima de um Determinado Valor

```python
# Somar o valor dos pedidos onde o valor total do pedido é maior que 2000
soma_valor_pedidos_condicional = spark.sql("""
    SELECT id_cliente, SUM(valor_total_pedido) as valor_total
    FROM pedidos
    WHERE valor_total_pedido > 2000
    GROUP BY id_cliente
""")
soma_valor_pedidos_condicional.show()
```

## 5. Rollups e Cubes

Rollups e cubes são técnicas avançadas de agregação que permitem sumarizar dados em múltiplos níveis de granularidade. Essas operações são extremamente úteis para análises multidimensionais, como relatórios de vendas por diferentes combinações de dimensões (por exemplo, por produto, por região, por período).

### 5.1 Rollups

A operação de rollup permite criar subtotais em uma hierarquia de grupos. Por exemplo, você pode calcular subtotais de vendas por ano, por trimestre dentro de cada ano, e por mês dentro de cada trimestre.

#### 5.1.1 Exemplo de Rollup

```python
# Exemplo de rollup para calcular vendas por ano, trimestre e mês
rollup_vendas = spark.sql("""
    SELECT id_cliente, 
           YEAR(data_pedido) as ano, 
           QUARTER(data_pedido) as trimestre, 
           MONTH(data_pedido) as mes, 
           SUM(valor_total_pedido) as total_vendas
    FROM pedidos
    GROUP BY ROLLUP(id_cliente, ano, trimestre, mes)
    ORDER BY id_cliente, ano, trimestre, mes
""")
rollup_vendas.show()
```

### 5.2 Cubes

A operação de cube é uma generalização do rollup que permite calcular subtotais para todas as combinações possíveis de um conjunto de dimensões. Isso é útil para análises que requerem a visualização de dados em todas as perspectivas possíveis.

#### 5.2.1 Exemplo de Cube

```python
# Exemplo de cube para calcular vendas por combinação de cliente, ano, trimestre e mês
cube_vendas = spark.sql("""
    SELECT id_cliente, 
           YEAR(data_pedido) as ano, 
           QUARTER(data_pedido) as trimestre, 
           MONTH(data_pedido) as mes, 
           SUM(valor_total_pedido) as total_vendas
    FROM pedidos
    GROUP BY CUBE(id_cliente, ano, trimestre, mes)
    ORDER BY id_cliente, ano, trimestre, mes
""")
cube_vendas.show()
```

### 5.3 Considerações Finais

Rollups e cubes são poderosas ferramentas para análise de dados, permitindo que você obtenha insights detalhados e resumidos de grandes volumes de dados. No PySpark, essas operações são realizadas de maneira eficiente, aproveitando o poder de processamento distribuído do Spark.


## 6. Desafio
Faça o clone dos datasets a seguir:
```sh
git clone https://github.com/infobarbosa/datasets-csv-clientes
```

```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos
```

```sh
git clone https://github.com/infobarbosa/dataset-json-pagamentos
```

Utilizando os datasets acima e os conhecimentos adquiridos ao longo do curso, elabore os scripts PySpark que respondam às seguintes questões:
1. Qual o valor total de pedidos para o estado de São Paulo?
2. Apresente o valor total de pedidos com status de fraude igual a verdadeiro. Agrupe por UF e inclua o total geral considerando todos os estados.

## 7. Parabéns!
Você concluiu com sucesso o módulo de PySpark com SQL! Agora você está apto a integrar consultas SQL com DataFrames no PySpark, realizar junções, agregações e utilizar técnicas avançadas como rollups e cubes. Continue praticando e explorando novas funcionalidades para se tornar ainda mais proficiente em análise de dados com PySpark.

Bom trabalho e continue aprendendo!

## 8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.