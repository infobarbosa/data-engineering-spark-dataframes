# Manipulação Avançada de DataFrame - Funções de Janela

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
As Window Functions possibilitam a realização de cálculos avançados, permitindo o particionamento e a ordenação de dados para análises mais sofisticadas.

A função `Window.partitionBy` é utilizada para definir uma janela de partição em um DataFrame, permitindo que operações de janela sejam aplicadas a subconjuntos específicos dos dados. Ao particionar os dados com base em uma ou mais colunas, é possível realizar cálculos como agregações, classificações e funções analíticas dentro de cada partição, sem afetar outras partições. Isso é particularmente útil para análises que exigem a segmentação dos dados por categorias, como clientes, produtos ou departamentos, garantindo que as operações sejam realizadas de forma isolada e eficiente dentro de cada grupo.

A função `Window.partitionBy` aceita um ou mais parâmetros que especificam as colunas pelas quais os dados serão particionados. Esses parâmetros determinam como o DataFrame será dividido em subconjuntos, permitindo que operações de janela sejam aplicadas de forma independente dentro de cada partição. Por exemplo, ao usar `Window.partitionBy("coluna1", "coluna2")`, o DataFrame será particionado com base nos valores combinados de `coluna1` e `coluna2`. Isso é útil para realizar cálculos como somas, médias, classificações e outras funções analíticas dentro de grupos específicos de dados, garantindo que os resultados sejam calculados isoladamente para cada partição.

## 2. Exemplo 1 com `row_number()`
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, hour, date_trunc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("dataeng-window-functions") \
    .getOrCreate()

# Definindo o dataset
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

print("Dataframe df_pedido: ")
df_pedido.show(truncate=False)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Definindo a janela para particionar os dados por 'id_cliente' e ordenar por 'data_pedido'
window_spec_cliente = Window.partitionBy("id_cliente").orderBy("data_pedido")

# Aplicando a função row_number()
df_pedido_window_cliente = df_pedido.withColumn("numero_pedido", row_number().over(window_spec_cliente))

print("Dataframe df_pedido_window_cliente: ")
df_pedido_window_cliente \
    .select("id_cliente", "numero_pedido", "data_pedido") \
    .orderBy("id_cliente", "numero_pedido").show()

```

Output esperado:
```
+----------+-------------+-----------+
|id_cliente|numero_pedido|data_pedido|
+----------+-------------+-----------+
|  2b162060|            1| 2024-09-01|
|  2b162060|            2| 2024-09-05|
|  2b162345|            1| 2024-09-12|
|  2b162346|            1| 2024-09-13|
|  2b162347|            1| 2024-09-14|
|  2b162348|            1| 2024-09-15|
|  2b162349|            1| 2024-09-16|
|  2b162350|            1| 2024-09-17|
|  2b162351|            1| 2024-09-18|
|  2b162352|            1| 2024-09-19|
|  2b162353|            1| 2024-09-20|
|  2b162354|            1| 2024-09-21|
|  2b16242a|            1| 2024-09-03|
|  2b16256a|            1| 2024-09-02|
|  2b16256a|            2| 2024-09-04|
|  2b16256a|            3| 2024-09-06|
|  2b16256a|            4| 2024-09-09|
|  2b16256a|            5| 2024-09-11|
|  2b16353c|            1| 2024-09-10|
|  2b16396a|            1| 2024-09-08|
+----------+-------------+-----------+
```

## 2. Exemplo 2 com `row_number()`

```python
# Definindo a janela para particionar os dados por 'departamento_produto' e ordenar por 'data_pedido'
window_spec_departamento = Window.partitionBy("departamento_produto").orderBy("data_pedido")

# Aplicando a função row_number()
df_pedido_window_departamento = df_pedido.withColumn("numero_pedido", row_number().over(window_spec_departamento))

print("Dataframe df_pedido_window_departamento: ")
df_pedido_window_departamento \
    .select("departamento_produto", "numero_pedido", "data_pedido") \
    .orderBy("departamento_produto", "numero_pedido").show()

```

Output esperado:
```
+--------------------+-------------+-----------+
|departamento_produto|numero_pedido|data_pedido|
+--------------------+-------------+-----------+
|   CASA E CONSTRUCAO|            1| 2024-09-16|
|    ELETRODOMESTICOS|            1| 2024-09-02|
|    ELETRODOMESTICOS|            2| 2024-09-03|
|    ELETRODOMESTICOS|            3| 2024-09-04|
|    ELETRODOMESTICOS|            4| 2024-09-06|
|    ELETRODOMESTICOS|            5| 2024-09-09|
|    ELETRODOMESTICOS|            6| 2024-09-11|
|    ELETRODOMESTICOS|            7| 2024-09-12|
|    ELETRODOMESTICOS|            8| 2024-09-21|
|         ELETRONICOS|            1| 2024-09-01|
|         ELETRONICOS|            2| 2024-09-05|
|         ELETRONICOS|            3| 2024-09-08|
|         ELETRONICOS|            4| 2024-09-19|
|             ESPORTE|            1| 2024-09-15|
|         INFORMATICA|            1| 2024-09-10|
|         INFORMATICA|            2| 2024-09-13|
|         INFORMATICA|            3| 2024-09-14|
|              MOVEIS|            1| 2024-09-17|
|              MOVEIS|            2| 2024-09-18|
|           PAPELARIA|            1| 2024-09-20|
+--------------------+-------------+-----------+
```

O mesmo pode ser feito sem uso de `withColumn`:
```python
print("Diretamente no select, sem criar a nova coluna")
df_pedido \
    .select("departamento_produto", \
            "data_pedido", \
            row_number().over(window_spec_departamento).alias("posicao")) \
    .orderBy("departamento_produto", "posicao") \
    .show()

```

## 3. Exemplo com `rank()`

A função `rank()` é uma função de janela que atribui uma classificação a cada linha dentro de uma partição, com base na ordenação especificada. Em caso de empates, as linhas recebem a mesma classificação, e a próxima classificação é pulada.

### Exemplo:

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("dataeng-rank-function") \
    .getOrCreate()

# Criando um DataFrame de exemplo
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

# Aplicando a função rank
df_com_rank = df.withColumn("rank", rank().over(janela_departamento))

# Exibindo o resultado
df_com_rank.show()

# Parando a sessão Spark
spark.stop()
```

### Explicação do código

1. **Criação do DataFrame**: Criamos um DataFrame com dados de funcionários, seus departamentos e salários.
2. **Definição da Janela**: Definimos uma janela que particiona os dados por `departamento` e ordena por `salario` em ordem decrescente.
3. **Aplicação da Função `rank`**: Adicionamos uma nova coluna `rank` ao DataFrame, que contém a classificação dos salários dentro de cada departamento.
4. **Exibição do Resultado**: Mostramos o DataFrame resultante, que inclui a classificação de cada funcionário dentro de seu departamento.

Output esperado:

```
+--------+------------+-------+----+
|    nome|departamento|salario|rank|
+--------+------------+-------+----+
|   Pedro|      Vendas|   7000|   1|
|   Lucas|      Vendas|   7000|   1|
|   Maria|      Vendas|   6000|   3|
|    João|      Vendas|   5000|   4|
| Ricardo|          TI|   9000|   1|
|  Bianca|          TI|   9000|   1|
|  Marcos|          TI|   8500|   3|
|Fernanda|          TI|   8000|   4|
|   Bruno|   Marketing|   5500|   1|
|   Sofia|   Marketing|   5000|   2|
| Letícia|   Marketing|   5000|   2|
|   Paula|          RH|   4000|   1|
|  Carlos|          RH|   4000|   1|
|     Ana|          RH|   3000|   3|
+--------+------------+-------+----+
```

Observe que:

- Funcionários com o mesmo salário dentro do mesmo departamento recebem a mesma classificação (`rank`).
- A classificação seguinte é pulada após um empate.

A função `rank()` é útil para atribuir classificações a registros dentro de partições, considerando empates. Isso é especialmente útil em análises onde a posição relativa dos registros é importante, como em rankings de desempenho ou classificações de vendas.

---


## 4. Função `dense_rank`

A função `dense_rank` é uma função de janela que atribui uma classificação a cada linha dentro de uma partição, com base na ordenação especificada. Ao contrário da função `rank`, a `dense_rank` não pula classificações em caso de empates.

### Exemplo:

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("dataeng-dense-rank-function") \
    .getOrCreate()

# Criando um DataFrame de exemplo
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

# Aplicando a função dense_rank
df_com_dense_rank = df.withColumn("dense_rank", dense_rank().over(janela_departamento))

# Exibindo o resultado
df_com_dense_rank.show()

# Parando a sessão Spark
spark.stop()
```

### Explicação do código

1. **Criação do DataFrame**: Criamos um DataFrame com dados de funcionários, seus departamentos e salários.
2. **Definição da Janela**: Definimos uma janela que particiona os dados por `departamento` e ordena por `salario` em ordem decrescente.
3. **Aplicação da Função `dense_rank`**: Adicionamos uma nova coluna `dense_rank` ao DataFrame, que contém a classificação dos salários dentro de cada departamento.
4. **Exibição do Resultado**: Mostramos o DataFrame resultante, que inclui a classificação de cada funcionário dentro de seu departamento.

Output esperado:

```
+--------+------------+-------+----------+
|    nome|departamento|salario|dense_rank|
+--------+------------+-------+----------+
|   Pedro|      Vendas|   7000|         1|
|   Lucas|      Vendas|   7000|         1|
|   Maria|      Vendas|   6000|         2| # Observe que, ao contrário de rank(), dense_rank() não pula classificações em caso de empates
|    João|      Vendas|   5000|         3|
| Ricardo|          TI|   9000|         1|
|  Bianca|          TI|   9000|         1|
|  Marcos|          TI|   8500|         2|
|Fernanda|          TI|   8000|         3|
|   Bruno|   Marketing|   5500|         1|
|   Sofia|   Marketing|   5000|         2|
| Letícia|   Marketing|   5000|         2|
|   Paula|          RH|   4000|         1|
|  Carlos|          RH|   4000|         1|
|     Ana|          RH|   3000|         2|
+--------+------------+-------+----------+
```

Observe que:

- Funcionários com o mesmo salário dentro do mesmo departamento recebem a mesma classificação (`dense_rank`).
- A classificação seguinte não é pulada após um empate.

A função `dense_rank` é útil para atribuir classificações a registros dentro de partições, considerando empates, mas sem pular classificações subsequentes. Isso é especialmente útil em análises onde a posição relativa dos registros é importante, como em rankings de desempenho ou classificações de vendas.

---


## 5. Função `lag`

A função `lag` é uma função de janela que permite acessar o valor de uma coluna em uma linha anterior, baseada na ordenação definida na janela. Isso é útil para calcular diferenças entre linhas consecutivas, como variações de vendas diárias ou mudanças de preços.

### Exemplo:

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("dataeng-lag-function") \
    .getOrCreate()

# Criando um DataFrame de exemplo
dados = [
    ("2023-01-01", "Produto A", 100),
    ("2023-01-02", "Produto A", 150),
    ("2023-01-03", "Produto A", 200),
    ("2023-01-04", "Produto A", 250),
    ("2023-01-01", "Produto B", 80),
    ("2023-01-02", "Produto B", 120),
    ("2023-01-03", "Produto B", 160),
    ("2023-01-04", "Produto B", 200)
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
df_com_lag.show()

# Parando a sessão Spark
spark.stop()
```

### Explicação do código

1. **Criação do DataFrame**: Criamos um DataFrame com dados de vendas diárias de dois produtos.
2. **Conversão da Coluna de Data**: Convertendo a coluna `data` para o tipo `Date`.
3. **Definição da Janela**: Definimos uma janela que particiona os dados por `produto` e ordena por `data`.
4. **Aplicação da Função `lag`**: Usamos `lag("vendas", 1).over(janela)` para obter o valor das vendas na linha anterior dentro da mesma partição.
5. **Cálculo da Diferença de Vendas**: Calculamos a diferença entre as vendas atuais e as vendas do dia anterior.

Ouput esperado:

```
+----------+---------+------+-------------------+----------+
|      data|  produto|vendas|vendas_dia_anterior|dif_vendas|
+----------+---------+------+-------------------+----------+
|2023-01-01|Produto A|   100|               null|      null|
|2023-01-02|Produto A|   150|                100|        50|
|2023-01-03|Produto A|   200|                150|        50|
|2023-01-04|Produto A|   250|                200|        50|
|2023-01-01|Produto B|    80|               null|      null|
|2023-01-02|Produto B|   120|                 80|        40|
|2023-01-03|Produto B|   160|                120|        40|
|2023-01-04|Produto B|   200|                160|        40|
+----------+---------+------+-------------------+----------+
```

Observe que para cada produto, a coluna `vendas_dia_anterior` mostra as vendas do dia anterior, e `dif_vendas` mostra a diferença em relação ao dia anterior.

A função `lag` é extremamente útil para análises que requerem a comparação de valores entre linhas consecutivas. Ela permite calcular variações, taxas de crescimento e outras métricas que dependem de valores sequenciais.

---

## 6. Desafio

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

