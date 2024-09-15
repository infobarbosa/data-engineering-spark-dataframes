# Módulo 2: Manipulação Avançada de DataFrame

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Neste módulo, exploraremos técnicas avançadas de manipulação de DataFrames no Apache Spark. Abordaremos a aplicação de funções definidas pelo usuário (UDFs e UDAFs), manipulação de DataFrames com estruturas de dados aninhadas e transformações complexas como pivot, unpivot, rollups e cubes.

## 2. Aplicação de Funções Complexas (UDFs, UDAFs)
### 2.1. User-Defined Functions (UDFs)
UDFs permitem a aplicação de funções personalizadas em colunas de um DataFrame. Elas são úteis para operações complexas que não são diretamente suportadas pelas funções nativas do Spark.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

# Criando uma sessão do Spark
spark = SparkSession.builder \
    .appName("DataFrames Lab") \
    .getOrCreate()

# Exemplo de criação de DataFrame para teste
data = [('Barbosa', 7300), ('Roberto', 3650), ( 'Charles', 1825), ('Leandro', 912), ('Evanildo', 14056), ('Francisco', 2103)]
columns = ["nome", "idade_em_dias"]
df = spark.createDataFrame(data, columns)
df.show()
from pyspark.sql.types import IntegerType

# Definindo uma UDF para calcular a idade em anos
@udf(IntegerType())
def calcular_idade_em_anos(idade_em_dias):
    return idade_em_dias // 365

# Aplicando a UDF em uma coluna do DataFrame
df = df.withColumn("idade_em_anos", calcular_idade_em_anos(df["idade_em_dias"]))
df.show()

```

### 2.2. Desafio

**Desafio PySpark - Uso de UDF (User Defined Function)**

Crie uma UDF que, dado o nome de uma pessoa e sua data de nascimento, retorne uma saudação personalizada informando a idade atual da pessoa. Adicione essa saudação como uma nova coluna no DataFrame.

**Código inicial:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("DesafioUDF").getOrCreate()

# Criando um DataFrame de exemplo
data = [
    ("Barbosa", "1990-05-14"),
    ("Roberto", "1985-07-23"),
    ("Charles", "1992-12-02"),
    ("Leandro", "1988-03-08"),
    ("Evanildo", "1995-10-30"),
    ("Francisco", "1991-08-19"),
    ("Graciane", "1987-01-11"),
    ("Heidson", "1993-11-29"),
    ("Ivan", "1989-06-05"),
    ("Judite", "1994-09-17")
]

columns = ["nome", "data_nascimento"]
df = spark.createDataFrame(data, columns)

# Seu código aqui para definir e aplicar a UDF

# Exibindo o DataFrame resultante
df.show(truncate=False)
```

**Instruções:**

- Implemente uma UDF que calcule a idade da pessoa com base na data de nascimento.
- A UDF deve retornar uma saudação no formato: `"Olá, {nome}! Você tem {idade} anos."`
- Adicione essa saudação como uma nova coluna chamada `"saudacao"` no DataFrame.
- Considere a data atual como `"2023-10-01"` para o cálculo da idade.

**Exemplo de saída esperada:**

```
+----------+---------------+--------------------------------------+
|nome      |data_nascimento|saudacao                              |
+----------+---------------+--------------------------------------+
|Barbosa   |1990-05-14     |Olá, Barbosa! Você tem 33 anos.       |
|Roberto   |1985-07-23     |Olá, Roberto! Você tem 38 anos.       |
|Charles   |1992-12-02     |Olá, Charles! Você tem 30 anos.       |
|Leandro   |1988-03-08     |Olá, Leandro! Você tem 35 anos.       |
|Evanildo  |1995-10-30     |Olá, Evanildo! Você tem 27 anos.      |
|Francisco |1991-08-19     |Olá, Francisco! Você tem 32 anos.     |
|Graciane  |1987-01-11     |Olá, Graciane! Você tem 36 anos.      |
|Heidson   |1993-11-29     |Olá, Heidson! Você tem 29 anos.       |
|Ivan      |1989-06-05     |Olá, Ivan! Você tem 34 anos.          |
|Judite    |1994-09-17     |Olá, Judite! Você tem 29 anos.        |
+----------+---------------+--------------------------------------+

```

**Dica:** Você pode usar bibliotecas padrão do Python dentro da UDF para auxiliar no cálculo da idade.

**Observação:** Certifique-se de que todas as importações necessárias estejam presentes e que o código seja executado sem erros.

#### Solução 1 do desafio
<details>
  <summary>Clique aqui</summary>

**Solução do Desafio PySpark - Uso de UDF**

Vamos implementar a UDF que calcula a idade e retorna a saudação personalizada conforme solicitado.

**Código completo:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf
from datetime import datetime

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("DesafioUDF").getOrCreate()

# Criando um DataFrame de exemplo
data = [
    ("Barbosa", "1990-05-14"),
    ("Roberto", "1985-07-23"),
    ("Charles", "1992-12-02"),
    ("Leandro", "1988-03-08"),
    ("Evanildo", "1995-10-30"),
    ("Francisco", "1991-08-19"),
    ("Graciane", "1987-01-11"),
    ("Heidson", "1993-11-29"),
    ("Ivan", "1989-06-05"),
    ("Judite", "1994-09-17")
]

columns = ["nome", "data_nascimento"]
df = spark.createDataFrame(data, columns)

# Definindo a UDF para calcular a idade e criar a saudação
def saudacao_personalizada(nome, data_nascimento):
    # Convertendo a data de nascimento para um objeto datetime
    data_nasc = datetime.strptime(data_nascimento, "%Y-%m-%d")
    # Obtendo a data atual
    data_atual = datetime.now().date()
    # Calculando a idade
    idade = data_atual.year - data_nasc.year - ((data_atual.month, data_atual.day) < (data_nasc.month, data_nasc.day))
    # Criando a saudação
    saudacao = f"Olá, {nome}! Você tem {idade} anos."
    return saudacao

# Registrando a UDF
saudacao_udf = udf(saudacao_personalizada, StringType())

# Aplicando a UDF ao DataFrame
df = df.withColumn("saudacao", saudacao_udf(df.nome, df.data_nascimento))

# Exibindo o DataFrame resultante
df.show(truncate=False)
```

**Explicação do Código:**

1. **Importações Necessárias:**
- `SparkSession` para iniciar a sessão Spark.
- `StringType` e `IntegerType` para definir os tipos de dados.
- `udf` para criar a função definida pelo usuário.
- `datetime` para manipular datas e calcular a idade.

2. **Inicializando a Sessão Spark:**
- Criamos uma sessão Spark com o nome `"DesafioUDF"`.

3. **Criando o DataFrame de Exemplo:**
- Utilizamos os dados fornecidos e definimos as colunas `"nome"` e `"data_nascimento"`.

4. **Definindo a UDF `saudacao_personalizada`:**
- A função recebe o `nome` e a `data_nascimento` como parâmetros.
- Converte a `data_nascimento` de `string` para um objeto `datetime`.
- Define a `data_atual` como `"2023-10-01"` e converte para um objeto `datetime`.
- Calcula a `idade` considerando se a pessoa já fez aniversário no ano atual.
- Retorna a saudação personalizada no formato desejado.

5. **Registrando a UDF:**
- Utilizamos `udf()` para registrar a função `saudacao_personalizada` como uma UDF do Spark, especificando que o tipo de retorno é `StringType()`.

6. **Aplicando a UDF ao DataFrame:**
- Usamos `withColumn()` para adicionar uma nova coluna `"saudacao"` ao DataFrame, aplicando a UDF aos campos `"nome"` e `"data_nascimento"`.

7. **Exibindo o DataFrame Resultante:**
- Utilizamos `df.show(truncate=False)` para mostrar o DataFrame completo sem truncar as colunas.

**Saída Esperada:**

```
+----------+---------------+--------------------------------------+
|nome      |data_nascimento|saudacao                              |
+----------+---------------+--------------------------------------+
|Barbosa   |1990-05-14     |Olá, Barbosa! Você tem 33 anos.       |
|Roberto   |1985-07-23     |Olá, Roberto! Você tem 38 anos.       |
|Charles   |1992-12-02     |Olá, Charles! Você tem 30 anos.       |
|Leandro   |1988-03-08     |Olá, Leandro! Você tem 35 anos.       |
|Evanildo  |1995-10-30     |Olá, Evanildo! Você tem 27 anos.      |
|Francisco |1991-08-19     |Olá, Francisco! Você tem 32 anos.     |
|Graciane  |1987-01-11     |Olá, Graciane! Você tem 36 anos.      |
|Heidson   |1993-11-29     |Olá, Heidson! Você tem 29 anos.       |
|Ivan      |1989-06-05     |Olá, Ivan! Você tem 34 anos.          |
|Judite    |1994-09-17     |Olá, Judite! Você tem 29 anos.        |
+----------+---------------+--------------------------------------+
```

**Notas Adicionais:**

- **Cálculo da Idade:**
- A idade é calculada subtraindo o ano de nascimento do ano atual.
- O ajuste `- ((data_atual.month, data_atual.day) < (data_nasc.month, data_nasc.day))` considera se a pessoa já fez aniversário no ano atual.

- **Uso de UDF:**
- As UDFs em PySpark permitem utilizar funções Python em operações de DataFrame do Spark.
- É importante especificar o tipo de retorno da UDF para que o Spark possa otimizar o processamento.

- **Performance:**
- Embora as UDFs sejam úteis, elas podem impactar a performance, pois quebram a otimização baseada em JVM do Spark.
- Para operações em larga escala, considere usar funções embutidas do Spark ou expressões SQL quando possível.

**Executando o Código:**

Certifique-se de que você tem o PySpark instalado e configurado corretamente no seu ambiente. Salve o código em um arquivo, por exemplo, `desafio_udf.py`, e execute com o comando:

```bash
spark-submit desafio_udf.py
```

Ou execute diretamente em um notebook ou ambiente interativo que suporte PySpark.  

</details>

#### Solução 2 do desafio
<details>
  <summary>Clique aqui</summary>

**Solução 2 do Desafio PySpark - Anotação `@udf`**

Vamos implementar a UDF que calcula a idade e retorna a saudação personalizada, desta vez utilizando a anotação `@udf` do PySpark.

**Código completo:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf
from datetime import datetime

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("DesafioUDF").getOrCreate()

# Criando um DataFrame de exemplo
data = [
    ("Barbosa", "1990-05-14"),
    ("Roberto", "1985-07-23"),
    ("Charles", "1992-12-02"),
    ("Leandro", "1988-03-08"),
    ("Evanildo", "1995-10-30"),
    ("Francisco", "1991-08-19"),
    ("Graciane", "1987-01-11"),
    ("Heidson", "1993-11-29"),
    ("Ivan", "1989-06-05"),
    ("Judite", "1994-09-17")
]

columns = ["nome", "data_nascimento"]
df = spark.createDataFrame(data, columns)

# Definindo a UDF para calcular a idade e criar a saudação
@udf(StringType())
def saudacao_personalizada(nome, data_nascimento):
    # Convertendo a data de nascimento para um objeto datetime
    data_nasc = datetime.strptime(data_nascimento, "%Y-%m-%d")
    # Obtendo a data atual
    data_atual = datetime.now().date()
    # Calculando a idade
    idade = data_atual.year - data_nasc.year - ((data_atual.month, data_atual.day) < (data_nasc.month, data_nasc.day))
    # Criando a saudação
    saudacao = f"Olá, {nome}! Você tem {idade} anos."
    return saudacao

# Aplicando a UDF ao DataFrame
df = df.withColumn("saudacao", saudacao_personalizada(df.nome, df.data_nascimento))

# Exibindo o DataFrame resultante
df.show(truncate=False)

```

</details>


#### Solução 3 do desafio
<details>
  <summary>Clique aqui</summary>

**Solução 3 do Desafio PySpark - Sem uso de UDF**

Vamos implementar o calculo da idade e retornar a saudação personalizada, desta vez sem qualquer uso de UDF.

**Código completo:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, lit
from datetime import datetime

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("DesafioUDF").getOrCreate()

# Criando um DataFrame de exemplo
data = [
    ("Barbosa", "1990-05-14"),
    ("Roberto", "1985-07-23"),
    ("Charles", "1992-12-02"),
    ("Leandro", "1988-03-08"),
    ("Evanildo", "1995-10-30"),
    ("Francisco", "1991-08-19"),
    ("Graciane", "1987-01-11"),
    ("Heidson", "1993-11-29"),
    ("Ivan", "1989-06-05"),
    ("Judite", "1994-09-17")
]

columns = ["nome", "data_nascimento"]
df = spark.createDataFrame(data, columns)
df.show(truncate=False)

# Calculando a idade e criando a saudação sem UDF
current_date = datetime.now().date()
df = df.withColumn("idade", expr(f"floor(datediff(current_date(), to_date(data_nascimento, 'yyyy-MM-dd')) / 365)"))
df = df.withColumn("saudacao", concat(lit("Olá, "), col("nome"), lit("! Você tem "), col("idade"), lit(" anos.")))

# Exibindo o DataFrame resultante
df.show(truncate=False)

```

</details>


-----------------------------------------

### 2.3. User-Defined Aggregate Functions (UDAFs)
UDAFs permitem a criação de agregações personalizadas que podem ser aplicadas em grupos de dados. Isso é útil para cálculos complexos que não são possíveis com funções agregadas padrão.

**Exemplo de código:**
Neste exemplo, vamos criar uma Função Agregada Definida pelo Usuário (UDAF) no PySpark que calcula o produto dos valores em cada grupo de dados.

```python
### 1. Importe as bibliotecas necessárias:
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

###  Inicialize o SparkSession:
spark = SparkSession.builder.appName("Exemplo UDAF").getOrCreate()

### 3. Crie um DataFrame de exemplo:
data = [("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5)]
df = spark.createDataFrame(data, ["grupo", "valor"])

### 4. Defina a UDAF:
@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def produto_udaf(v: pd.Series) -> float:
    return v.prod()

### 5. Use a UDAF no agrupamento:
resultado_df = df.groupby("grupo").agg(produto_udaf(df.valor).alias("produto"))

### 6. Mostre os resultados:
resultado_df.show()
```

**Saída esperada:**

```
+------+-------+
| grupo|produto|
+------+-------+
|     B|   20.0|
|     A|    6.0|
+------+-------+
```

**Explicação:**

- **produto_udaf**: Esta função calcula o produto de uma série de valores dentro de cada grupo.
- **@pandas_udf**: Decorador que define uma função UDAF usando Pandas UDFs.
  - `"double"`: Tipo de dado de retorno da função.
  - `PandasUDFType.GROUPED_AGG`: Indica que a função é uma agregação agrupada.
- **df.groupby("grupo")**: Agrupa o DataFrame pelo campo "grupo".
- **agg(...)**: Aplica a função agregada definida ao grupo.


## 3. Manipulação de DataFrames Aninhados (Arrays, Structs)
DataFrames no Spark podem conter estruturas de dados complexas como arrays e structs. Manipular esses tipos de dados requer técnicas específicas.

**Exemplo de código:**
```python
### 1. Importe as bibliotecas necessárias:
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType

###  Inicialize o SparkSession:
spark = SparkSession.builder.appName("Exemplo UDAF").getOrCreate()

### 3. Crie um DataFrame de exemplo:
data = [
    ("João", [{"curso": "Matemática", "nota": 85}, {"curso": "História", "nota": 90}]),
    ("Maria", [{"curso": "Matemática", "nota": 95}, {"curso": "História", "nota": 80}])
]
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("cursos", ArrayType(StructType([
        StructField("curso", StringType(), True),
        StructField("nota", IntegerType(), True)
    ])), True)
])
df = spark.createDataFrame(data, schema)

# Explodindo o array para linhas individuais
df_exploded = df.withColumn("curso", explode(df["cursos"]))
df_exploded.select("nome", col("curso.curso"), col("curso.nota")).show()
```

**Desafio**

---

**Descrição do Desafio:**

Você recebeu um dataset contendo informações de clientes de uma empresa. O dataset possui estruturas de dados complexas, como arrays e structs. Seu objetivo é manipular esse dataset usando PySpark para extrair insights específicos.

---

**Dataset de Exemplo:**

O dataset está em formato JSON com o seguinte conteúdo:

```json
[
  {
    "nome": "Ana",
    "idade": 25,
    "notas": {
      "matematica": 90,
      "portugues": 85,
      "ciencias": 92
    },
    "contatos": [
      {"tipo": "email", "valor": "ana@example.com"},
      {"tipo": "telefone", "valor": "123456789"}
    ],
    "interesses": ["música", "esportes", "leitura"]
  },
  {
    "nome": "Bruno",
    "idade": 30,
    "notas": {
      "matematica": 78,
      "portugues": 88,
      "ciencias": 75
    },
    "contatos": [
      {"tipo": "email", "valor": "bruno@example.com"}
    ],
    "interesses": ["cinema", "viagens"]
  },
  {
    "nome": "Carla",
    "idade": 28,
    "notas": {
      "matematica": 85,
      "portugues": 80,
      "ciencias": 88
    },
    "contatos": [
      {"tipo": "telefone", "valor": "987654321"}
    ],
    "interesses": ["arte", "leitura", "viagens"]
  }
]
```

---

**Código Inicial:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("DesafioPySpark").getOrCreate()

# Carregar o dataset JSON
caminho_arquivo = "caminho/para/o/dataset.json"
dados_clientes = spark.read.json(caminho_arquivo)

# Mostrar o schema do DataFrame
dados_clientes.printSchema()

# Mostrar os dados
dados_clientes.show(truncate=False)

# Escreva sua logica aqui

# Apresente o resultado aqui

```

---

**Desafios a serem realizados:**

1. **Flatten das Structs:**

   - Extraia as notas de cada matéria (`matematica`, `portugues`, `ciencias`) e adicione como colunas separadas no DataFrame.

2. **Explodir os Arrays de Contatos:**

   - Transforme o array de contatos em linhas individuais, de forma que cada contato (email ou telefone) fique em uma linha separada, mantendo o nome da pessoa associado.

3. **Explodir os Interesses:**

   - Faça o mesmo para o array de interesses, criando uma linha para cada interesse por pessoa.

4. **Contagem de Interesses:**

   - Calcule quantas pessoas têm cada interesse, listando o interesse e a contagem correspondente.

5. **Média das Notas de Matemática:**

   - Calcule a média das notas de matemática de todos os clientes.

6. **Filtrar por Idade:**

   - Filtre o DataFrame para mostrar apenas os clientes com idade superior a 25 anos.

7. **Agrupar e Agregar:**

   - Agrupe os clientes por idade e calcule a média das notas de ciências para cada grupo de idade.

---

##### Soluções dos desafios

<details>
    <summary>Solução do Item 1: Flatten das Structs</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("DesafioPySpark").getOrCreate()

# Carregar o dataset JSON
caminho_arquivo = "dataset.json"
dados_clientes = spark.read.json(caminho_arquivo, multiLine=True)

# Mostrar o schema do DataFrame original
dados_clientes.printSchema()

# Mostrar os dados originais
dados_clientes.show(truncate=False)

# ----------------------------------------------------------------------
# Solução do Item 1: Flatten das Structs
# ----------------------------------------------------------------------

# Importar as funções necessárias
from pyspark.sql.functions import col, coalesce

# Criar um novo DataFrame com as notas extraídas
dados_notas = dados_clientes \
    .withColumn("nota_matematica", col("notas.matematica")) \
    .withColumn("nota_portugues", col("notas.portugues")) \
    .withColumn("nota_ciencias", col("notas.ciencias"))

# Exibir o esquema atualizado do DataFrame
dados_notas.printSchema()

# Mostrar os dados com as novas colunas de notas
dados_notas.select("nome", "idade", "nota_matematica", "nota_portugues", "nota_ciencias").show()

```
</details>

<details>
    <summary>Solução do Item 2: Explodir os Arrays de Contatos</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("DesafioPySpark").getOrCreate()

# Carregar o dataset JSON
caminho_arquivo = "dataset.json"
dados_clientes = spark.read.json(caminho_arquivo, multiLine=True)

# Mostrar o schema do DataFrame original
dados_clientes.printSchema()

# Mostrar os dados originais
dados_clientes.show(truncate=False)

# ----------------------------------------------------------------------
# Solução do Item 2: Explodir os Arrays de Contatos
# ----------------------------------------------------------------------

# Importar as funções necessárias
from pyspark.sql.functions import explode, col

# Explodir o array de contatos em linhas individuais
dados_contatos_explodido = dados_clientes.select(
    "nome",
    "idade",
    explode("contatos").alias("contato")
)

# Extrair os campos 'tipo' e 'valor' da struct 'contato'
dados_contatos_extracao = dados_contatos_explodido.select(
    "nome",
    "idade",
    col("contato.tipo").alias("tipo_contato"),
    col("contato.valor").alias("valor_contato")
)

# Exibir o esquema atualizado do DataFrame
dados_contatos_extracao.printSchema()

# Mostrar os dados após a explosão dos contatos
dados_contatos_extracao.show(truncate=False)

```

**Resultado esperado**
```
root
 |-- nome: string (nullable = true)
 |-- idade: long (nullable = true)
 |-- tipo_contato: string (nullable = true)
 |-- valor_contato: string (nullable = true)

+-----+-----+------------+------------------+
|nome |idade|tipo_contato|valor_contato     |
+-----+-----+------------+------------------+
|Ana  |25   |email       |ana@example.com   |
|Ana  |25   |telefone    |123456789         |
|Bruno|30   |email       |bruno@example.com |
|Carla|28   |telefone    |987654321         |
+-----+-----+------------+------------------+

```
</details>

## 4. Transformações Avançadas (Pivot, Unpivot, Rollups, Cubes)
### 4.1. Pivot e Unpivot
O pivot transforma valores únicos de uma coluna em múltiplas colunas, enquanto o unpivot faz o processo inverso.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("DesafioPySpark").getOrCreate()

from pyspark.sql.functions import explode, col

# Exemplo de DataFrame com arrays e structs
data = [
    ("João", [{"curso": "MATEMATICA", "nota": 85}, {"curso": "HISTORIA", "nota": 90}]),
    ("Maria", [{"curso": "MATEMATICA", "nota": 95}, {"curso": "HISTORIA", "nota": 80}])
]
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("cursos", ArrayType(StructType([
        StructField("curso", StringType(), True),
        StructField("nota", IntegerType(), True)
    ])), True)
])
df = spark.createDataFrame(data, schema)

df.show( truncate=False)

# Explodindo o array para linhas individuais
df_exploded = df.withColumn("curso", explode(df["cursos"]))
df = df_exploded.select("nome", col("curso.curso"), col("curso.nota"))

df.show()

print('Exemplo de Pivot')
df_pivot = df.groupBy("nome").pivot("curso").agg({"nota": "max"})
df_pivot.show()

print('Exemplo de Unpivot (Requer manipulação manual no PySpark')

unpivoted = df_pivot.selectExpr("nome", "stack(2, 'MATE', MATEMATICA, 'HIST', HISTORIA) as (curso, nota)")
unpivoted.show()
```

### 4.2. Rollups e Cubes
Rollups e cubes são usados para criar agregações hierárquicas em grupos de dados, sendo especialmente úteis para relatórios multidimensionais.

#### Rollup

A função `rollup` é usada para gerar uma hierarquia de agregações. Ela permite agregar dados em diferentes níveis de granularidade. O `rollup` cria uma série de subtotais e um total geral, permitindo ver a evolução dos dados em níveis agregados.

**Exemplo de uso do `rollup`**:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Criação da sessão do Spark
spark = SparkSession.builder.appName("dataeng-rollup").getOrCreate()

# Criação de um DataFrame de exemplo
data = [
    ("2024-01-01", "A", 10),
    ("2024-01-01", "B", 20),
    ("2024-01-02", "A", 30),
    ("2024-01-02", "B", 40),
]
df = spark.createDataFrame(data, ["data", "categoria", "valor"])
df.show()

print('Aplicação do rollup')
result = df.rollup("data", "categoria").agg(sum("valor").alias("valor_total"))

result.show()
```

Neste exemplo, o `rollup` gera subtotais por `data` e `categoria`, além de um total geral.

#### Cube

A função `cube` é usada para criar uma agregação em todas as combinações possíveis dos grupos especificados. Isso é útil para obter um resumo completo dos dados para todas as combinações dos grupos fornecidos.

**Exemplo de uso do `cube`**:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Criação da sessão do Spark
spark = SparkSession.builder.appName("dataeng-cube").getOrCreate()

# Criação de um DataFrame de exemplo
data = [
    ("2024-01-01", "A", 10),
    ("2024-01-01", "B", 20),
    ("2024-01-02", "A", 30),
    ("2024-01-02", "B", 40),
]
df = spark.createDataFrame(data, ["data", "categoria", "valor"])
df.show()

print('Aplicação do cube') 
result = df.cube("data", "categoria").agg(sum("valor").alias("valor_total"))

result.show()
```

Neste exemplo, o `cube` gera todas as combinações possíveis das colunas `data` e `category`, retornando um resumo completo das agregações.

#### Resumo

- **`rollup`**: Cria agregações hierárquicas e totais gerais. Útil para relatórios que exigem subtotais em diferentes níveis.
- **`cube`**: Cria todas as combinações possíveis dos grupos, permitindo análises mais detalhadas em várias dimensões.

Ambos são ferramentas poderosas para análise e sumarização de dados em PySpark.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("dataeng-rollup-cube").getOrCreate()

from pyspark.sql.functions import explode, col

# Exemplo de DataFrame com arrays e structs
data = [
    ("João", [{"curso": "MATEMATICA", "nota": 85}, {"curso": "HISTORIA", "nota": 90}]),
    ("Maria", [{"curso": "MATEMATICA", "nota": 95}, {"curso": "HISTORIA", "nota": 80}])
]
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("cursos", ArrayType(StructType([
        StructField("curso", StringType(), True),
        StructField("nota", IntegerType(), True)
    ])), True)
])
df = spark.createDataFrame(data, schema)

df.show( truncate=False)

# Explodindo o array para linhas individuais
df_exploded = df.withColumn("curso", explode(df["cursos"]))
df = df_exploded.select("nome", col("curso.curso"), col("curso.nota"))

df.show()

print('Exemplo de Rollup')
df_rollup = df.rollup("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_rollup.show()

print('Exemplo de Cube')
df_cube = df.cube("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_cube.show()

```

## 5. Parabéns!
Parabéns por concluir o módulo! Você aprendeu técnicas avançadas de manipulação de DataFrames no Apache Spark, aplicando UDFs, UDAFs e explorando transformações complexas.

## 6. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

