### Módulo 2: Manipulação Avançada de DataFrame

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 2.1. Introdução
Neste módulo, exploraremos técnicas avançadas de manipulação de DataFrames no Apache Spark. Abordaremos a aplicação de funções definidas pelo usuário (UDFs e UDAFs), manipulação de DataFrames com estruturas de dados aninhadas e transformações complexas como pivot, unpivot, rollups e cubes.

### 2.2. Aplicação de Funções Complexas (UDFs, UDAFs)
#### 2.2.1. User-Defined Functions (UDFs)
UDFs permitem a aplicação de funções personalizadas em colunas de um DataFrame. Elas são úteis para operações complexas que não são diretamente suportadas pelas funções nativas do Spark.

**Exemplo de código:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Definindo uma UDF para calcular a idade em anos
@udf(IntegerType())
def calcular_idade_em_anos(idade_em_dias):
    return idade_em_dias // 365

# Aplicando a UDF em uma coluna do DataFrame
df = df.withColumn("idade_em_anos", calcular_idade_em_anos(df["idade_em_dias"]))
df.show()
```

#### 2.2.2. Desafio

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
    ("Alice", "1990-05-14"),
    ("Bob", "1985-07-23"),
    ("Charlie", "1992-12-02"),
    ("Diana", "1988-03-08"),
    ("Eve", "1995-10-30"),
    ("Frank", "1991-08-19"),
    ("Grace", "1987-01-11"),
    ("Heidi", "1993-11-29"),
    ("Ivan", "1989-06-05"),
    ("Judy", "1994-09-17")
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
+-------+---------------+--------------------------------------+
|nome   |data_nascimento|saudacao                              |
+-------+---------------+--------------------------------------+
|Alice  |1990-05-14     |Olá, Alice! Você tem 33 anos.         |
|Bob    |1985-07-23     |Olá, Bob! Você tem 38 anos.           |
|Charlie|1992-12-02     |Olá, Charlie! Você tem 30 anos.       |
|Diana  |1988-03-08     |Olá, Diana! Você tem 35 anos.         |
|Eve    |1995-10-30     |Olá, Eve! Você tem 27 anos.           |
|Frank  |1991-08-19     |Olá, Frank! Você tem 32 anos.         |
|Grace  |1987-01-11     |Olá, Grace! Você tem 36 anos.         |
|Heidi  |1993-11-29     |Olá, Heidi! Você tem 29 anos.         |
|Ivan   |1989-06-05     |Olá, Ivan! Você tem 34 anos.          |
|Judy   |1994-09-17     |Olá, Judy! Você tem 29 anos.          |
+-------+---------------+--------------------------------------+
```

**Dica:** Você pode usar bibliotecas padrão do Python dentro da UDF para auxiliar no cálculo da idade.

**Observação:** Certifique-se de que todas as importações necessárias estejam presentes e que o código seja executado sem erros.





-----------------------------------------

#### 2.2.2. User-Defined Aggregate Functions (UDAFs)
UDAFs permitem a criação de agregações personalizadas que podem ser aplicadas em grupos de dados. Isso é útil para cálculos complexos que não são possíveis com funções agregadas padrão.

**Exemplo de código:**
```python
from pyspark.sql.expressions import UserDefinedAggregateFunction
from pyspark.sql.types import *

class MediaPersonalizadaUDAF(UserDefinedAggregateFunction):
    def inputSchema(self):
        return StructType([StructField("valor", IntegerType())])

    def bufferSchema(self):
        return StructType([StructField("soma", IntegerType()), StructField("contagem", IntegerType())])

    def dataType(self):
        return FloatType()

    def deterministic(self):
        return True

    def initialize(self, buffer):
        buffer[0] = 0
        buffer[1] = 0

    def update(self, buffer, input):
        buffer[0] += input.valor
        buffer[1] += 1

    def merge(self, buffer1, buffer2):
        buffer1[0] += buffer2[0]
        buffer1[1] += buffer2[1]

    def evaluate(self, buffer):
        return buffer[0] / buffer[1]

# Aplicando a UDAF em um grupo de dados
df.groupBy("grupo").agg(MediaPersonalizadaUDAF(df["valor"])).show()
```

### 2.3. Manipulação de DataFrames Aninhados (Arrays, Structs)
DataFrames no Spark podem conter estruturas de dados complexas como arrays e structs. Manipular esses tipos de dados requer técnicas específicas.

**Exemplo de código:**
```python
from pyspark.sql.functions import explode, col

# Exemplo de DataFrame com arrays e structs
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

### 2.4. Transformações Avançadas (Pivot, Unpivot, Rollups, Cubes)
#### 2.4.1. Pivot e Unpivot
O pivot transforma valores únicos de uma coluna em múltiplas colunas, enquanto o unpivot faz o processo inverso.

**Exemplo de código:**
```python
# Exemplo de Pivot
df_pivot = df.groupBy("nome").pivot("curso").agg({"nota": "max"})
df_pivot.show()

# Exemplo de Unpivot (Requer manipulação manual no Spark)
from pyspark.sql import functions as F

unpivoted = df_pivot.selectExpr("nome", "stack(2, 'Matemática', Matemática, 'História', História) as (curso, nota)")
unpivoted.show()
```

#### 2.4.2. Rollups e Cubes
Rollups e cubes são usados para criar agregações hierárquicas em grupos de dados, sendo especialmente úteis para relatórios multidimensionais.

**Exemplo de código:**
```python
# Exemplo de Rollup
df_rollup = df.rollup("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_rollup.show()

# Exemplo de Cube
df_cube = df.cube("nome", "curso").agg({"nota": "avg"}).orderBy("nome", "curso")
df_cube.show()
```

### 2.5. Exercício Prático Avançado
**Objetivo:** Aplicar transformações complexas em um DataFrame contendo estruturas aninhadas, usar UDFs para cálculos personalizados e realizar operações de pivot, unpivot, rollup e cube.

**Instruções:**
1. Clone o repositório do curso:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-2.git
   ```
2. Navegue até a pasta do módulo 2:
   ```bash
   cd dataeng-modulo-2
   ```
3. Execute o script `modulo2.py` que realiza as seguintes etapas:
   - Criação de UDFs e UDAFs.
   - Manipulação de DataFrames com arrays e structs.
   - Aplicação de transformações avançadas como pivot, unpivot, rollups e cubes.

**Código do laboratório:**
```python
# Exemplo de script modulo2.py

# Definindo e aplicando uma UDF
@udf(IntegerType())
def calcular_bonus(nota):
    return nota + 5

df_bonus = df_exploded.withColumn("nota_bonus", calcular_bonus(df_exploded["nota"]))
df_bonus.show()

# Aplicando transformações avançadas
df_pivot_bonus = df_bonus.groupBy("nome").pivot("curso").agg({"nota_bonus": "max"})
df_pivot_bonus.show()
```

### 2.6. Parabéns!
Parabéns por concluir o módulo 2! Você aprendeu técnicas avançadas de manipulação de DataFrames no Apache Spark, aplicando UDFs, UDAFs e explorando transformações complexas.

### 2.7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-2/
│
├── README.md
├── modulo2.py
```

