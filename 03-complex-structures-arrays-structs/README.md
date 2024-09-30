# Estruturas complexas (Arrays, Structs)

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
DataFrames no Spark podem conter estruturas de dados complexas como arrays e structs. Manipular esses tipos de dados requer técnicas específicas.

**Exemplo de código:**
```python
### 1. Importe as bibliotecas necessárias:
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType

###  Inicialize o SparkSession:
spark = SparkSession.builder.appName("dataeng-complex-structures").getOrCreate()

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

## 2. Desafio (Arrays, Structs)

---

**Descrição do Desafio:**

Você recebeu um dataset contendo informações de clientes de uma empresa. O dataset possui estruturas de dados complexas, como arrays e structs. Seu objetivo é manipular esse dataset usando PySpark para extrair insights específicos.

---

**Tarefas do desafio:**

1. **Flatten das Structs:**

   - Extraia as notas de cada matéria (`matematica`, `portugues`, `ciencias`) e adicione como colunas separadas no DataFrame.

2. **Explodir os Arrays de Contatos:**

   - Transforme o array de contatos em linhas individuais, de forma que cada contato (email ou telefone) fique em uma linha separada, mantendo o nome da pessoa associado.

3. **Explodir os Interesses:**

   - Faça o mesmo para o array de interesses, criando uma linha para cada interesse por pessoa.

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


## 3. Parabéns!
Parabéns por concluir o módulo! Você aprendeu técnicas manipulação de DataFrames no Apache Spark explorando estruturas complexas em arrays e structs.

## 4. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

