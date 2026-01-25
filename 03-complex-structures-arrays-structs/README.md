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

## Exemplo 1

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

---



## 2. **Manuseio de Dados Complexos**
  **Array e objetos aninhados**: JSONs frequentemente contêm arrays ou objetos aninhados. Para manipular esses dados, você pode precisar usar funções como `explode()` para quebrar arrays ou acessar campos internos com `dot notation` (ex.: `dataframe.select("campo.objeto_interno")`).

#### 📌 O que a função explode faz?
A função explode() transforma valores que estão em arrays (ou mapas) em várias linhas, uma para cada elemento. É usada quando você quer "desaninhar" estruturas complexas, como listas ou arrays de structs, para processar ou visualizar cada item separadamente.

#### ✅ Quando é necessário usar explode?
Você deve usar explode quando:
- A coluna contém listas ou arrays (ex: ArrayType)
- Você quer transformar cada item da lista em uma linha separada

No exemplo de código apresentado anteriormente, manipulamos um DataFrame contendo uma coluna de arrays de structs (no caso, os cursos de cada aluno). Ao utilizar explode(df["cursos"]), transformamos cada elemento do array presente na coluna cursos em uma nova linha do DataFrame, mantendo as demais informações associadas ao registro original. Isso facilita a análise e o processamento de dados aninhados, permitindo, por exemplo, visualizar cada curso e nota de um aluno em linhas separadas. Assim, o uso do explode é fundamental para "desaninhar" estruturas complexas e trabalhar de forma mais eficiente com dados que possuem arrays ou listas em seu esquema.

## Exemplo 2

Vamos considerar um dataset de um campeonato de futebol com 3 times. O dataset inclui duas estruturas aninhadas:

- **Array de jogadores** (necessita de `explode` para analisar cada jogador individualmente)
- **Struct de estatísticas do time** (não necessita de `explode`, basta acessar os campos internos)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType

# Iniciar sessão Spark
spark = SparkSession.builder.appName("CampeonatoFutebol").getOrCreate()

data = [
  {
    "time": "Linhares",
    "jogadores": [
      {"nome": "Chimbinha", "gols": 5, "cartoes": 1},
      {"nome": "Fofão", "gols": 2, "cartoes": 0}
    ],
    "estatisticas": {
      "vitorias": 8,
      "empates": 2,
      "derrotas": 1
    }
  },
  {
    "time": "Ibiraçu",
    "jogadores": [
      {"nome": "Sansão", "gols": 3, "cartoes": 2},
      {"nome": "Morte Lenta", "gols": 4, "cartoes": 1}
    ],
    "estatisticas": {
      "vitorias": 7,
      "empates": 3,
      "derrotas": 1
    }
  },
  {
    "time": "Colatina",
    "jogadores": [
      {"nome": "Pepê", "gols": 6, "cartoes": 0},
      {"nome": "Neném", "gols": 1, "cartoes": 2}
    ],
    "estatisticas": {
      "vitorias": 6,
      "empates": 4,
      "derrotas": 1
    }
  }
]

schema = StructType([
    StructField("time", StringType(), True),
    StructField("jogadores", ArrayType(
        StructType([
            StructField("nome", StringType(), True),
            StructField("gols", IntegerType(), True),
            StructField("cartoes", IntegerType(), True)
        ])
    ), True),
    StructField("estatisticas", StructType([
        StructField("vitorias", IntegerType(), True),
        StructField("empates", IntegerType(), True),
        StructField("derrotas", IntegerType(), True)
    ]), True)
])

df = spark.createDataFrame(data, schema)

# 1. Explodir o array de jogadores para analisar cada jogador individualmente
df_jogadores = df.select(
    "time",
    explode("jogadores").alias("jogador"),
    "estatisticas"
)

# Selecionar informações detalhadas dos jogadores
df_jogadores.select(
    "time",
    col("jogador.nome").alias("nome_jogador"),
    col("jogador.gols"),
    col("jogador.cartoes")
).show()

# 2. Acessar campos internos da struct de estatísticas (sem explode)
df_estatisticas = df.select(
    "time",
    col("estatisticas.vitorias"),
    col("estatisticas.empates"),
    col("estatisticas.derrotas")
)

df_estatisticas.show()

```

**Resumo:**
- Use `explode` para transformar o array de jogadores em linhas individuais.
- Para acessar campos de uma struct (como estatísticas), basta usar a notação de ponto, sem necessidade de `explode`.

---

## 10. Desafio (Arrays, Structs)

**Descrição do Desafio:**

Você recebeu um dataset contendo informações de clientes de uma empresa. O dataset possui estruturas de dados complexas, como arrays e structs. Seu objetivo é manipular esse dataset usando PySpark para extrair insights específicos.

**Tarefas do desafio:**

1. **Flatten das Structs:**

  - Extraia as notas de cada matéria (`matematica`, `portugues`, `ciencias`) e adicione como colunas separadas no DataFrame.

    Output esperado:
    ```
    +-----+-----+----------+---------+--------+
    |nome |idade|matematica|portugues|ciencias|
    +-----+-----+----------+---------+--------+
    |Ana  |25   |90        |85       |92      |
    |Bruno|30   |78        |88       |75      |
    |Carla|28   |85        |80       |88      |
    +-----+-----+----------+---------+--------+
    ```

2. **Explodir os Arrays de Contatos:**

  - Transforme o array de contatos em linhas individuais, de forma que cada contato (email ou telefone) fique em uma linha separada, mantendo o nome da pessoa associado.

    Output esperado:
    ```
    +-----+-----+--------+-----------------+
    |nome |idade|tipo    |valor            |
    +-----+-----+--------+-----------------+
    |Ana  |25   |email   |ana@example.com  |
    |Ana  |25   |telefone|123456789        |
    |Bruno|30   |email   |bruno@example.com|
    |Carla|28   |telefone|987654321        |
    +-----+-----+--------+-----------------+
    ```

3. **Explodir os Interesses:**

  - Faça o mesmo para o array de interesses, criando uma linha para cada interesse por pessoa.

    Output esperado:
    ```
    +-----+-----+---------+
    | nome|idade|interesse|
    +-----+-----+---------+
    |  Ana|   25|   música|
    |  Ana|   25| esportes|
    |  Ana|   25|  leitura|
    |Bruno|   30|   cinema|
    |Bruno|   30|  viagens|
    |Carla|   28|     arte|
    |Carla|   28|  leitura|
    |Carla|   28|  viagens|
    +-----+-----+---------+
    ```  
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

## 11. Parabéns!
Parabéns por concluir o módulo! Você aprendeu técnicas manipulação de DataFrames no Apache Spark explorando estruturas complexas em arrays e structs.

## 12. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

