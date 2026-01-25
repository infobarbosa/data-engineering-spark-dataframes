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


### 🧱 Tipos de Estruturas Complexas

No Spark (e em formatos como JSON), nem tudo cabe em uma célula simples de Excel. Às vezes precisamos de estruturas mais robustas para organizar os dados. Existem três tipos principais:

#### 1. ArrayType (A Lista) `[...]`

É uma coleção de itens do **mesmo tipo**. O tamanho da lista pode variar: uma linha pode ter 2 itens, a outra 10, e a outra nenhum.

* **Como reconhecer:** Colchetes `[ ]`
* **Exemplo:**
  ```json
  "interesses": ["Música", "Futebol", "Leitura"]
  ```
* **Schema PySpark**:
  ```python
  StructField("interesses", ArrayType(StringType()), True)
  ```

* **No PySpark:** Usa-se a função`explode()` para transformar essa lista horizontal linhas verticais.

---

### 2. StructType (Objeto Fixo) `{...}`

É uma estrutura rígida onde cada campo tem um **nome** e um **tipo** definidos previamente. É como uma mini-tabela dentro de uma coluna.

* **Como reconhecer:** Chaves `{ }` com chaves conhecidas.
* **Exemplo:**
  ```json
  "endereco": {
      "rua": "Av. Paulista",
      "numero": 1000,
      "cidade": "São Paulo"
  }
  ```
* **Schema PySpark**:
  ```python
  StructField("endereco", StructType([
      StructField("rua", StringType(), True),
      StructField("numero", IntegerType(), True),
      StructField("cidade", StringType(), True)
  ]), True)
  ```
* **No PySpark:** Acessamos os campos internos usando ponto: `col("endereco.cidade")`.

---

### 3. MapType (Dicionário Dinâmico) `{ k -> v }`

É uma coleção de pares **Chave-Valor**. Diferente do Struct, aqui as chaves **não são fixas** no esquema. Cada linha pode ter chaves totalmente diferentes.

* **Como reconhecer:** Chaves `{ }` mas tratado como pares.
* **Exemplo:**
  ```json
  "investimentos": {
      "FIIs": 5000.00,
      "Bitcoin": 200.00
  }
  ```
* **Schema PySpark**:
  ```python
  StructField("investimentos", MapType(StringType(), DoubleType()), True)
  ````

* **No PySpark:** Usa-se a função `explode()` para gerar duas colunas genéricas: uma para a `key` (a etiqueta) e outra para o `value` (o valor).

---

### Exemplo 1

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

### Exemplo 2

Vamos considerar um dataset de um campeonato de futebol com 3 times. O dataset inclui duas estruturas aninhadas:

- **Array de jogadores** (necessita de `explode` para analisar cada jogador individualmente)
- **Struct de estatísticas do time** (não necessita de `explode`, basta acessar os campos internos)
- **Map de patrocinadores** (necessita de `explode` para analisar cada patrocinador individualmente)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType, MapType

# Iniciar sessão Spark
spark = SparkSession.builder.appName("CampeonatoFutebolMap").getOrCreate()

data = [
  {
    "time": "Linhares",
    # ARRAY: Lista de objetos
    "jogadores": [
      {"nome": "Chimbinha", "gols": 5, "cartoes": 1},
      {"nome": "Fofão", "gols": 2, "cartoes": 0}
    ],
    # STRUCT: Colunas fixas (sempre tem vitoria, empate, derrota)
    "estatisticas": {
      "vitorias": 8,
      "empates": 2,
      "derrotas": 1
    },
    # MAP: Chaves dinâmicas (cada time tem patrocinadores diferentes)
    "patrocinadores": {"Supermercado Tio Zé": 3000, "Padaria Central": 1500}
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
    },
    "patrocinadores": {"Mecânica Simão": 4500}
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
    },
    "patrocinadores": {"Farmácia Saúde": 2000, "Lanchonete da Praça": 1200}
  }
]

# Definindo o Schema explicitamente para garantir o MapType
schema = StructType([
    StructField("time", StringType(), True),
    
    # 1. ARRAY de STRUCTS
    StructField("jogadores", ArrayType(
        StructType([
            StructField("nome", StringType(), True),
            StructField("gols", IntegerType(), True),
            StructField("cartoes", IntegerType(), True)
        ])
    ), True),
    
    # 2. STRUCT Simples
    StructField("estatisticas", StructType([
        StructField("vitorias", IntegerType(), True),
        StructField("empates", IntegerType(), True),
        StructField("derrotas", IntegerType(), True)
    ]), True),

    # 3. MAP (Chave: Nome do Patrocinador, Valor: Contrato R$)
    StructField("patrocinadores", MapType(StringType(), IntegerType()), True)
])

df = spark.createDataFrame(data, schema)
print("--- Schema do DataFrame ---")
df.printSchema()

# ---------------------------------------------------------
# EXEMPLO 1: ARRAY (Jogadores)
# Explode gera uma linha por item da lista
# ---------------------------------------------------------
print("\n--- 1. Explode Array (Jogadores) ---")
df_jogadores = df.select(
    "time",
    explode("jogadores").alias("jogador")
)
# Acessando campos internos da struct que foi explodida
df_jogadores.select(
    "time",
    col("jogador.nome"),
    col("jogador.gols")
).show(truncate=False)

# ---------------------------------------------------------
# EXEMPLO 2: STRUCT (Estatísticas)
# Não usa explode. Usa "ponto" para navegar na hierarquia fixa.
# ---------------------------------------------------------
print("\n--- 2. Acesso a Struct (Estatísticas) ---")
df_estatisticas = df.select(
    "time",
    col("estatisticas.vitorias"),
    col("estatisticas.derrotas")
)
df_estatisticas.show(truncate=False)

# ---------------------------------------------------------
# EXEMPLO 3: MAP (Patrocinadores)
# Explode gera duas colunas fixas: 'key' e 'value'
# ---------------------------------------------------------
print("\n--- 3. Explode Map (Patrocinadores) ---")
df_patrocinadores = df.select(
    "time",
    explode("patrocinadores").alias("patrocinador_nome", "valor_contrato")
)
df_patrocinadores.show(truncate=False)

```

**Resumo:**
- Use `explode` para transformar o array de jogadores (ArrayType) m linhas individuais.
- Use `explode` para transformar a estrutura de patrocinadores (MapType) em pares chave-valor.
- Para acessar campos de uma struct (como estatísticas), basta usar a notação de ponto, sem necessidade de `explode`.

---

## 3. Desafio 1

Você recebeu um dataset contendo informações de clientes de uma empresa. O dataset possui estruturas de dados complexas, como arrays e structs. Seu objetivo é manipular esse dataset usando PySpark para extrair insights específicos.

**Tarefas:**

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
spark = SparkSession.builder.appName("data-eng-complex-structures").getOrCreate()

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

### Soluções dos desafios

<details>
    <summary>Solução do Item 1: Flatten das Structs</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("data-eng-flatten-structs").getOrCreate()

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
spark = SparkSession.builder.appName("data-eng-explode-arrays").getOrCreate()

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

---

## 4. Desafio 2 - Campanha Leitura & Renda
Utilizando o dataset de clientes, gere um DataFrame que mostre apenas os candidatos à campanha **Leitura & Renda**. Para isso, você precisará normalizar (explodir) a coluna de investimentos para filtrar quem tem **FIIs** e também verificar a lista de interesses."

### Tarefas
1. Explodir a coluna `carteira_investimentos` para transformar o Map em linhas de tipo_investimento e valor.
  * Atenção! Nesse caso você vai precisar tratar `carteira_investimentos` como `MapType`.
2. Filtrar apenas as linhas onde o `tipo_investimento` é igual a "FIIs".
3. Filtrar os clientes que possuem "Livros" OU "Economia" dentro do array `interesses`.

### Output esperado
```
+---------------------+-----+--------+------------------------------+
|nome                 |ativo|valor   |interesses                    |
+---------------------+-----+--------+------------------------------+
|Donna Luna           |FIIs |45980.88|[Música, Livros, Astronomia]  |
|Beatriz Souza        |FIIs |23776.66|[Livros]                      |
|Davi Lucca Costa     |FIIs |46564.9 |[Livros, Lazer, Religião]     |
|Emanuella da Mota    |FIIs |20496.74|[Ciências, Astrologia, Livros]|
|Otto Cavalcanti      |FIIs |38825.44|[Livros, Lazer]               |
|Vitor Hugo Moreira   |FIIs |1818.16 |[Livros, Gastronomia, Música] |
|Fernando Cardoso     |FIIs |28402.07|[Economia]                    |
|Giovanna Caldeira    |FIIs |31379.87|[Economia, Política, Viagens] |
|Rodrigo Nascimento   |FIIs |35713.46|[Viagens, Livros, Economia]   |
|Ian Marques          |FIIs |19400.76|[Viagens, Livros]             |
|Luna Costela         |FIIs |21244.44|[Economia]                    |
|Bento Almeida        |FIIs |24032.7 |[Economia]                    |
|Gabriel Gomes        |FIIs |48443.13|[Economia, Esportes, Lazer]   |
|Dante Vargas         |FIIs |1000.58 |[Livros, Gastronomia]         |
|Danilo Cavalcante    |FIIs |12326.4 |[Livros]                      |
|Samuel da Cruz       |FIIs |4554.25 |[Astronomia, Música, Economia]|
|Allana Marques       |FIIs |21540.26|[Economia]                    |
|Luiz Gustavo Ferreira|FIIs |32314.42|[Livros]                      |
|Dawn Collins         |FIIs |5886.46 |[Economia]                    |
|Daniela Moraes       |FIIs |17485.78|[Economia, Filmes, Astrologia]|
+---------------------+-----+--------+------------------------------+
```

### Dataset
```sh
mkdir -p ./data/inputs/

```

```sh
wget -P ./data/inputs https://raw.githubusercontent.com/infobarbosa/dataset-json-clientes/main/data/clientes.json.gz

```

```sh
zcat ./data/inputs/clientes.json.gz | head -n 10

```

### Código inicial
```sh
touch desafio-campanha-leitura-renda.py

```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, array_contains, explode_outer

# Inicialização (ajuste o caminho do arquivo conforme seu ambiente)
spark = SparkSession.builder.appName("data-eng-desafio-map-type").getOrCreate()

# 1. DEFINA O SCHEMA
schema = None

df = spark.read.schema(schema).json("./data/inputs/clientes.json.gz")

# 2. Explodir o Map 
# df_investimentos = df.select(...)

# 3. Filtrar FIIs
# df_final = df_investimentos.filter(...)

# 4. Filtrar por Interesses (Array) 
# df_final = df_investimentos.filter(array_contains( ...

# 5. Mostre o resultado
print("### Público Campanha Leitura & Renda")
df_final.select("nome", "ativo", "valor", "interesses").show(truncate=False)

```

### Solução
**Não desista!** Tente ao menos uma solução antes de checar a resposta. ;)

<details>
    <summary>Solução do desafio 2</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, array_contains
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, MapType, DoubleType

spark = SparkSession.builder \
    .appName("data-eng-desafio-map-type") \
    .getOrCreate()

# 1. Defina o schema
schema = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("data_nasc", StringType(), True),
    StructField("cpf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("interesses", ArrayType(StringType()), True),
    StructField("carteira_investimentos", MapType(StringType(), DoubleType()), True)
])

df = spark.read.schema(schema).json("./data/inputs/clientes.json.gz")

# 2. Explodir o Map
df_investimentos = df.select(
    "id",
    "nome",
    "interesses",
    explode("carteira_investimentos").alias("ativo", "valor")
)

# 3. Filtrar FIIs
df_investimentos = df_investimentos.filter(col("ativo") == "FIIs")

# 4. Filtrar por Interesses (Array)
df_final = df_investimentos.filter(
    array_contains(col("interesses"), "Livros") | 
    array_contains(col("interesses"), "Economia")
)

# Mostre o resultado
print("### Campanha Leitura & Renda")
df_final.select("nome", "ativo", "valor", "interesses").show(truncate=False)

```

</details>


## 5. Parabéns!
Parabéns por concluir o módulo! Você aprendeu técnicas manipulação de DataFrames no Apache Spark explorando estruturas complexas em arrays e structs.

## 6. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

