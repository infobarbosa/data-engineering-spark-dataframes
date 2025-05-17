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

Ao trabalhar com arquivos JSON no PySpark, a lógica de leitura segue alguns passos fundamentais que você precisa considerar para garantir uma leitura eficiente e correta. Aqui estão os principais pontos:

---

## 2. **Formato do Arquivo**
  - **Simples vs. Multilinha**: Um arquivo JSON pode ser escrito em um formato de linha única, onde cada linha é um objeto JSON separado, ou em um formato multilinha, onde um objeto JSON pode se estender por várias linhas. PySpark trata essas variações de maneira diferente.
  - **Formato padrão** (linhas únicas): Cada linha deve conter um objeto JSON completo.
  - **Formato multilinha** (`multiLine=True`): O arquivo pode ter um objeto JSON distribuído em várias linhas.

---

## 3. **Estrutura do JSON**
  - **Estrutura plana**: Se o JSON contém uma estrutura simples, onde os dados estão diretamente no nível superior (campos de chave-valor simples), a leitura será direta.
  - **Estrutura aninhada**: Se o JSON contém campos complexos (como listas, dicionários aninhados), PySpark pode lidar com isso, mas você pode precisar "explodir" esses campos ou usar funções específicas para manipular esses dados complexos.

---

## 4. **Schema Inference (Inferência de Esquema)**
  - Por padrão, PySpark tenta inferir automaticamente o esquema do JSON. No entanto, isso pode não ser a abordagem mais eficiente ou precisa, especialmente para arquivos grandes ou complexos. Para evitar isso:
  - **Esquema explícito**: Você pode definir manualmente o esquema ao ler o arquivo para melhorar o desempenho e a precisão.
  - **Inferência de esquema automática**: Usar `inferSchema=True` é uma opção para JSONs simples, mas pode ser mais lento em arquivos muito grandes.

---

## 5. **Manuseio de Dados Complexos**
  **Array e objetos aninhados**: JSONs frequentemente contêm arrays ou objetos aninhados. Para manipular esses dados, você pode precisar usar funções como `explode()` para quebrar arrays ou acessar campos internos com `dot notation` (ex.: `dataframe.select("campo.objeto_interno")`).

#### 📌 O que a função explode faz?
A função explode() transforma valores que estão em arrays (ou mapas) em várias linhas, uma para cada elemento. É usada quando você quer "desaninhar" estruturas complexas, como listas ou arrays de structs, para processar ou visualizar cada item separadamente.

#### ✅ Quando é necessário usar explode?
Você deve usar explode quando:
- A coluna contém listas ou arrays (ex: ArrayType)
- Você quer transformar cada item da lista em uma linha separada

No exemplo de código apresentado anteriormente, manipulamos um DataFrame contendo uma coluna de arrays de structs (no caso, os cursos de cada aluno). Ao utilizar explode(df["cursos"]), transformamos cada elemento do array presente na coluna cursos em uma nova linha do DataFrame, mantendo as demais informações associadas ao registro original. Isso facilita a análise e o processamento de dados aninhados, permitindo, por exemplo, visualizar cada curso e nota de um aluno em linhas separadas. Assim, o uso do explode é fundamental para "desaninhar" estruturas complexas e trabalhar de forma mais eficiente com dados que possuem arrays ou listas em seu esquema.

---

## 6. **Leitura de Arquivos**
   O código básico para ler um arquivo JSON em PySpark é:

   ```python
   from pyspark.sql import SparkSession

   # Criar a sessão do Spark
   spark = SparkSession.builder.appName("LeituraJSON").getOrCreate()

   # Ler o arquivo JSON
   df = spark.read.json("caminho_do_arquivo.json")

   # Mostrar os dados
   df.show()
   ```

   Se o arquivo JSON for multilinha, você deve especificar o parâmetro `multiLine=True`:

   ```python
   df = spark.read.option("multiLine", True).json("caminho_do_arquivo.json")
   ```

---

## 7. **Considerações de Desempenho**
  - **Particionamento**: Se o JSON for muito grande, considere o particionamento adequado para otimizar o processamento distribuído no PySpark.
  - **Compressão**: Se o arquivo JSON estiver compactado (como `.gz` ou `.bz2`), PySpark pode ler diretamente esses arquivos sem descompactá-los manualmente.
  - **Schema pré-definido**: Sempre que possível, defina o esquema explicitamente para evitar inferências demoradas em grandes volumes de dados.

---

## 8. **Outro exemplo**
  Aqui está mais um exemplo completo com inferência de esquema e tratamento de um arquivo multilinha:

   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.types import StructType, StructField, StringType, IntegerType

   # Criar a sessão do Spark
   spark = SparkSession.builder.appName("LeituraJSON").getOrCreate()

   # Definir o esquema manualmente
   schema = StructType([
       StructField("id", IntegerType(), True),
       StructField("nome", StringType(), True),
       StructField("idade", IntegerType(), True),
       StructField("cidade", StringType(), True)
   ])

   # Ler o arquivo JSON com o esquema definido
   df = spark.read.option("multiline", "true").schema(schema).json("caminho_do_arquivo.json")

   # Mostrar os dados
   df.show()
   ```

---

## 9. Considerações adicionais
  - **Tipo de dados**: Certifique-se de que os tipos de dados no esquema estão alinhados com os valores no JSON, especialmente em arquivos grandes.
  - **Tratamento de erros**: Às vezes, os arquivos JSON podem conter registros corrompidos ou malformados. Use `mode="DROPMALFORMED"` para ignorar esses registros.

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

