# Revisão dos Conceitos Básicos de DataFrame

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Neste módulo, vamos revisar os conceitos fundamentais do Apache Spark relacionados ao uso de DataFrames. Esta base teórica é essencial para os próximos módulos do curso, onde abordaremos tópicos mais avançados.

### 1.1. Apache Spark
O **Apache Spark** é uma plataforma de computação distribuída de código aberto projetada para processar grandes volumes de dados de forma rápida e eficiente. Ele foi desenvolvido para superar as limitações de desempenho do Hadoop MapReduce, oferecendo um modelo de processamento em memória que reduz significativamente o tempo de execução de tarefas.

### Propósito
O principal objetivo do Apache Spark é fornecer uma estrutura unificada para o processamento de dados em larga escala, permitindo a execução de tarefas como:
- Processamento em lote;
- Processamento em tempo real (streaming);
- Consultas interativas;
- Machine Learning;
- Análise de grafos.

### Principais Elementos da Arquitetura

![Apache Spark Architecture](apache-spark-architecture.png)

Crédito: https://spark.apache.org/docs/latest/cluster-overview.html

1. **Driver**: O componente central que coordena a execução do programa Spark. Ele traduz as operações de alto nível em tarefas distribuídas e as envia para os executores.

2. **Executors**: São os processos responsáveis por executar as tarefas atribuídas pelo driver. Cada executor armazena dados em memória e realiza cálculos.

3. **Cluster Manager**: Gerencia os recursos do cluster e aloca nós para o driver e os executores. Exemplos incluem YARN, Mesos e o gerenciador de cluster embutido do Spark.

4. **RDD (Resilient Distributed Dataset)**: A abstração fundamental do Spark, representando um conjunto de dados distribuído e imutável que pode ser processado em paralelo.

5. **Spark SQL**: Um módulo para trabalhar com dados estruturados, permitindo consultas SQL e integração com DataFrames e Datasets.

6. **Spark Streaming**: Um módulo para processamento de dados em tempo real, permitindo a análise contínua de fluxos de dados.

7. **MLlib**: Uma biblioteca de aprendizado de máquina que fornece algoritmos e ferramentas para tarefas como classificação, regressão e clustering.

8. **GraphX**: Um módulo para análise e processamento de grafos.

Esses elementos trabalham juntos para fornecer uma plataforma robusta e escalável para análise de dados em larga escala.

## 2. Conceitos Fundamentais de DataFrame
### 2.1. Criação de DataFrames
Os DataFrames são estruturas de dados distribuídas, imutáveis e organizadas em colunas nomeadas. No Spark, você pode criar DataFrames de várias fontes, como arquivos CSV, JSON, Parquet, tabelas SQL, entre outros.

**Exemplo**

1. Faça o clone do repositório a seguir:
   ```sh
   git clone https://github.com/infobarbosa/datasets-csv-clientes

   ```

   ```
   zcat datasets-csv-clientes/clientes.csv.gz | column -t -s ';' | head -10

   ```

2. Instale o **pyspark**:
   ```
   pip install pyspark

   ```

3. A seguir vamos criar um script `exemplos.py` que carrega o arquivo `clientes.csv.gz`.

   ```sh
   touch exemplos.py

   ```

   ```python
   from pyspark.sql import SparkSession
   import pyspark.sql.functions as F

   # Inicializando a SparkSession
   spark = SparkSession.builder.appName("dataeng-exemplo-dataframe").getOrCreate()

   # Criando um DataFrame a partir de um arquivo CSV
   schema = "id INT, nome STRING, data_nasc DATE, cpf STRING, email STRING, cidade STRING, uf STRING"
   df = spark.read \
      .format("csv") \
      .option("sep", ";") \
      .option("header", True) \
      .schema(schema) \
      .load("./datasets-csv-clientes/clientes.csv.gz")

   # Mostrando o schema
   df.printSchema()

   # Mostrando as primeiras linhas do DataFrame
   df.show()

   ```

   **Execução**
   ```sh
   python exemplos.py

   ```

---

### 2.2. Seleção de Colunas com `select`
A operação `select` no Spark permite selecionar colunas específicas de um DataFrame. Isso é útil quando você deseja trabalhar apenas com um subconjunto dos dados.

#### Exemplo 1

   **Código**
   ```python

   # Selecionando colunas específicas
   df_selected = df.select("id", "nome", "email")
   df_selected.show(5, truncate=False)

   ```

   **Execução**
   ```sh
   python exemplos.py

   ```

Neste exemplo, utilizamos a função `select` para escolher apenas as colunas `id`, `nome` e `email` do DataFrame original. Isso pode ser útil para reduzir a quantidade de dados processados ou para focar em informações específicas.

#### Exemplo 2
   É possível determinar apelidos (ou aliases) para as colunas selecionadas.

   1. Usando a função `selectExpr`:
   ```python
   print("### selectExpr")
   df_selected = df.selectExpr("id", "nome as nome_cliente", "email")   
   df_selected.show(5, truncate=False)

   ```   

   2. Usando a função `select` com `alias`:
   ```python
   print("### select com alias")
   df_selected = df.select("id", F.col("nome").alias("nome_cliente"), "email")
   df_selected.show(5, truncate=False)
   
   ```

   3. Usando o método `withColumnRenamed`:
   ```python
   print("### withColumnRenamed")
   df_selected = df.withColumnRenamed("nome", "nome_cliente")
   df_selected.show(5, truncate=False) 

   ```   

---
### 2.3. Filtragem com `filter`
No PySpark, existem dois métodos para aplicar filtros em DataFrames:

1. `filter()`: O método padrão para aplicar condições.
2. `where()`: Exatamente igual ao `filter()`, mas útil para quem vem do SQL e prefere essa semântica.

#### Formas de Expressar a Condição
1. Usando Strings (Estilo SQL): `df.filter("idade > 18")`. É simples e intuitivo para iniciantes.
2. Usando a Coluna (Objeto Column): `df.filter(df.idade > 18)` ou `df.filter(col("idade") > 18)`. Esta é a forma mais "Spark" e permite maior flexibilidade.

#### Exemplo 1

   ```python

   # Filtrando linhas onde a data de nascimento é menor ou igual a 1973-01-01
   df_filtrado = df.filter(F.col("data_nasc") <= "1973-01-01")
   # Mostrando as primeiras linhas do DataFrame filtrado
   df_filtrado.show(5, truncate=False)

   ```

#### Exemplo 2 (isin)
   ```python
   print("### isin")
   df_filtrado = df.filter(F.col("cidade").isin(["São Paulo", "Guarulhos"]))
   df_filtrado.show(5, truncate=False)

   ```

   **Desafio** 
   
   Crie um filtro que retorne apenas os clientes cujo CPF esteja na lista:
   - 487.602.159-76
   - 579.640.821-67
   - 167.259.048-58

#### Exemplo 3 (like)
   ```python
   print("### like")
   df_filtrado = df.filter(F.col("nome").like("%Barbosa%"))
   df_filtrado.show(5, truncate=False)

   ```

   **Desafio**
   
   Crie um filtro que retorne apenas os clientes cujos emails contêm o termo "gmail".

#### Exemplo 4 (rlike)
   O rlike permite que você use toda a biblioteca de Regex do Java/Python. Com ele, você pode criar filtros complexos que seriam impossíveis com o like simples.

   ```python
   print("### rlike")
   # O operador ~ inverte a lógica: "traga tudo que NÃO corresponde ao rlike"
   df_cpfs_invalidos = df.filter(~(F.col("cpf").rlike(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")))
   df_cpfs_invalidos.show(5, truncate=False)

   ```

   **Desafio**

   Verifique quais emails estão inválidos.<br/>
   A expressão regular para validar emails é: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`

#### Exemplo 5 (startswith)
   ```python
   print("### startswith")
   df_filtrado = df.filter(F.col("nome").startswith("Maria"))
   df_filtrado.show(5, truncate=False)

   ```

#### Exemplo 6 (endswith)
   ```python
   print("### endswith")
   df_filtrado = df.filter(F.col("nome").endswith("Silva"))
   df_filtrado.show(5, truncate=False)

   ```

#### Exemplo 7 (between)
   ```python
   print("### between")
   # Filtrando pessoas que nasceram entre 1975 e 1980
   df_filtrado = df.filter(F.col("data_nasc").between("1975-01-01", "1980-12-31"))
   df_filtrado.show(5, truncate=False)

   ```

#### Exemplo 8 (isNull / isNotNull)
   ```python
   print("### isNull")
   df_filtrado = df.filter(F.col("email").isNull())
   df_filtrado.show(5, truncate=False)

   ```

   ```python
   print("### isNotNull")
   df_filtrado = df.filter(F.col("email").isNotNull())
   df_filtrado.show(5, truncate=False)

   ```

   **Desafio**

   Verifique registros que não possuem **CPF**.

#### Exemplo 9 (where)
   Se você preferir usar a sintaxe SQL, pode usar o método `where`: 
   
   ```python
   # Filtrando pessoas que nasceram entre 1975 e 1980
   print("### where")
   df_filtrado = df.where("data_nasc between '1975-01-01' and '1980-12-31'")
   df_filtrado.show(5, truncate=False)

   ```

   **Desafio**

   Verifique registros de clientes que moram na cidade de **São Paulo** e que nasceram entre 1990 e 2000.

---
### 2.4. Operadores lógicos
Os operadores lógicos no PySpark são usados para combinar ou inverter condições ao filtrar ou manipular dados em DataFrames. Eles permitem criar expressões complexas para selecionar ou transformar dados com base em múltiplos critérios. Os operadores mais comuns incluem `&` (AND), `|` (OR), e `~` (NOT). Esses operadores são aplicados em conjunto com funções como `col` ou expressões de coluna.

**Exemplos de operadores lógicos:**

1. **AND (`&`)**  
   Filtra linhas que atendem a TODAS as condições especificadas.  
   ```python
   print("### AND")
   df_filtrado = df.filter((F.year(F.col("data_nasc")) < 1990) & (F.col("cidade") == "São Paulo"))
   df_filtrado.show(5, truncate=False)

   ```

2. **OR (`|`)**  
   Filtra linhas que atendem a pelo menos uma das condições especificadas.  
   ```python
   print("### OR")
   df_filtrado = df.filter((F.year(F.col("data_nasc")) < 1990) | (F.col("cidade") == "São Paulo"))
   df_filtrado.show(5, truncate=False)

   ```

3. **NOT (`~`)**  
   Filtra linhas que não atendem a uma condição específica.  
   ```python
   print("### NOT (isin)")
   estados_sudeste = ["SP", "RJ", "MG", "ES"]
   df_filtrado = df.filter(~(F.col("uf").isin(estados_sudeste)))
   df_filtrado.show(5, truncate=False)
   
   ```

   ```python
   print("### NOT (contains)")
   df_filtrado = df.filter(~(F.col("email").contains("hotmail.com")))
   df_filtrado.show(5, truncate=False)

   ```

#### Exemplo 1 (filtro composto)
   ```python

   # O operador & é o AND. Nesse caso, filtrando pessoas com nome iniciando por Pedro E nascimento entre 1975 e 1980
   df_filtrado = df.filter(
      (F.col("data_nasc").between("1978-01-01", "1980-12-31")) &
      (F.col("nome").startswith("Pedro"))
   )
   
   df_filtrado.show(5, truncate=False)

   ```

#### Exemplo 2 (filtro composto)
   ```python
   # O operador ~ inverte a lógica: "traga tudo que NÃO corresponde ao rlike"
   # O operador | é o OR. Nesse caso, traz todos os registros que não correspondem ao formato do CPF ou do email
   df_registros_invalidos = df.filter(
      ~(F.col("cpf").rlike(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")) | 
      ~(F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
   )

   df_registros_invalidos.show()

   ```

---
### 2.5. Enriquecimento com `withColumn`

#### Exemplo 1

   **Código**
   ```python
   print("### withColumn")
   # Criando as novas colunas: ano_nasc e primeiro_nome
   df_enriquecido = df \
      .withColumn("ano_nasc", F.year(F.col("data_nasc"))) \
      .withColumn("primeiro_nome", F.split(F.col("nome"), " ")[0])

   df_enriquecido.select("nome", "data_nasc", "ano_nasc", "primeiro_nome").show(5, truncate=False)

   ```

#### Exemplo 2 (When/Otherwise)

   No PySpark, as funções when e otherwise são usadas para criar colunas condicionais, de forma semelhante a um if...else ou CASE WHEN no SQL.

   ```python
   print("### withColumn com when/otherwise (1)")
   # Criando a coluna 'maior_de_idade' com base na diferença de dias
   df_com_maioridade = df.withColumn(
      "maior_de_idade",
      F.when((F.datediff(F.current_date(), F.col("data_nasc")) / 365.25) >= 18, "Sim").otherwise("Não")
   )

   df_com_maioridade.select("nome", "data_nasc", "maior_de_idade").show(5, truncate=False)

   ```

#### Exemplo 3 (When/Otherwise)

   É possível ter diversas cláusulas when no PySpark — e essa é, inclusive, a forma recomendada para simular um if...elif...else ou um CASE WHEN completo do SQL.

   ```python
   print("### withColumn com when/otherwise (2)")

   # Calculando a idade (diferença em anos, aproximada)
   df_com_idade = df.withColumn("idade", (F.datediff(F.current_date(), F.col("data_nasc")) / 365.25).cast("int"))

   # Classificando por faixa etária
   df_com_faixa_etaria = df_com_idade.withColumn("faixa_etaria",
      F.when(F.col("idade") < 12, "Criança")
      .when(F.col("idade") < 18, "Adolescente")
      .when(F.col("idade") < 60, "Adulto")
      .otherwise("Idoso")
   )

   df_com_faixa_etaria.select("nome", "data_nasc", "idade", "faixa_etaria").show(10, truncate=False)

   ```

---

### 2.6. Transformações e Ações
As transformações no Spark são operações "lazy", ou seja, elas não são executadas até que uma ação seja chamada. <br>
Exemplos de transformações incluem `filter`, `select`, `groupBy`, enquanto ações incluem `show`, `count`, `collect`.

#### Transformations

As **Transformações** no PySpark são operações que definem como os dados devem ser modificados, mas — ao contrário das ações — elas não são executadas imediatamente. Elas seguem o conceito de **Lazy Evaluation** (Avaliação Preguiçosa): o Spark apenas registra a sequência de transformações em um plano de execução chamado **DAG** (*Directed Acyclic Graph*).

As transformações são divididas em duas categorias principais, baseadas na necessidade de movimentar dados entre os nós do cluster (*shuffle*).

---

##### 1. Transformações Estreitas (*Narrow Transformations*)

São as mais eficientes, pois os dados necessários para calcular o resultado de uma partição estão em uma única partição do RDD/DataFrame pai. **Não exigem troca de dados entre os executores (no shuffle).**

| Transformação | Descrição |
| --- | --- |
| **`select()`** | Seleciona colunas específicas do DataFrame. |
| **`filter()`** / **`where()`** | Filtra linhas com base em uma condição lógica. |
| **`withColumn()`** | Adiciona uma nova coluna ou substitui uma existente. |
| **`drop()`** | Remove colunas específicas. |
| **`map()`** | (RDD) Aplica uma função a cada elemento e retorna um novo RDD. |
| **`flatMap()`** | Semelhante ao map, mas cada elemento de entrada pode ser mapeado para 0 ou mais elementos de saída (achata o resultado). |
| **`union()`** | Combina dois DataFrames com a mesma estrutura. |
| **`alias()`** | Atribui um apelido ao DataFrame ou coluna. |

---

##### 2. Transformações Largas (*Wide Transformations*)

Essas transformações exigem dados de várias partições para calcular o resultado, o que dispara um **Shuffle**. Isso significa que o Spark precisa mover dados pela rede entre os nós do cluster, o que é computacionalmente caro.

| Transformação | Descrição |
| --- | --- |
| **`groupBy()`** | Agrupa os dados com base em colunas específicas (geralmente seguida de uma agregação como `sum` ou `avg`). |
| **`orderBy()`** / **`sort()`** | Ordena os dados em todo o cluster. |
| **`distinct()`** | Remove linhas duplicadas (exige comparação global). |
| **`join()`** | Combina dois DataFrames com base em uma chave comum. |
| **`repartition(n)`** | Aumenta ou diminui o número de partições de forma balanceada (gera shuffle total). |
| **`coalesce(n)`** | Diminui o número de partições tentando evitar o shuffle (minimiza a movimentação). |
| **`intersect()`** | Retorna apenas as linhas que aparecem em ambos os DataFrames. |
| **`cube()`** / **`rollup()`** | Gera agregações multidimensionais complexas. |

---

##### 3. Transformações de Conjunto e Estrutura

Utilizadas para manipular a forma e o relacionamento entre datasets.

* **`dropDuplicates()`**: Remove duplicatas considerando colunas específicas.
* **`fillna()` / `na.fill()**`: Substitui valores nulos por um valor padrão.
* **`replace()`**: Substitui valores específicos por outros.
* **`sample()`**: Retorna uma fração aleatória dos dados (pode ser narrow ou wide dependendo dos parâmetros).
* **`pivot()`**: Transpõe linhas em colunas (operação cara que gera shuffle).

---
#### Actions
No PySpark, as **Ações (Actions)** são as operações que efetivamente iniciam o processamento dos dados. Diferente das transformações, que são "preguiçosas" (*Lazy Evaluation*) e apenas criam um plano de execução, as ações forçam o Spark a executar esse plano e retornar um resultado para o Driver ou gravar dados em um armazenamento externo.

---

##### 1. Ações de Recuperação e Visualização

Essas ações trazem dados do cluster para o seu programa local (Driver) ou os exibem no console.

| Ação | Descrição |
| --- | --- |
| **`collect()`** | Retorna todos os elementos do conjunto de dados como uma lista/array para o Driver. *Cuidado com datasets gigantes.* |
| **`show(n)`** | Exibe as primeiras `n` linhas de um DataFrame de forma tabular. |
| **`take(n)`** | Retorna os primeiros `n` elementos em uma lista. |
| **`first()`** | Retorna apenas o primeiro elemento (equivalente a `take(1)`). |
| **`head(n)`** | Semelhante ao `take`, retorna as primeiras `n` linhas do DataFrame. |
| **`top(n)`** | Retorna os `n` maiores elementos (baseado na ordenação padrão ou customizada). |
| **`takeSample()`** | Retorna uma amostra aleatória de tamanho fixo. |

##### 2. Ações de Contagem e Estatística

Utilizadas para obter métricas rápidas sobre a distribuição e volume dos dados.

* **`count()`**: Retorna o número total de linhas/elementos.
* **`describe(*cols)`**: Computa estatísticas básicas (média, desvio padrão, min, max) para colunas numéricas.
* **`summary(*statistics)`**: Uma versão mais flexível do `describe`, permitindo escolher os percentis.
* **`countByKey()`**: (Exclusivo RDD) Conta o número de elementos para cada chave.
* **`countByValue()`**: Retorna a contagem de cada valor único no dataset.

##### 3. Ações de Processamento e Agregação

Estas ações realizam cálculos matemáticos ou lógicos sobre os dados distribuídos.

* **`reduce(f)`**: Agrega os elementos usando uma função binária (ex: soma todos os valores).
* **`fold(zeroValue, op)`**: Semelhante ao `reduce`, mas utiliza um valor inicial (*zeroValue*).
* **`aggregate(zeroValue, seqOp, combOp)`**: Permite retornar um tipo de dado diferente do original através de funções de sequência e combinação.
* **`isEmpty()`**: Retorna `True` se o DataFrame/RDD não contiver registros.

##### 4. Ações de Escrita e Armazenamento

Essas ações persistem os resultados em sistemas de arquivos, bancos de dados ou tópicos de streaming.

* **`df.write.save(path)`**: Ação genérica para salvar um DataFrame.
* **`df.write.csv/parquet/json/orc(path)`**: Atalhos para salvar em formatos específicos.
* **`saveAsTextFile(path)`**: (RDD) Grava o conteúdo como arquivos de texto.
* **`saveAsTable(tableName)`**: Grava o DataFrame como uma tabela no catálogo do Spark (Hive Metastore).
* **`saveAsPickleFile(path)`**: Salva o RDD usando a serialização Python Pickle.

##### 5. Ações de Iteração (Side Effects)

Usadas quando você precisa executar uma função para cada registro, geralmente para interagir com serviços externos (ex: enviar um log ou e-mail).

* **`foreach(f)`**: Aplica a função `f` a cada linha do dataset.
* **`foreachPartition(f)`**: Aplica a função `f` a cada partição. É muito mais eficiente que o `foreach` comum para abrir conexões com bancos de dados, pois abre uma conexão por partição em vez de uma por linha.

---

As transformações constroem o grafo (DAG), mas nada acontece até que uma dessas ações seja chamada:

1. **Transformations** (`map`, `filter`, `join`) → **Plano Lógico**
2. **Actions** (`count`, `collect`, `save`) → **Execução Física (Job)**

[Detailed PySpark Actions and Transformations Guide](https://www.youtube.com/watch?v=DLsHq8TTvaY)

---

## 3. Tipos de Dados e Esquemas
No Spark, o esquema de um DataFrame define as colunas e seus tipos de dados. É possível definir o esquema manualmente ou permitir que o Spark infira automaticamente a partir dos dados.

### 3.1. Tipos de Dados no Spark

O Spark suporta vários tipos de dados que podem ser usados para definir o esquema de um DataFrame. Aqui estão alguns dos tipos de dados mais comuns:

   1. **StringType**: Representa uma string.
   2. **IntegerType**: Representa um número inteiro.
   3. **LongType**: Representa um número inteiro longo.
   4. **FloatType**: Representa um número de ponto flutuante de precisão simples.
   5. **DoubleType**: Representa um número de ponto flutuante de precisão dupla.
   6. **BooleanType**: Representa um valor booleano (True ou False).
   7. **DateType**: Representa uma data (sem hora).
   8. **TimestampType**: Representa uma data e hora.
   9. **ArrayType**: Representa uma lista de elementos de um tipo específico.
   10. **StructType**: Representa um esquema que contém uma lista de campos (StructField).
   11. **MapType**: Representa um mapa de chaves e valores de tipos específicos.

Esses tipos de dados são definidos no módulo `pyspark.sql.types` e são usados para especificar o esquema de um DataFrame.

**Exemplo**
   ```sh
   touch exemplo-3.1.data-types.py

   ```

   **Código**
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

   # Inicializando a SparkSession
   spark = SparkSession.builder.appName("dataeng-exemplo-dataframe").getOrCreate()

   # Definindo o schema
   schema = StructType([
      StructField("ID", LongType(), True),
      StructField("NOME", StringType(), True),
      StructField("DATA_NASC", DateType(), True),
      StructField("CPF", StringType(), True),
      StructField("EMAIL", StringType(), True),
      StructField("CIDADE", StringType(), True),
      StructField("UF", StringType(), True)
   ])

   # Criando o dataframe utilizando o schema definido acima
   df = spark.read \
         .format("csv") \
         .option("compression", "gzip") \
         .option("sep", ";") \
         .option("header", True) \
         .load("./datasets-csv-clientes/clientes.csv.gz", schema=schema)

   # Mostrando o schema
   df.printSchema()

   # Mostrando as primeiras linhas do DataFrame
   df.show(10, False)

   ```

   O **StructType** é uma classe do módulo `pyspark.sql.types` usada para definir o esquema de um DataFrame no Apache Spark. Ele representa uma estrutura de dados composta por uma lista de campos, onde cada campo é definido por um objeto **StructField**. Os parâmetros que o **StructType** recebe incluem uma lista de **StructField**, onde cada **StructField** especifica o nome da coluna, o tipo de dado (como `StringType`, `IntegerType`, etc.) e se o campo pode conter valores nulos (`nullable`). Essa abordagem permite criar esquemas personalizados para leitura de dados estruturados, como arquivos CSV ou JSON.

## 4. Exercício 1
**Objetivo:** Definir um esquema personalizado, criar um DataFrame a partir de um arquivo JSON e aplicar uma algumas transformações e ações.

1. Crie o arquivo `data.json` com o seguinte conteúdo:
   ```json
   [
      { "nome": "João"     ,"idade": 25,"cidade": "São Paulo"     },
      { "nome": "Maria"    ,"idade": 30,"cidade": "Rio de Janeiro" },
      { "nome": "Pedro"    ,"idade": 35,"cidade": "Belo Horizonte" },
      { "nome": "Ana"      ,"idade": 28,"cidade": "Brasília"       },
      { "nome": "Lucas"    ,"idade": 22,"cidade": "Salvador"       },
      { "nome": "Mariana"  ,"idade": 27,"cidade": "Porto Alegre"   },
      { "nome": "Carlos"   ,"idade": 33,"cidade": "Fortaleza"      },
      { "nome": "Juliana"  ,"idade": 29,"cidade": "Recife"         },
      { "nome": "Rafael"   ,"idade": 31,"cidade": "Manaus"         },
      { "nome": "Isabela"  ,"idade": 26,"cidade": "Curitiba"       },
      { "nome": "Gustavo"  ,"idade": 24,"cidade": "Florianópolis"  },
      { "nome": "Laura"    ,"idade": 32,"cidade": "Goiania"        },
      { "nome": "Fernando" ,"idade": 23,"cidade": "Vitória"        },
      { "nome": "Camila"   ,"idade": 34,"cidade": "Natal"          },
      { "nome": "Diego"    ,"idade": 27,"cidade": "Cuiabá"         },
      { "nome": "Amanda"   ,"idade": 29,"cidade": "João Pessoa"    },
      { "nome": "Rodrigo"  ,"idade": 25,"cidade": "Aracaju"        },
      { "nome": "Larissa"  ,"idade": 30,"cidade": "Teresina"       },
      { "nome": "Thiago"   ,"idade": 28,"cidade": "Maceió"         },
      { "nome": "Patrícia" ,"idade": 26,"cidade": "Macapá"         },
      { "nome": "Henrique" ,"idade": 33,"cidade": "Boa Vista"      },
      { "nome": "Carolina" ,"idade": 31,"cidade": "Palmas"         },
      { "nome": "Renata"   ,"idade": 24,"cidade": "Rio Branco"     },
      { "nome": "Bruno"    ,"idade": 32,"cidade": "Porto Velho"    },
      { "nome": "Marina"   ,"idade": 55,"cidade": "São Luís"       },
      { "nome": "Carlota"  ,"idade": 45,"cidade": "Belém"          },
      { "nome": "Juliete"  ,"idade": 40,"cidade": "Boa Vista"      },
      { "nome": "Rafaela"  ,"idade": 41,"cidade": "Palmas"         },
      { "nome": "Isabel"   ,"idade": 46,"cidade": "Rio Branco"     },
      { "nome": "Augusto"  ,"idade": 44,"cidade": "Porto Velho"    },
      { "nome": "Laura"    ,"idade": 52,"cidade": "São Luís"       },
      { "nome": "Wilson"   ,"idade": 43,"cidade": "Belém"          },
      { "nome": "Beatriz"  ,"idade": 54,"cidade": "Macapá"         },
      { "nome": "Diogenes" ,"idade": 47,"cidade": "Maceió"         },
      { "nome": "Amanda"   ,"idade": 49,"cidade": "Teresina"       },
      { "nome": "Rodrigo"  ,"idade": 45,"cidade": "Aracaju"        },
      { "nome": "Larissa"  ,"idade": 50,"cidade": "João Pessoa"    },
      { "nome": "Thiago"   ,"idade": 48,"cidade": "Cuiabá"         },
      { "nome": "Patrícia" ,"idade": 46,"cidade": "Natal"          },
      { "nome": "Marta"    ,"idade": 53,"cidade": "Vitória"        },
      { "nome": "Emilia"   ,"idade": 51,"cidade": "Florianópolis"  },
      { "nome": "Jucilene" ,"idade": 44,"cidade": "Goiania"        },
      { "nome": "Marivalda","idade": 52,"cidade": "Curitiba"       }
   ]
   ```

2. Crie o script `exemplo-4-exercicio-1.py` que realiza as seguintes etapas:
   - Carrega o arquivo JSON de exemplo.
   - Aplica transformações para filtrar e agrupar dados.
   - Define um esquema personalizado para o DataFrame.
   - Exibe o resultado final das transformações.

   ```sh
   touch exemplo-4-exercicio-1.py

   ```

   **Código**
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.types import StructType, StructField, StringType, IntegerType

   # Iniciar uma sessão Spark
   spark = SparkSession.builder.appName("dataeng-pyspark").getOrCreate()

   # Esquema personalizado
   schema_custom = StructType([
      StructField("nome", StringType(), True),
      StructField("idade", IntegerType(), True),
      StructField("cidade", StringType(), True)
   ])

   df_json = spark.read.json("data.json", schema=schema_custom, multiLine=True)
   df_json.show(10, False)
   df_json.printSchema()

   # Transformações e ações
   df_result = df_json.filter(df_json["idade"] > 25).groupBy("cidade").count()

   # Mostrando o resultado
   df_result.show(10, False)
   df_result.printSchema()

   ```

3. Execute o script:
   ```sh
   python exemplo-4-exercicio-1.py

   ```

---
## 5. Desafio Bolsa Família

1. Baixe a base de pagamentos do programa **Novo Bolsa Família** referente a **Novembro de 2025**.
   - Baixe o arquivo `202511_NovoBolsaFamilia.csv` do [Portal da Transparência](https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia)
   - O dicionário de dados pode ser obtido [aqui](https://portaldatransparencia.gov.br/dicionario-de-dados/novo-bolsa-familia)
   - Salve o arquivo em um diretório chamado `data` na raiz do projeto
2. Obtenha as seguintes informações:
   - Lista dos primeiros 10 registros
   - Lista dos primeiros 10 beneficiários que receberam valor **maior** que R$ 1.000,00
   - Lista dos primeiros 10 beneficiários que receberam valor **maior** que R$ 2.000,00
   - Lista dos primeiros 10 beneficiários que moram no municipio de `BONITO` no estado da **Bahia**
   - Lista dos beneficiários que:
      * moram no municipio de `BONITO` no estado da **Bahia** 
      * receberam valor **maior** que R$ 2.000,00
   - Lista dos beneficiários que:
      * moram no municipio de `BONITO` no estado da **Bahia** 
      * receberam valor **menor** que R$ 2.000,00 
      * número do CPF esteja nulo
   - Lista dos beneficiários que:
      * moram em qualquer estado da região **Norte**
      * receberam valor **menor** que R$ 1.000,00 
      * número do CPF **não** esteja nulo
      * O primeiro nome do beneficiário seja **Marcelo**
      * O último nome do beneficiário seja **Barbosa**

### Código inicial
   ```python
   from pyspark.sql import SparkSession
   import pyspark.sql.functions as F

   spark = SparkSession.builder.appName("dataeng-bolsa-familia").getOrCreate()

   schema = """
      mes_competencia INT, 
      mes_referencia INT, 
      uf STRING, 
      codigo_municipio_siafi STRING, 
      nome_municipio STRING,
      cpf_favorecido STRING,
      nis_favorecido STRING,
      nome_favorecido STRING,
      valor_parcela STRING
   """

   df = spark.read \
      .format("csv") \
      .option("sep", ";") \
      .option("header", True) \
      .option("encoding", "ISO-8859-1") \
      .schema(schema) \
      .load("./202511_NovoBolsaFamilia.csv")

   df = df.withColumn(
      "valor_parcela", 
      F.regexp_replace(F.col("valor_parcela"), r"\.", "")
   ).withColumn(
      "valor_parcela", 
      F.regexp_replace(F.col("valor_parcela"), ",", ".") 
   ).withColumn(
      "valor_parcela", 
      F.col("valor_parcela").cast("decimal(10,2)")
   )

   df.show(10, truncate=False)
   df.printSchema()

   ```

---
## 6. Parabéns!
Parabéns por concluir o módulo! Você revisou os conceitos fundamentais de DataFrames no Apache Spark e praticou com transformações, ações e manipulação de esquemas.

---
## 7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.
