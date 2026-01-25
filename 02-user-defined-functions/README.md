# User Defined Function (UDF)

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução

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

## 3. Desafio 1

**Desafio PySpark - Uso de UDF (User Defined Function)**

Crie uma UDF que, dado o nome de uma pessoa e sua data de nascimento, retorne uma saudação personalizada informando a idade atual da pessoa. Adicione essa saudação como uma nova coluna no DataFrame.

**Código inicial:**

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from datetime import datetime

spark = SparkSession.builder.appName("dataeng-exemplo-dataframe").getOrCreate()

schema = "id INT, nome STRING, data_nasc DATE, cpf STRING, email STRING, cidade STRING, uf STRING"
df = spark.read \
   .format("csv") \
   .option("sep", ";") \
   .option("header", True) \
   .schema(schema) \
   .load("./datasets-csv-clientes/clientes.csv.gz")

##############################################
# Seu código aqui para definir e aplicar a UDF
##############################################


```

**Instruções:**

- Implemente uma UDF que calcule a idade da pessoa com base na data de nascimento.
- A UDF deve retornar uma saudação no formato: `"Olá, {nome}! Você tem {idade} anos."`
- Adicione essa saudação como uma nova coluna chamada `"saudacao"` no DataFrame.

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
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from datetime import datetime

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

# Definindo a UDF para calcular a idade e criar a saudação
def saudacao_personalizada(nome, data_nasc):

    # Obtendo a data atual
    data_atual = datetime.now().date()
    # Calculando a idade
    idade = data_atual.year - data_nasc.year - ((data_atual.month, data_atual.day) < (data_nasc.month, data_nasc.day))
    # Criando a saudação
    saudacao = f"Olá, {nome}! Você tem {idade} anos."
    return saudacao

# Registrando a UDF
saudacao_udf = F.udf(saudacao_personalizada, StringType())

# Aplicando a UDF ao DataFrame
df_saudacao = df.withColumn("saudacao", saudacao_udf(df.nome, df.data_nasc)).select("id", "nome", "data_nasc", "saudacao")

# Mostrando as primeiras linhas do DataFrame
df_saudacao.show(truncate=False)

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
- A função recebe o `nome` e a `data_nasc` como parâmetros.
- Converte a `data_nasc` de `string` para um objeto `datetime`.
- Calcula a `idade` considerando se a pessoa já fez aniversário no ano atual.
- Retorna a saudação personalizada no formato desejado.

5. **Registrando a UDF:**
- Utilizamos `udf()` para registrar a função `saudacao_personalizada` como uma UDF do Spark, especificando que o tipo de retorno é `StringType()`.

6. **Aplicando a UDF ao DataFrame:**
- Usamos `withColumn()` para adicionar uma nova coluna `"saudacao"` ao DataFrame, aplicando a UDF aos campos `"nome"` e `"data_nascimento"`.

7. **Exibindo o DataFrame Resultante:**
- Utilizamos `df.show(truncate=False)` para mostrar o DataFrame completo sem truncar as colunas.


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
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime

spark = SparkSession.builder.appName("dataeng-udf").getOrCreate()

schema = "id INT, nome STRING, data_nasc DATE, cpf STRING, email STRING, cidade STRING, uf STRING"
df = spark.read \
   .format("csv") \
   .option("sep", ";") \
   .option("header", True) \
   .schema(schema) \
   .load("./datasets-csv-clientes/clientes.csv.gz")

# Definindo a UDF para calcular a idade e criar a saudação
@udf(StringType())
def saudacao_personalizada(nome, data_nasc):

    # Obtendo a data atual
    data_atual = datetime.now().date()
    # Calculando a idade
    idade = data_atual.year - data_nasc.year - ((data_atual.month, data_atual.day) < (data_nasc.month, data_nasc.day))
    # Criando a saudação
    saudacao = f"Olá, {nome}! Você tem {idade} anos."
    return saudacao

# Aplicando a UDF ao DataFrame
df_saudacao = df.withColumn("saudacao", saudacao_personalizada(df.nome, df.data_nasc)).select("id", "nome", "data_nasc", "saudacao")

# Mostrando as primeiras linhas do DataFrame
df_saudacao.show(truncate=False)


```

</details>

#### Solução 3 do desafio
<details>
  <summary>Clique aqui</summary>

**Solução 3 do Desafio PySpark - Pandas UDF**

Vamos implementar o calculo da idade e retornar a saudação personalizada, desta vez com Pandas UDF.

**Código completo:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from datetime import datetime
import pandas as pd

spark = SparkSession.builder.appName("dataeng-pandas-udf").getOrCreate()

schema = "id INT, nome STRING, data_nasc DATE, cpf STRING, email STRING, cidade STRING, uf STRING"
df = spark.read \
   .format("csv") \
   .option("sep", ";") \
   .option("header", True) \
   .schema(schema) \
   .load("./datasets-csv-clientes/clientes.csv.gz")

# Definindo a UDF para calcular a idade e criar a saudação
@pandas_udf(StringType())
def saudacao_personalizada(nome: pd.Series, data_nasc: pd.Series) -> pd.Series:
    data_nasc = pd.to_datetime(data_nasc, errors='coerce')
    data_atual = pd.to_datetime(datetime.now().date())

    # Cálculo da idade base
    idade = data_atual.year - data_nasc.dt.year

    # Ajuste caso o aniversário ainda não tenha ocorrido este ano
    aniversarios_ja_ocorreram = (
        (data_nasc.dt.month < data_atual.month) |
        ((data_nasc.dt.month == data_atual.month) & (data_nasc.dt.day <= data_atual.day))
    )
    idade_ajustada = idade.where(aniversarios_ja_ocorreram, idade - 1)

    idade_str = idade_ajustada.fillna(0).astype(int).astype(str)
    
    saudacao = "Olá, " + nome + "! Você tem " + idade_str + " anos."
    
    return saudacao

# Aplicando a UDF ao DataFrame
df_saudacao = df.withColumn("saudacao", saudacao_personalizada("nome", "data_nasc")).select("id", "nome", "data_nasc", "saudacao")

# Mostrando as primeiras linhas do DataFrame
df_saudacao.show(truncate=False)

``` 
</details>

#### Solução 4 do desafio
<details>
  <summary>Clique aqui</summary>

**Solução 4 do Desafio PySpark - Sem uso de UDF**

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


---
## 4. Desafio 2: Analisando Transações com Potencial de Fraude

Você recebeu um conjunto de dados de pedidos de uma plataforma de e-commerce que contém informações sobre cada pedido, incluindo seu status e se foi marcado como fraude ou não.

Cada registro no conjunto de dados é representado pelo seguinte formato JSON:
```json
[
    {"id_pedido": "89b90ce1-c12b-440e-a7db-ca43a4dd87ba", "status": true, "fraude": false},
    {"id_pedido": "aa18591a-e54d-4f84-bc20-ebbb653e016d", "status": false, "fraude": false},
    ...
]
```

### Objetivo:
O desafio é implementar uma UDF (User Defined Function) que verificará se um pedido é considerado de **alto risco**. Um pedido é considerado de alto risco se:
- O status for `false` (indicando que o pedido não foi processado corretamente).
- A flag de fraude for `true` (indicando que o pedido foi marcado como potencialmente fraudulento).

### Passos:
1. **Carregar os dados JSON no PySpark**: Carregue o conjunto de dados JSON fornecido utilizando a API de DataFrame do PySpark. Faça o clone do seguinte repositório:
```sh
git clone https://github.com/infobarbosa/dataset-json-pagamentos
```

2. **Criar a UDF**: Implemente uma função que será usada como UDF para identificar se um pedido é de alto risco.
3. **Aplicar a UDF no DataFrame**: Utilize a UDF para criar uma nova coluna no DataFrame chamada `risco_alto`, que será `True` se o pedido for de alto risco e `False` caso contrário.
4. **Filtrar pedidos de alto risco**: Filtre e exiba apenas os pedidos que são considerados de alto risco.

### Dicas:
- Você pode usar o método `.withColumn()` para adicionar uma nova coluna ao DataFrame.

### Exemplo de UDF:

Aqui está um exemplo básico de como criar e usar uma UDF:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

# Defina a função de alto risco
def pedido_alto_risco(LISTA_DE_PARAMETROS...):
    ADICIONE_SUA_LOGICA_AQUI

# Registre a função como UDF
pedido_alto_risco_udf = REGISTRE_A_UDF_AQUI

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("DesafioUDF").getOrCreate()

# Carregar o JSON em um DataFrame
df = spark.read.json("./dataset-json-pagamentos/pagamentos.json")
df.show(truncate=False)

# Aplicar a UDF para criar uma nova coluna 'risco_alto'
df_com_risco = df.withColumn("risco_alto", APLIQUE_A_UDF_AQUI)

# Filtrar os pedidos de alto risco
df_pedidos_risco_alto = df_com_risco.filter( ACRESCENTE_AQUI_A_LOGICA_DE_FILTRO )

# Exibir os resultados
df_pedidos_risco_alto.show()

```

---

## 5. Parabéns!
Parabéns! Nesse módulo você aprendeu sobre UDFs e como aplicar esse conhecimento em desafios práticos. <br> 
Continue assim e bons estudos!

## 6. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

