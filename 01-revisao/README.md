# Módulo 1: Revisão dos Conceitos Básicos de DataFrame

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Neste módulo, vamos revisar os conceitos fundamentais do Apache Spark relacionados ao uso de DataFrames. Esta base teórica é essencial para os próximos módulos do curso, onde abordaremos tópicos mais avançados.

## 2. Conceitos Fundamentais de DataFrame
### 2.1. Criação de DataFrames
Os DataFrames são estruturas de dados distribuídas, imutáveis e organizadas em colunas nomeadas. No Spark, você pode criar DataFrames de várias fontes, como arquivos CSV, JSON, Parquet, tabelas SQL, entre outros.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-1").getOrCreate()

# Criando um DataFrame a partir de um arquivo CSV
df = spark.read.csv("s3://bucket/data.csv", header=True, inferSchema=True)

# Mostrando as primeiras linhas do DataFrame
df.show()
```

### 2.2. Transformações e Ações
As transformações no Spark são operações "lazy", ou seja, elas não são executadas até que uma ação seja chamada. <br>
Exemplos de transformações incluem `filter`, `select`, `groupBy`, enquanto ações incluem `show`, `count`, `collect`.

**Exemplo de código:**
```python
# Transformação: Filtrando linhas onde a coluna 'idade' é maior que 30
df_filtered = df.filter(df["idade"] > 30)

# Ação: Contando o número de linhas resultantes
total = df_filtered.count()
print(f"Total de pessoas com mais de 30 anos: {total}")
```

## 3. Revisão dos Tipos de Dados e Esquemas
No Spark, o esquema de um DataFrame define as colunas e seus tipos de dados. É possível definir o esquema manualmente ou permitir que o Spark infira automaticamente a partir dos dados.

**Exemplo de código:**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Definindo o esquema manualmente
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("cidade", StringType(), True)
])

# Criando um DataFrame com o esquema definido
df_manual = spark.read.csv("s3://bucket/data.csv", schema=schema, header=True)

# Mostrando o esquema do DataFrame
df_manual.printSchema()
```

## 4. Exercício 1
**Objetivo:** Definir um esquema personalizado, criar um DataFrame a partir de um arquivo JSON e aplicar uma algumas transformações e ações.

**Instruções:**
1. Instale o `pyspark`:
   ```sh
   pip install pyspark

   ```

2. Crie o arquivo `data.json` com o seguinte conteúdo:
   ```json
   [
      { "nome": "João"     ,"idade": 25, "cidade": "São Paulo"     },
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

3. Crie o script `modulo1.py` que realiza as seguintes etapas:
   - Carrega o arquivo JSON de exemplo.
   - Aplica transformações para filtrar e agrupar dados.
   - Define um esquema personalizado para o DataFrame.
   - Exibe o resultado final das transformações.

   **Código do laboratório:**
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
   df_json.show()

   # Transformações e ações
   df_result = df_json.filter(df_json["idade"] > 25).groupBy("cidade").count()

   # Mostrando o resultado
   df_result.show()

   df_result.printSchema()
   ```

4. Execute o script `modulo1.py`:
   ```sh
   python modulo1.py 

   ```

## 5. Desafio

Faça o clone do dataset `clientes.csv.gz`
```sh
git clone https://github.com/infobarbosa/datasets-csv-clientes
```

Dado o arquivo clientes.csv.gz com o seguinte leiaute

- Separador: ";"
- Header: True
- Compressão: gzip

### Atributos
| Atributo        | Tipo      | Obs                                               |
| ---             | ---       | ---                                               |
| ID              | long      | O identificador da pessoa                         |
| NOME            | string    | O nome da pessoa                                  |
| DATA_NASC       | date      | A data de nascimento da pessoa                    |
| CPF             | string    | O CPF da pessoa                                   |
| EMAIL           | string    | O email da pessoa                                 |

### Amostra

```
id;nome;data_nasc;cpf;email
1;Isabelly Barbosa;1963-08-15;137.064.289-03;isabelly.barbosa@example.com<br>
2;Larissa Fogaça;1933-09-29;703.685.294-10;larissa.fogaca@example.com<br>
3;João Gabriel Silveira;1958-05-27;520.179.643-52;joao.gabriel.silveira@example.com<br>
4;Pedro Lucas Nascimento;1950-08-23;274.351.896-00;pedro.lucas.nascimento@example.com<br>
5;Felipe Azevedo;1986-12-31;759.061.842-01;felipe.azevedo@example.com<br>
6;Ana Laura Lopes;1963-04-27;165.284.390-60;ana.laura.lopes@example.com<br>
7;Ana Beatriz Aragão;1958-04-21;672.135.804-26;ana.beatriz.aragao@example.com<br>
8;Murilo da Rosa;1944-07-13;783.640.251-71;murilo.da.rosa@example.com<br>
9;Alícia Souza;1960-08-26;784.563.029-29;alicia.souza@example.com<br>
```

Elabore o script pyspark para abrir esse arquivo e filtrar todos os clientes com mais de 50 anos agrupando por ano de nascimento.

### Dica 1
Para resolver o desafio, você pode utilizar a função `year` do pacote `pyspark.sql.functions` para extrair o ano de nascimento dos clientes. <br>
Exemplo:

```python
# Filtrar clientes com mais de 25 anos
df_filtrado = df_clientes.filter(year("data_nasc") <= 1996)
```

### Dica 2
Para resolver o desafio, você pode adicionar uma coluna `IDADE` ao DataFrame para facilitar o filtro dos clientes com mais de 50 anos. <br>
A função `withColumn` serve para adicionar novas colunas ao DataFrame.

Exemplo:

```python
# Adicionar coluna IDADE
df_clientes = df_clientes.withColumn("IDADE", 2023 - year(col("data_nasc")))
```

### Dica 3
Para calcular a idade com base na data atual, você pode utilizar as funções `current_date` e `datediff` do pacote `pyspark.sql.functions`. <br>
Exemplo:

```python
from pyspark.sql.functions import current_date, datediff

# Adicionar coluna IDADE calculada com base na data atual
df_clientes = df_clientes.withColumn("IDADE", (datediff(current_date(), col("data_nasc")) / 365.25).cast("int"))
```

Neste exemplo, a função `year` é usada para extrair o ano da coluna `data_nasc`, permitindo que você agrupe os dados por ano de nascimento.

## 6. Parabéns!
Parabéns por concluir o módulo! Você revisou os conceitos fundamentais de DataFrames no Apache Spark e praticou com transformações, ações e manipulação de esquemas.

## 7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.
