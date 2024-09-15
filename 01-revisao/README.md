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
As transformações no Spark são operações "lazy", ou seja, elas não são executadas até que uma ação seja chamada. Exemplos de transformações incluem `filter`, `select`, `groupBy`, enquanto ações incluem `show`, `count`, `collect`.

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
**Objetivo:** Criar um DataFrame a partir de um arquivo JSON, aplicar uma série de transformações e ações, e definir um esquema personalizado.

**Instruções:**
1. Crie o arquivo `data.json` com o seguinte conteúdo:
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

# Exemplo de script modulo1.py
df_json = spark.read.json("data.json")

# Transformações e ações
df_result = df_json.filter(df_json["idade"] > 25).groupBy("cidade").count()

# Mostrando o resultado
df_result.show()

# Esquema personalizado
schema_custom = StructType([
    StructField("cidade", StringType(), True),
    StructField("total", IntegerType(), True)
])

df_custom = spark.createDataFrame(df_result.rdd, schema=schema_custom)
df_custom.printSchema()
```

## 5. Parabéns!
Parabéns por concluir o módulo! Você revisou os conceitos fundamentais de DataFrames no Apache Spark e praticou com transformações, ações e manipulação de esquemas.

## 6. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.
