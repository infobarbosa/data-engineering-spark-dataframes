from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Create DataFrame from JSON") \
    .getOrCreate()

# Define the JSON data
json_data = [
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
    { "nome": "Patrícia" ,"idade": 26,"cidade": "Macapá"         }
]

# Define the schema
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("cidade", StringType(), True)
])

# Create a DataFrame from the JSON data with the defined schema
df = spark.createDataFrame(json_data, schema)

# Show the DataFrame
df.show()
