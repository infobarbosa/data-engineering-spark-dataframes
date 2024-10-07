# Manipulação Avançada de DataFrame - Pivot e Unpivot

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução
Neste módulo, exploraremos técnicas avançadas de manipulação de DataFrames no Apache Spark. Abordaremos transformações complexas como pivot e unpivot.
O pivot transforma valores únicos de uma coluna em múltiplas colunas, enquanto o unpivot faz o processo inverso.

## 2. Pivot

![pivot](pivot-unpivot.png)



```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("DesafioPySpark").getOrCreate()

from pyspark.sql.functions import explode, col

# Exemplo de DataFrame com arrays e structs
data = [
    ("João", [{"curso": "MATEMATICA", "nota": 85}, {"curso": "HISTORIA", "nota": 90}]),
    ("Maria", [{"curso": "MATEMATICA", "nota": 95}, {"curso": "HISTORIA", "nota": 80}])
]
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("cursos", ArrayType(StructType([
        StructField("curso", StringType(), True),
        StructField("nota", IntegerType(), True)
    ])), True)
])
df = spark.createDataFrame(data, schema)

df.show( truncate=False)

# Explodindo o array para linhas individuais
df_exploded = df.withColumn("curso", explode(df["cursos"]))
df = df_exploded.select("nome", col("curso.curso"), col("curso.nota"))

df.show()

print('Exemplo de Pivot')
df_pivot = df.groupBy("nome").pivot("curso").agg({"nota": "max"})
df_pivot.show()


```

**Dataframe original:**
```
+-----+----------+----+
| nome|     curso|nota|
+-----+----------+----+
| João|MATEMATICA|  85|
| João|  HISTORIA|  90|
|Maria|MATEMATICA|  95|
|Maria|  HISTORIA|  80|
+-----+----------+----+
```

Output esperado após o pivot:
```
+-----+--------+----------+
| nome|HISTORIA|MATEMATICA|
+-----+--------+----------+
| João|      90|        85|
|Maria|      80|        95|
+-----+--------+----------+
```

---

## 3. Unpivot

O processo de unpivot é utilizado para transformar colunas em linhas, o que pode ser útil para normalizar dados ou prepará-los para análises específicas. No PySpark, essa operação pode ser realizada utilizando a função `selectExpr` com a expressão `stack`.

**Exemplo de código:**

Considere o seguinte DataFrame:

```python
print('Exemplo de Unpivot (Requer manipulação manual no PySpark')
unpivoted = df_pivot.selectExpr("nome", "stack(2, 'Matematica', MATEMATICA, 'Historia', HISTORIA) as (curso, nota)")
unpivoted.show()

```

Output:
```

```

Para realizar o unpivot e transformar os trimestres em linhas, utilizamos a expressão `stack`. <br>
A sintaxe da função stack é:
```
stack(n, coluna1, valor1, coluna2, valor2, ..., colunaN, valorN)
```

- `n` é o número de colunas que você deseja empilhar.
- Cada par subsequente representa o nome que será atribuído na nova coluna e o valor correspondente.

Exemplo:
```
unpivot_expr = "stack(3, 'produto_A', produto_A, 'produto_B', produto_B, 'produto_C', produto_C) as (produto, valor)"
```

---

## 4. Desafio 1 - Pivot
Examine o dataset a seguir:
```
+-----------------+----+------+                                                 
|categoria_produto|ano |vendas|
+-----------------+----+------+
|Eletrônicos      |2022|1000  |
|Eletrônicos      |2023|1500  |
|Eletrônicos      |2024|400   |
|Móveis           |2022|700   |
|Móveis           |2023|800   |
|Móveis           |2024|200   |
|Vestuário        |2022|500   |
|Vestuário        |2023|600   |
|Vestuário        |2024|300   |
+-----------------+----+------+
```

Utilizando a função `pivot`, faça a transposição dos dados e obtenha a `soma` das vendas por `ano` agrupando pelo atributo `categoria_produto`.

Output esperado:
```
+-----------------+----+----+----+                                              
|categoria_produto|2022|2023|2024|
+-----------------+----+----+----+
|           Móveis| 700| 800| 200|
|      Eletrônicos|1000|1500| 400|
|        Vestuário| 500| 600| 300|
+-----------------+----+----+----+

```

**Código inicial**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Inicializando a sessão do Spark
spark = SparkSession.builder.appName("dataeng-pivot").getOrCreate()

data = [
    ("Eletrônicos", 2022, 1000),
    ("Eletrônicos", 2023, 1500),
    ("Eletrônicos", 2024, 400),
    ("Móveis", 2022, 700),
    ("Móveis", 2023, 800),
    ("Móveis", 2024, 200),
    ("Vestuário", 2022, 500),
    ("Vestuário", 2023, 600),
    ("Vestuário", 2024, 300)
]

# Definir as colunas do DataFrame
columns = ["categoria_produto", "ano", "vendas"]

# Criando o DataFrame com os dados fornecidos
df = spark.createDataFrame(data, columns)

# Utilizando a função `pivot`, faça a transposição dos dados e 
# obtenha a `soma` das vendas por `ano` agrupando pelo atributo `categoria_produto`.
df_pivot = ESCREVA SUA LOGICA AQUI 

# Mostrar o resultado
df_pivot.show()

```

---

## 5. Desafio 2 - Unpivot

Você recebeu um dataset contendo 5 colunas:
- Produto
- Vendas no 1o trimestre
- Vendas no 2o trimestre
- Vendas no 3o trimestre
- Vendas no 4o trimestre

DataFrame Original:
```
+----------+---+---+---+---+                                                    
|   Produto| Q1| Q2| Q3| Q4|
+----------+---+---+---+---+
| GELADEIRA|100|200|300|200|
| TELEVISAO|400|500|600|400|
|COMPUTADOR|700|800|900|950|
+----------+---+---+---+---+
```

Dado o script a seguir, elabore a expressão `unpivot_expr` de forma que um novo dataframe contendo as colunas:
- Produto
- Trimestre (Q1, Q2, Q3, Q4)
- Vendas

Resultado esperado:
```
+----------+---------+------+
|   Produto|Trimestre|Vendas|
+----------+---------+------+
| GELADEIRA|       Q1|   100|
| GELADEIRA|       Q2|   200|
| GELADEIRA|       Q3|   300|
| GELADEIRA|       Q4|   200|
| TELEVISAO|       Q1|   400|
| TELEVISAO|       Q2|   500|
| TELEVISAO|       Q3|   600|
| TELEVISAO|       Q4|   400|
|COMPUTADOR|       Q1|   700|
|COMPUTADOR|       Q2|   800|
|COMPUTADOR|       Q3|   900|
|COMPUTADOR|       Q4|   950|
+----------+---------+------+
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Criar sessão do Spark
spark = SparkSession.builder.appName("dataeng-unpivot").getOrCreate()

# Dados de exemplo em um DataFrame
data = [
    ("GELADEIRA" , 100, 200, 300, 200),
    ("TELEVISAO" , 400, 500, 600, 400),
    ("COMPUTADOR", 700, 800, 900, 950)
]

columns = ["Produto", "Q1", "Q2", "Q3", "Q4"]

# Criar DataFrame
df = spark.createDataFrame(data, columns)

# Exibir DataFrame original
print("DataFrame Original:")
df.show()

# Usar selectExpr para simular unpivot (melt)
unpivot_expr = """
DESENVOLVA A EXPRESSAO AQUI!
"""

# Realizando unpivot
df_unpivot = df.select("Produto", expr(unpivot_expr))

# Exibir DataFrame após unpivot
print("DataFrame Após Unpivot:")
df_unpivot.show()

```

## 6. Parabéns!
Parabéns por concluir o módulo! Você aprendeu técnicas de manipulação de DataFrames no Apache Spark, aplicando as operações **pivot** e **unpivot**.

## 7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

