# User Defined Aggregate Functions (UDAF)

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

## 1. Introdução

UDAFs permitem a criação de agregações personalizadas que podem ser aplicadas em grupos de dados. Isso é útil para cálculos complexos que não são possíveis com funções agregadas padrão.

**Exemplo de código:**
Neste exemplo, vamos criar uma Função Agregada Definida pelo Usuário (UDAF) no PySpark que calcula o produto dos valores em cada grupo de dados.

```python
### 1. Importe as bibliotecas necessárias:
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

###  Inicialize o SparkSession:
spark = SparkSession.builder.appName("Exemplo UDAF").getOrCreate()

### 3. Crie um DataFrame de exemplo:
data = [("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5)]
df = spark.createDataFrame(data, ["grupo", "valor"])

### 4. Defina a UDAF:
@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def produto_udaf(v: pd.Series) -> float:
    return v.prod()

### 5. Use a UDAF no agrupamento:
resultado_df = df.groupby("grupo").agg(produto_udaf(df.valor).alias("produto"))

### 6. Mostre os resultados:
resultado_df.show()
```

**Saída esperada:**

```
+------+-------+
| grupo|produto|
+------+-------+
|     B|   20.0|
|     A|    6.0|
+------+-------+
```

**Explicação:**

- **produto_udaf**: Esta função calcula o produto de uma série de valores dentro de cada grupo.
- **@pandas_udf**: Decorador que define uma função UDAF usando Pandas UDFs.
  - `"double"`: Tipo de dado de retorno da função.
  - `PandasUDFType.GROUPED_AGG`: Indica que a função é uma agregação agrupada.
- **df.groupby("grupo")**: Agrupa o DataFrame pelo campo "grupo".
- **agg(...)**: Aplica a função agregada definida ao grupo.


## 2. Parabéns!
Parabéns por concluir o módulo de Funções Agregadas Definidas pelo Usuário (UDAFs) no PySpark! 

## 6. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

