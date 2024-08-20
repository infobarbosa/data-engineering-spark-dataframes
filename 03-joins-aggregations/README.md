### Módulo 3: Operações de Junção e Agregação

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 3.1. Introdução
Neste módulo, vamos aprofundar nosso conhecimento em operações de junção e agregação no Apache Spark, explorando tipos de joins avançados, técnicas de agregação, e o uso de funções analíticas para cálculos mais sofisticados.

### 3.2. Tipos de Join Avançados
#### 3.2.1. Broadcast Join
O Broadcast Join é uma técnica eficiente para realizar joins quando uma das tabelas é pequena o suficiente para ser copiada para todos os nós de processamento.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-3").getOrCreate()

# Exemplo de DataFrames para join
df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "valor"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "desc"])

# Broadcast join
df_join = df1.join(broadcast(df2), "id")
df_join.show()
```

#### 3.2.2. Shuffle Join
O Shuffle Join é usado quando ambas as tabelas são grandes e precisam ser redistribuídas (shuffle) entre os nós para executar o join.

**Exemplo de código:**
```python
# Shuffle join
df_shuffle_join = df1.join(df2, "id")
df_shuffle_join.show()
```

### 3.3. Técnicas de Agregação
#### 3.3.1. groupBy
A operação `groupBy` permite agrupar os dados com base em uma ou mais colunas e aplicar funções agregadas.

**Exemplo de código:**
```python
# Exemplo de groupBy
df_grouped = df1.groupBy("valor").count()
df_grouped.show()
```

#### 3.3.2. Window Functions
As Window Functions permitem a execução de cálculos complexos que envolvem particionamento e ordenação de dados.

**Exemplo de código:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Definindo a janela de dados
window_spec = Window.partitionBy("valor").orderBy("id")

# Aplicando uma função de janela
df_window = df1.withColumn("row_number", row_number().over(window_spec))
df_window.show()
```

### 3.4. Exploração de Funções Analíticas e Agregações Complexas
As funções analíticas permitem a execução de cálculos que envolvem operações mais sofisticadas, como rank, dense_rank, lead e lag.

**Exemplo de código:**
```python
from pyspark.sql.functions import rank, dense_rank, lag

# Funções analíticas: rank, dense_rank, lag
df_analytic = df1.withColumn("rank", rank().over(window_spec))
df_analytic = df_analytic.withColumn("dense_rank", dense_rank().over(window_spec))
df_analytic = df_analytic.withColumn("lag", lag("id", 1).over(window_spec))
df_analytic.show()
```

### 3.5. Exercício Prático Avançado
**Objetivo:** Implementar operações de junção e agregação utilizando técnicas avançadas de joins, funções de janela e agregações complexas.

**Instruções:**
1. Clone o repositório do curso:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-3.git
   ```
2. Navegue até a pasta do módulo 3:
   ```bash
   cd dataeng-modulo-3
   ```
3. Execute o script `modulo3.py`, que realizará as seguintes etapas:
   - Implementação de Broadcast Join e Shuffle Join.
   - Aplicação de funções de janela (window functions).
   - Uso de funções analíticas como rank, dense_rank, e lag.

**Código do laboratório:**
```python
# Exemplo de script modulo3.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, row_number, rank, dense_rank, lag
from pyspark.sql.window import Window

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-3").getOrCreate()

# Criando DataFrames de exemplo
df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "valor"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "desc"])

# Broadcast join
df_broadcast_join = df1.join(broadcast(df2), "id")
df_broadcast_join.show()

# Shuffle join
df_shuffle_join = df1.join(df2, "id")
df_shuffle_join.show()

# GroupBy com agregação
df_grouped = df1.groupBy("valor").count()
df_grouped.show()

# Definindo uma janela de dados
window_spec = Window.partitionBy("valor").orderBy("id")

# Aplicando funções de janela
df_window = df1.withColumn("row_number", row_number().over(window_spec))
df_window = df_window.withColumn("rank", rank().over(window_spec))
df_window = df_window.withColumn("dense_rank", dense_rank().over(window_spec))
df_window = df_window.withColumn("lag", lag("id", 1).over(window_spec))
df_window.show()

# Encerrando a SparkSession
spark.stop()
```

### 3.6. Parabéns!
Parabéns por concluir o módulo 3! Agora você domina operações de junção e agregação avançadas no Apache Spark, incluindo o uso de funções analíticas e agregações complexas.

### 3.7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-3/
│
├── README.md
├── modulo3.py
```

Este é o conteúdo do módulo 3. Podemos seguir para o próximo módulo quando você estiver pronto!