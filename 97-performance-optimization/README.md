### Módulo 4: Otimização e Performance

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 4.1. Introdução
Este módulo aborda técnicas avançadas de otimização e performance em Apache Spark, fundamentais para trabalhar com grandes volumes de dados de forma eficiente. Veremos os princípios do Catalyst Optimizer, o Tungsten engine, análise de planos de execução com `explain()`, cache/persistência de DataFrames, e técnicas de particionamento e paralelismo.

### 4.2. Princípios de Otimização de Consultas
#### 4.2.1. Catalyst Optimizer
O Catalyst é o otimizador de consultas do Spark, responsável por transformar o código SQL e DataFrame em um plano de execução otimizado. Ele realiza diversas transformações como simplificações, reordenação de joins e pushdown de filtros.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-4").getOrCreate()

# Exemplo simples para demonstrar otimizações do Catalyst
df = spark.read.csv("path/to/data.csv", header=True, inferSchema=True)
df_filtered = df.filter(df['column1'] > 100).select("column2", "column3")
df_filtered.show()

# Análise do plano de execução
df_filtered.explain(True)
```

#### 4.2.2. Tungsten
O Tungsten é um motor de processamento de execução do Spark focado em otimização de memória e CPU. Ele melhora o uso de hardware para executar as tarefas mais rápido, utilizando técnicas como código gerado, gerenciamento eficiente de memória, e compactação de dados.

### 4.3. Uso do explain() para Analisar Planos de Execução
O método `explain()` permite visualizar o plano de execução de uma query ou operação de DataFrame, mostrando como o Spark planeja executar a tarefa. Analisar esses planos ajuda a identificar gargalos de performance.

**Exemplo de código:**
```python
# Continuando o exemplo anterior
df_filtered.explain(True)  # Exibe um plano detalhado de execução
```

### 4.4. Técnicas de Cache e Persistência de DataFrames
#### 4.4.1. Cache
O método `cache()` armazena o DataFrame em memória, acelerando operações subsequentes que utilizam o mesmo DataFrame.

**Exemplo de código:**
```python
# Cacheando o DataFrame
df_cached = df_filtered.cache()
df_cached.show()

# A análise com cache
df_cached.explain(True)
```

#### 4.4.2. Persistência
Além de `cache()`, o método `persist()` permite armazenar o DataFrame em diferentes níveis de armazenamento, como memória e disco.

**Exemplo de código:**
```python
# Persistindo o DataFrame no disco
df_persisted = df_filtered.persist()
df_persisted.show()

# Análise com persistência
df_persisted.explain(True)
```

### 4.5. Particionamento e Paralelismo Eficiente
#### 4.5.1. Particionamento
Particionar dados permite distribuí-los de forma eficiente entre os nós do cluster, otimizando a paralelização das operações.

**Exemplo de código:**
```python
# Reparticionando o DataFrame para aumentar o paralelismo
df_repartitioned = df.repartition(10, "column2")
df_repartitioned.explain(True)
```

#### 4.5.2. Paralelismo
O Spark permite ajustar o nível de paralelismo, que define o número de tarefas que podem ser executadas simultaneamente.

**Exemplo de código:**
```python
# Definindo o número de partitions para leitura
df_parallel = spark.read.csv("path/to/data.csv", header=True, inferSchema=True).repartition(20)
df_parallel.explain(True)
```

### 4.6. Exercício Prático Avançado
**Objetivo:** Aplicar técnicas de otimização e performance em um pipeline de dados, usando Catalyst Optimizer, caching, particionamento e análise de planos de execução.

**Instruções:**
1. Clone o repositório do curso:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-4.git
   ```
2. Navegue até a pasta do módulo 4:
   ```bash
   cd dataeng-modulo-4
   ```
3. Execute o script `modulo4.py`, que irá:
   - Carregar um grande conjunto de dados.
   - Otimizar consultas usando Catalyst e Tungsten.
   - Implementar cache/persistência de DataFrames.
   - Analisar o plano de execução usando `explain()`.

**Código do laboratório:**
```python
# Exemplo de script modulo4.py

from pyspark.sql import SparkSession

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-4").getOrCreate()

# Carregando um grande conjunto de dados
df = spark.read.csv("path/to/large_data.csv", header=True, inferSchema=True)

# Otimizações com Catalyst (filtros, projeções)
df_optimized = df.filter(df['column1'] > 100).select("column2", "column3")
df_optimized.explain(True)

# Cacheando o DataFrame para otimizar performance
df_cached = df_optimized.cache()
df_cached.show()

# Persistindo o DataFrame em memória e disco
df_persisted = df_optimized.persist()
df_persisted.show()

# Particionamento para paralelismo eficiente
df_repartitioned = df.repartition(10, "column2")
df_repartitioned.explain(True)

# Análise final do plano de execução
df_final = df_repartitioned.filter(df_repartitioned['column3'] < 50)
df_final.explain(True)

# Encerrando a SparkSession
spark.stop()
```

### 4.7. Parabéns!
Parabéns por concluir o módulo 4! Agora você possui habilidades avançadas para otimizar consultas e melhorar a performance de pipelines de dados no Apache Spark.

### 4.8. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-4/
│
├── README.md
├── modulo4.py
```
