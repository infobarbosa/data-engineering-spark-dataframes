### Módulo 6: Tuning e Configuração de Performance

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 6.1. Introdução
No módulo 6, vamos explorar as melhores práticas e configurações para otimizar a performance de DataFrames no Apache Spark. Abordaremos ajustes de configurações de Spark, ajustes de parâmetros de memória, CPU e paralelismo, e a utilização do Spark UI para troubleshooting e monitoramento de performance.

### 6.2. Configurações de Spark para Otimização de DataFrame
#### 6.2.1. Configurações Gerais de Spark
O Apache Spark oferece uma variedade de configurações que podem ser ajustadas para otimizar o desempenho de processamento de DataFrames. As principais configurações incluem ajustes de paralelismo, particionamento e memória.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession

# Inicializando a SparkSession com configurações customizadas
spark = SparkSession.builder \
    .appName("dataeng-modulo-6") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Exemplo de operação com DataFrame
df = spark.read.parquet("s3://bucket_name/data/parquet/")
df.show()
```

### 6.3. Ajuste de Parâmetros de Memória, CPU e Paralelismo
#### 6.3.1. Ajuste de Memória e CPU
Ajustar a alocação de memória e CPU entre executores e o driver é essencial para melhorar a performance em cenários de processamento intensivo.

**Exemplo de código:**
```python
# Configuração de memória e CPU para executores
spark = SparkSession.builder \
    .appName("dataeng-modulo-6") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Realizando uma operação pesada em um DataFrame
df = spark.read.parquet("s3://bucket_name/data/parquet/")
df.groupBy("column").sum().show()
```

#### 6.3.2. Paralelismo e Particionamento
O ajuste do paralelismo e particionamento impacta diretamente a eficiência do processamento distribuído no Spark.

**Exemplo de código:**
```python
# Ajustando o número de partições para operações de shuffle
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Exemplo de uma operação de join com alto volume de dados
df1 = spark.read.parquet("s3://bucket_name/data1/parquet/")
df2 = spark.read.parquet("s3://bucket_name/data2/parquet/")

df_join = df1.join(df2, "id")
df_join.show()
```

### 6.4. Troubleshooting e Monitoramento de Performance com Spark UI
#### 6.4.1. Introdução ao Spark UI
O Spark UI é uma ferramenta poderosa para monitorar e diagnosticar a performance das aplicações Spark. Ele oferece visualizações detalhadas de tarefas, estágios, execuções de jobs, e permite a identificação de gargalos.

**Exemplo de acesso ao Spark UI:**
```python
# O Spark UI pode ser acessado via o seguinte URL
# http://<driver-node>:4040
```

#### 6.4.2. Análise de Planos de Execução e Performance
Utilize o Spark UI para inspecionar os planos de execução física e entender como o Spark está processando as tarefas.

**Exemplo de uso do `explain`:**
```python
# Analisando o plano de execução de uma operação complexa
df = spark.read.parquet("s3://bucket_name/data/parquet/")
df.groupBy("column").sum().explain(True)
```

### 6.5. Exercício Prático Avançado
**Objetivo:** Aplicar técnicas de tuning e configuração de performance no Spark, ajustando parâmetros de memória, CPU, e paralelismo, além de realizar monitoramento utilizando o Spark UI.

**Instruções:**
1. Clone o repositório do curso:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-6.git
   ```
2. Navegue até a pasta do módulo 6:
   ```bash
   cd dataeng-modulo-6
   ```
3. Execute o script `modulo6.py`, que:
   - Ajustará configurações de memória, CPU, e paralelismo.
   - Executará operações complexas de DataFrame para análise de performance.
   - Realizará monitoramento e troubleshooting via Spark UI.

**Código do laboratório:**
```python
# Exemplo de script modulo6.py

from pyspark.sql import SparkSession

# Inicializando a SparkSession com ajustes de performance
spark = SparkSession.builder \
    .appName("dataeng-modulo-6") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Leitura de dados e operação de groupBy para análise de performance
df = spark.read.parquet("s3://bucket_name/data/parquet/")
df_grouped = df.groupBy("column").sum()
df_grouped.show()

# Analisando o plano de execução da operação
df_grouped.explain(True)

# Finalizando a sessão Spark
spark.stop()
```

### 6.6. Parabéns!
Parabéns por concluir o módulo 6! Agora você está apto a realizar tuning e configurações avançadas de performance no Apache Spark.

### 6.7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

