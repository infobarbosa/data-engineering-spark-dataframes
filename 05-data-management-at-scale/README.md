### Módulo 5: Gerenciamento de Dados em Grande Escala

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 5.1. Introdução
No módulo 5, exploraremos as práticas recomendadas para o gerenciamento de grandes volumes de dados no Apache Spark. Abordaremos o trabalho com dados particionados em formatos como Parquet e ORC, o controle de evolução de esquemas (schema evolution), e técnicas de otimização para leitura e escrita eficiente de grandes volumes de dados.

### 5.2. Trabalhando com Dados Particionados
#### 5.2.1. Formato Parquet
Parquet é um formato de armazenamento em coluna que oferece compressão eficiente e desempenho otimizado para leitura de dados.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-5").getOrCreate()

# Carregando um DataFrame particionado em formato Parquet
df_parquet = spark.read.parquet("s3://bucket_name/data/parquet/")
df_parquet.show()

# Salvando um DataFrame em formato Parquet com particionamento
df_parquet.write.mode("overwrite").partitionBy("column1").parquet("s3://bucket_name/output/parquet/")
```

#### 5.2.2. Formato ORC
ORC é outro formato de armazenamento em coluna, otimizado para leitura e escrita de grandes volumes de dados com compressão eficiente.

**Exemplo de código:**
```python
# Carregando um DataFrame particionado em formato ORC
df_orc = spark.read.orc("s3://bucket_name/data/orc/")
df_orc.show()

# Salvando um DataFrame em formato ORC com particionamento
df_orc.write.mode("overwrite").partitionBy("column1").orc("s3://bucket_name/output/orc/")
```

### 5.3. Controle de Schema Evolution
#### 5.3.1. Evolução de Esquema
Schema Evolution refere-se à capacidade de modificar o esquema de um DataFrame ou tabela ao longo do tempo sem perder compatibilidade com dados antigos.

**Exemplo de código:**
```python
# Carregando um DataFrame com suporte a Schema Evolution
df_schema_evolution = spark.read.option("mergeSchema", "true").parquet("s3://bucket_name/data/parquet/")
df_schema_evolution.printSchema()
df_schema_evolution.show()
```

**Alterando e Salvando um DataFrame com Novo Esquema:**
```python
# Criando um novo DataFrame com colunas adicionais
from pyspark.sql import Row

new_data = [Row(id=1, name="Alice", age=30, country="US"),
            Row(id=2, name="Bob", age=25, country="UK")]
df_new = spark.createDataFrame(new_data)

# Salvando o novo DataFrame com o esquema atualizado
df_new.write.mode("append").parquet("s3://bucket_name/data/parquet/")
```

### 5.4. Otimização de Leitura e Escrita de Grandes Volumes de Dados
#### 5.4.1. Leitura Otimizada
A leitura de grandes volumes de dados pode ser otimizada utilizando técnicas como pushdown de filtros e leitura seletiva de colunas.

**Exemplo de código:**
```python
# Leitura otimizada com pushdown de filtros e seleção de colunas
df_optimized_read = spark.read.parquet("s3://bucket_name/data/parquet/") \
    .select("column1", "column2") \
    .filter("column1 > 100")

df_optimized_read.show()
```

#### 5.4.2. Escrita Otimizada
A escrita de grandes volumes de dados pode ser otimizada utilizando compressão, particionamento e ajuste do número de arquivos de saída.

**Exemplo de código:**
```python
# Escrita otimizada com compressão e particionamento
df_optimized_write = df_optimized_read.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("column1") \
    .parquet("s3://bucket_name/output/parquet/")

df_optimized_write.show()
```

### 5.5. Exercício Prático Avançado
**Objetivo:** Aplicar técnicas avançadas para gerenciar dados em grande escala, incluindo o uso de formatos de dados particionados, controle de schema evolution e otimização de leitura/escrita.

**Instruções:**
1. Clone o repositório do curso:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-5.git
   ```
2. Navegue até a pasta do módulo 5:
   ```bash
   cd dataeng-modulo-5
   ```
3. Execute o script `modulo5.py`, que:
   - Carregará e manipulará dados em formatos Parquet e ORC.
   - Demonstrará o controle de schema evolution.
   - Implementará técnicas de otimização de leitura e escrita.

**Código do laboratório:**
```python
# Exemplo de script modulo5.py

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Inicializando a SparkSession
spark = SparkSession.builder.appName("dataeng-modulo-5").getOrCreate()

# Carregando dados em formato Parquet
df_parquet = spark.read.parquet("s3://bucket_name/data/parquet/")
df_parquet.show()

# Carregando dados em formato ORC
df_orc = spark.read.orc("s3://bucket_name/data/orc/")
df_orc.show()

# Adicionando novos dados com esquema evoluído
new_data = [Row(id=1, name="Alice", age=30, country="US"),
            Row(id=2, name="Bob", age=25, country="UK")]
df_new = spark.createDataFrame(new_data)

# Salvando o DataFrame com o novo esquema
df_new.write.mode("append").parquet("s3://bucket_name/data/parquet/")

# Leitura otimizada com pushdown de filtros e seleção de colunas
df_optimized_read = spark.read.parquet("s3://bucket_name/data/parquet/") \
    .select("id", "name") \
    .filter("age > 20")

df_optimized_read.show()

# Escrita otimizada com compressão e particionamento
df_optimized_read.write.mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("country") \
    .parquet("s3://bucket_name/output/parquet/")

# Encerrando a SparkSession
spark.stop()
```

### 5.6. Parabéns!
Parabéns por concluir o módulo 5! Agora você domina as práticas recomendadas para o gerenciamento de dados em grande escala no Apache Spark.

### 5.7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-5/
│
├── README.md
├── modulo5.py
```
