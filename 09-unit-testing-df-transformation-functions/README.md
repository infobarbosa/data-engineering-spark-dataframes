### Módulo 9: Testando Funções de Transformação de DataFrames

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 1. Introdução

Neste módulo, você aprenderá a testar funções de transformação em Spark DataFrame. Vamos explorar diferentes tipos de funções de transformação, estratégias para testar essas funções e como usar fixtures para preparar dados de teste.

### 2. Parte Teórica

#### 2.1. Tipos de Funções de Transformação em Spark DataFrame
As funções de transformação são usadas para alterar e manipular dados em um DataFrame. Aqui estão alguns tipos comuns:
- **Filtros:** Filtram linhas com base em condições (e.g., `filter`, `where`).
- **Seleções:** Selecionam colunas específicas (e.g., `select`).
- **Agregações:** Realizam cálculos agregados em grupos de dados (e.g., `groupBy`, `agg`).
- **Ordenação:** Ordenam dados com base em uma ou mais colunas (e.g., `orderBy`).
- **Transformações Complexas:** Incluem funções como `withColumn`, `drop`, `join`.

#### 2.2. Estratégias para Testar Funções de Transformação
Para garantir que suas funções de transformação funcionem corretamente, considere as seguintes estratégias:
- **Teste de Condições Limites:** Teste com dados que estão na fronteira das condições esperadas.
- **Teste de Casos de Erro:** Verifique como a função lida com dados incorretos ou ausentes.
- **Teste de Dados Mínimos e Máximos:** Garanta que a função funciona tanto com pequenos conjuntos de dados quanto com grandes volumes.

#### 2.3. Utilização de Fixtures para Preparar Dados de Teste
Fixtures são usadas para fornecer dados ou configurações para seus testes. No PySpark, você pode usar fixtures para criar DataFrames de teste:
- **Fixture Básica:** Prepara um DataFrame com dados básicos.
- **Fixture Avançada:** Cria DataFrames complexos com várias transformações pré-aplicadas.

### 3. Laboratório

#### 3.1. Escrever Testes para Funções de Transformação

1. **Criar um script de teste `test_transformations.py`:**
   - Crie um diretório `tests/` e um arquivo `test_transformations.py` dentro dele.
   - Escreva testes para funções de transformação básicas e avançadas.
   
   ```python
   import pytest
   from pyspark.sql import Row
   from pyspark.sql.functions import col
   from setup_pyspark import get_spark_session

   @pytest.fixture(scope="module")
   def spark():
       return get_spark_session()

   @pytest.fixture
   def sample_data(spark):
       data = [Row(name="Alice", age=30), Row(name="Bob", age=25)]
       return spark.createDataFrame(data)

   def test_filter_function(spark, sample_data):
       df_filtered = sample_data.filter(col("age") > 28)
       assert df_filtered.count() == 1
       assert df_filtered.collect()[0].name == "Alice"

   def test_select_function(spark, sample_data):
       df_selected = sample_data.select("name")
       assert df_selected.columns == ["name"]
       assert df_selected.count() == 2

   def test_aggregation_function(spark, sample_data):
       df_aggregated = sample_data.groupBy("age").count()
       assert df_aggregated.count() == 2
       assert df_aggregated.filter(col("age") == 30).collect()[0].count == 1
   ```

2. **Executar os Testes:**
   - No terminal, navegue até o diretório do projeto e execute:
     ```bash
     pytest tests/
     ```
   - Verifique a saída dos testes para garantir que tudo esteja funcionando conforme o esperado.

#### 3.2. Uso de Fixtures para Criar DataFrames de Teste

1. **Criar um script `fixtures.py` para definir fixtures avançadas:**
   - Crie um arquivo `fixtures.py` e defina fixtures que gerem DataFrames com diferentes configurações e dados.
   
   ```python
   import pytest
   from pyspark.sql import SparkSession, Row

   @pytest.fixture(scope="module")
   def spark():
       return SparkSession.builder \
           .appName("dataeng-testing") \
           .getOrCreate()

   @pytest.fixture
   def sample_data(spark):
       data = [Row(name="Alice", age=30), Row(name="Bob", age=25)]
       return spark.createDataFrame(data)

   @pytest.fixture
   def complex_data(spark):
       data = [
           Row(name="Alice", age=30, address=Row(city="New York", zip="10001")),
           Row(name="Bob", age=25, address=Row(city="San Francisco", zip="94105"))
       ]
       return spark.createDataFrame(data)
   ```

2. **Utilizar as fixtures nos testes:**
   - Importe as fixtures no arquivo de teste e utilize-as conforme necessário.

---

### 4. Parabéns!
Parabéns por concluir o módulo! Agora você está apto a escrever e executar testes para funções de transformação em Spark DataFrame.

### 5. Destruição dos Recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados:
- Exclua quaisquer ambientes de desenvolvimento não utilizados na AWS Cloud9.
- Remova arquivos temporários ou dados gerados durante os testes.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-9/
├── README.md
├── setup_pyspark.py
├── requirements.txt
├── fixtures.py
├── tests/
│   └── test_transformations.py
```
