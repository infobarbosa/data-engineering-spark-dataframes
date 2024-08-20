### Módulo 4: Testando Código com DataFrames Aninhados

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 4.1. Introdução

Neste módulo, você aprenderá a testar código que manipula DataFrames com estruturas aninhadas, como arrays e structs, no Apache Spark. Vamos explorar estratégias para lidar com dados complexos em testes unitários e como validar a integridade dessas estruturas.

### 4.2. Parte Teórica

#### 4.2.1. Manipulação e Teste de DataFrames com Estruturas Aninhadas

- **DataFrames com Arrays:** Arrays são listas de valores, e em Spark, podem ser manipulados e acessados usando várias funções de array como `array_contains`, `explode`, entre outras.

- **DataFrames com Structs:** Structs são coleções de valores que podem conter diferentes tipos de dados, organizados como um registro. Campos dentro de structs podem ser acessados e manipulados diretamente.

- **Desafios em Testes:** Testar DataFrames com estruturas aninhadas requer um entendimento profundo de como acessar, manipular e validar esses dados complexos, garantindo que todos os casos possíveis sejam cobertos.

#### 4.2.2. Estratégias para Lidar com Dados Complexos em Testes

- **Criação de DataFrames de Teste:** Use fixtures e funções dedicadas para criar DataFrames com arrays e structs aninhados, simulando casos reais.

- **Validação de Estruturas Aninhadas:** Verifique se os dados dentro de arrays e structs estão corretos, usando métodos como `selectExpr`, `withColumn`, e funções específicas para manipulação dessas estruturas.

### 4.3. Parte Prática

#### 4.3.1. Escrever Testes para DataFrames com Arrays e Structs

1. **Criar um script de teste `test_nested_structs.py`:**
   - Crie um diretório `tests/` e um arquivo `test_nested_structs.py` dentro dele.
   - Escreva testes para DataFrames que contenham arrays e structs.

   ```python
   import pytest
   from pyspark.sql import Row
   from pyspark.sql import functions as F
   from setup_pyspark import get_spark_session

   @pytest.fixture(scope="module")
   def spark():
       return get_spark_session()

   @pytest.fixture
   def nested_data(spark):
       data = [
           Row(id=1, info=Row(name="Alice", age=29, scores=[100, 85])),
           Row(id=2, info=Row(name="Bob", age=31, scores=[95, 88]))
       ]
       return spark.createDataFrame(data)

   def test_struct_access(spark, nested_data):
       df = nested_data.select("info.name", "info.age")
       result = df.filter(F.col("name") == "Alice").collect()[0]
       assert result.age == 29

   def test_array_access(spark, nested_data):
       df = nested_data.withColumn("first_score", F.col("info.scores")[0])
       result = df.filter(F.col("id") == 1).select("first_score").collect()[0]
       assert result.first_score == 100

   def test_array_functions(spark, nested_data):
       df = nested_data.withColumn("score_sum", F.expr("aggregate(info.scores, 0, (x, y) -> x + y)"))
       result = df.filter(F.col("id") == 1).select("score_sum").collect()[0]
       assert result.score_sum == 185
   ```

2. **Executar os Testes:**
   - No terminal, navegue até o diretório do projeto e execute:
     ```bash
     pytest tests/
     ```
   - Verifique os resultados dos testes para garantir que as operações com arrays e structs funcionem conforme esperado.

#### 4.3.2. Uso de Métodos para Validar Estruturas Complexas

1. **Manipular e Validar Arrays:**
   - Use funções como `explode`, `array_contains`, e `aggregate` para manipular arrays e validar os resultados esperados.

2. **Manipular e Validar Structs:**
   - Acesse e valide campos específicos dentro de structs, utilizando métodos como `selectExpr` para uma validação mais granular.

---

### 4.4. Parabéns!
Parabéns por concluir o Módulo 4! Agora você domina a manipulação e o teste de DataFrames com estruturas aninhadas em Spark, incluindo arrays e structs.

### 4.5. Destruição dos Recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados:
- Exclua quaisquer ambientes de desenvolvimento não utilizados na AWS Cloud9.
- Remova arquivos temporários ou dados gerados durante os testes.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-4/
├── README.md
├── setup_pyspark.py
├── requirements.txt
├── fixtures.py
├── tests/
│   └── test_nested_structs.py
```
