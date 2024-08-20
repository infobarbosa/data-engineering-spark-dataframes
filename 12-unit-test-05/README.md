### Módulo 5: Debugging e Melhorando Testes

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 5.1. Introdução

Neste módulo, você aprenderá técnicas de debugging para testes unitários em Spark e explorará as melhores práticas para escrever testes eficazes. A prática focará na depuração de falhas em testes e na melhoria da cobertura e robustez dos mesmos.

### 5.2. Parte Teórica

#### 5.2.1. Técnicas de Debugging para Testes Unitários

- **Identificação de Erros:** Saiba como identificar e isolar erros nos testes unitários. Ferramentas como logs e mensagens de erro detalhadas podem ajudar a localizar a origem dos problemas.

- **Uso de Breakpoints:** Em ambientes de desenvolvimento como o AWS Cloud9, utilize breakpoints para pausar a execução do código e inspecionar o estado das variáveis e do DataFrame.

- **Verificação de Dados:** Imprima e inspecione pequenos subconjuntos de dados para garantir que os DataFrames estejam sendo criados e transformados conforme esperado antes de realizar asserções.

#### 5.2.2. Melhores Práticas para Escrever Testes Eficazes

- **Cobertura de Testes:** Escreva testes que cobrem todos os casos de uso, incluindo casos limites e cenários inesperados.

- **Modularidade dos Testes:** Organize seus testes de forma modular, separando a lógica de preparação dos dados, a execução dos testes e as asserções.

- **Manutenção de Testes:** Escreva testes que sejam fáceis de manter e que possam ser facilmente adaptados a mudanças no código base.

### 5.3. Parte Prática

#### 5.3.1. Debugging de Falhas em Testes Unitários

1. **Criar e Configurar o Script `test_debugging.py`:**
   - Crie um arquivo `test_debugging.py` no diretório `tests/`.
   - Escreva um teste propositalmente incorreto para depuração.

   ```python
   import pytest
   from pyspark.sql import functions as F
   from setup_pyspark import get_spark_session

   @pytest.fixture(scope="module")
   def spark():
       return get_spark_session()

   @pytest.fixture
   def sample_data(spark):
       data = [("Alice", 29), ("Bob", 31), ("Charlie", 28)]
       schema = ["name", "age"]
       return spark.createDataFrame(data, schema)

   def test_incorrect_transformation(spark, sample_data):
       df = sample_data.withColumn("age_double", F.col("age") * 2)
       result = df.filter(F.col("name") == "Alice").collect()[0]
       assert result.age_double == 58  # Intencionalmente errado para debugging
   ```

2. **Executar e Debuggar o Teste:**
   - Execute o teste usando o pytest:
     ```bash
     pytest tests/test_debugging.py
     ```
   - Identifique o erro na saída do teste.
   - Utilize prints, logs e breakpoints para investigar o estado do DataFrame antes da falha.

3. **Corrigir e Validar o Teste:**
   - Corrija o erro no código e reexecute o teste para confirmar que ele passa.

#### 5.3.2. Melhorar a Cobertura e a Robustez dos Testes

1. **Cobertura de Casos Limites:**
   - Expanda os testes para cobrir casos limites, como valores nulos, strings vazias ou listas vazias.
   - Adicione um novo teste no `test_debugging.py` para cobrir um caso limite.

   ```python
   def test_null_values(spark, sample_data):
       df_with_null = sample_data.union(spark.createDataFrame([("Unknown", None)], ["name", "age"]))
       df_with_null = df_with_null.withColumn("age_double", F.col("age") * 2)
       result = df_with_null.filter(F.col("name") == "Unknown").collect()[0]
       assert result.age_double is None  # Testando comportamento com valores nulos
   ```

2. **Organização e Modularidade:**
   - Refatore o código de teste para melhor organização, criando funções auxiliares para evitar repetição e melhorar a clareza.

3. **Verificação de Performance:**
   - Adicione testes que garantam a performance aceitável dos DataFrames, como limites de tempo para certas operações.

---

### 5.4. Parabéns!
Parabéns por concluir o Módulo 5! Agora você tem habilidades avançadas de debugging e está mais capacitado para escrever testes unitários eficazes para pipelines Spark.

### 5.5. Destruição dos Recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados:
- Exclua quaisquer ambientes de desenvolvimento não utilizados na AWS Cloud9.
- Remova arquivos temporários ou dados gerados durante os testes.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-5/
├── README.md
├── setup_pyspark.py
├── requirements.txt
├── fixtures.py
├── tests/
│   └── test_debugging.py
```

Se precisar de mais detalhes ou ajustes, estou à disposição!