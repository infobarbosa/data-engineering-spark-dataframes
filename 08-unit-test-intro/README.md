### Módulo 8: Introdução aos Testes Unitários em Spark

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 1.1. Introdução

Neste módulo, você aprenderá os conceitos básicos de testes unitários, sua importância para a qualidade dos pipelines de dados e como configurar e usar ferramentas e frameworks para realizar testes unitários em Spark DataFrame.

### 1.2. Parte Teórica

#### 1.2.1. Conceitos Básicos de Testes Unitários
Testes unitários são uma prática fundamental para garantir que o código funcione como esperado. Eles testam pequenas unidades do código de forma isolada, permitindo detectar e corrigir erros mais facilmente.

**Conceitos chave:**
- **Unidade de Teste:** A menor parte do código que pode ser testada de forma independente.
- **Asserções:** Declarações que verificam se os resultados obtidos correspondem aos resultados esperados.
- **Mocking:** Técnica para simular o comportamento de objetos e funções que não estão sendo testados diretamente.

#### 1.2.2. Importância dos Testes em Pipelines de Dados
Em pipelines de dados, os testes garantem que os dados sejam processados corretamente e que as transformações e cálculos sejam realizados de forma precisa. Eles ajudam a evitar erros em grandes volumes de dados e a manter a integridade e a confiabilidade dos processos de dados.

**Benefícios dos testes:**
- **Detecção Precoce de Erros:** Identifica erros durante o desenvolvimento em vez de em produção.
- **Facilita Refatorações:** Permite alterar o código com confiança de que as funcionalidades existentes não serão quebradas.
- **Documentação:** Serve como documentação viva para o comportamento esperado do código.

#### 1.2.3. Ferramentas e Frameworks de Testes para Spark
- **PySpark:** A API Python para Apache Spark.
- **pytest:** Um framework de testes para Python que suporta fixtures e asserções.
- **pyspark.sql.functions:** Biblioteca que fornece funções para manipulação de DataFrames no PySpark.

### 1.3. Laboratório

#### 1.3.1. Configuração do Ambiente de Testes

1. **Configurar AWS Cloud9 com Ubuntu:**
   - Acesse o AWS Cloud9.
   - Abra um novo ambiente de desenvolvimento com Ubuntu.

2. **Instalar PySpark e pytest:**
   - Crie um arquivo `requirements.txt` com o seguinte conteúdo:
     ```
     pyspark==3.5.2
     pytest==8.3.2
     ```
   - Instale as dependências:
     ```bash
     pip install -r requirements.txt
     ```

3. **Configurar o PySpark:**
   - Crie um script de configuração `setup_pyspark.py` para iniciar a SparkSession:
     ```python
     from pyspark.sql import SparkSession

     def get_spark_session(app_name="dataeng-testing"):
         return SparkSession.builder \
             .appName(app_name) \
             .getOrCreate()
     ```

#### 1.3.2. Escrever e Executar Testes Unitários Básicos

1. **Criar um script de teste `test_basic.py`:**
   - Crie um diretório `tests/` e um arquivo `test_basic.py` dentro dele.
   - Escreva um teste básico para uma função de transformação:
     ```python
     import pytest
     from pyspark.sql import Row
     from setup_pyspark import get_spark_session

     @pytest.fixture(scope="module")
     def spark():
         return get_spark_session()

     def test_filter_data(spark):
         data = [Row(name="Alice", age=30), Row(name="Bob", age=25)]
         df = spark.createDataFrame(data)
         df_filtered = df.filter(df.age > 28)
         assert df_filtered.count() == 1
         assert df_filtered.collect()[0].name == "Alice"
     ```

2. **Executar os Testes:**
   - No terminal, navegue até o diretório do projeto e execute:
     ```bash
     pytest tests/
     ```
   - Verifique a saída dos testes para garantir que tudo esteja funcionando conforme o esperado.

### 1.4. Parabéns!
Parabéns por concluir o módulo! Agora você tem uma base sólida sobre testes unitários em Spark e está pronto para avançar para módulos mais complexos.

### 1.5. Destruição dos Recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados:
- Exclua quaisquer ambientes de desenvolvimento não utilizados na AWS Cloud9.
- Remova arquivos temporários ou dados gerados durante os testes.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-8/
├── README.md
├── setup_pyspark.py
├── requirements.txt
├── tests/
│   └── test_basic.py
```
