### Módulo 7: Boas Práticas e Padrões de Projeto

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 7.1. Introdução
Neste módulo, abordaremos as boas práticas e padrões de projeto para o desenvolvimento de pipelines em Apache Spark, focando na escrita de código eficiente e reutilizável, debugging e tratamento de erros, além do gerenciamento de dependências e integração contínua.

### 7.2. Padrões para Escrita de Código Eficiente e Reutilizável
#### 7.2.1. Modularização e Reuso de Código
Organizar o código em funções e módulos reutilizáveis não só facilita a manutenção, como também melhora a eficiência do desenvolvimento.

**Exemplo de código:**
```python
from pyspark.sql import SparkSession

# Inicializando a SparkSession
spark = SparkSession.builder \
    .appName("dataeng-modulo-7") \
    .getOrCreate()

def load_data(spark, path):
    return spark.read.parquet(path)

def process_data(df):
    return df.filter(df['value'] > 100)

# Uso de funções modulares
df = load_data(spark, "s3://bucket_name/data/parquet/")
df_processed = process_data(df)
df_processed.show()
```

#### 7.2.2. Padrões de Nomeação e Estruturação de Projetos
Manter uma nomenclatura consistente e uma estrutura de projetos organizada é fundamental para projetos de longa duração.

**Sugestão de Estrutura de Diretórios:**
```
src/
├── data_ingestion.py
├── data_processing.py
├── data_export.py
tests/
├── test_data_ingestion.py
├── test_data_processing.py
├── test_data_export.py
config/
├── spark_config.py
```

### 7.3. Debugging e Tratamento de Erros em Pipelines Spark
#### 7.3.1. Técnicas de Debugging
O uso de logs e a exploração de ferramentas como o Spark UI são cruciais para o debugging eficaz em pipelines Spark.

**Exemplo de código com logs:**
```python
import logging

# Configurando logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_data_with_logging(df):
    try:
        logger.info("Iniciando o processamento de dados.")
        df_filtered = df.filter(df['value'] > 100)
        logger.info("Processamento concluído com sucesso.")
        return df_filtered
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        raise

df_processed = process_data_with_logging(df)
```

#### 7.3.2. Tratamento de Erros
Implementar tratamentos de exceção robustos é essencial para a resiliência de pipelines de dados.

**Exemplo de tratamento de exceções:**
```python
def safe_divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        logger.error("Divisão por zero detectada.")
        return None
```

### 7.4. Gerenciamento de Dependências e Integração Contínua
#### 7.4.1. Gerenciamento de Dependências com `pip` e `requirements.txt`
Controlar as dependências do projeto através de arquivos como `requirements.txt` garante a replicabilidade e consistência do ambiente de desenvolvimento.

**Exemplo de `requirements.txt`:**
```
pyspark==3.4.0
boto3==1.28.0
```

**Instalação das dependências:**
```bash
pip install -r requirements.txt
```

#### 7.4.2. Integração Contínua (CI) com GitHub Actions
Configurar pipelines de CI para execução de testes e verificação de qualidade de código.

**Exemplo de configuração do GitHub Actions:**
```yaml
name: CI Pipeline

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.x'
    - name: Install dependencies
      run: pip install -r requirements.txt
    - name: Run tests
      run: pytest
```

### 7.5. Exercício Prático Avançado
**Objetivo:** Implementar boas práticas e padrões de projeto em um pipeline Spark, focando na modularização do código, gerenciamento de dependências e configuração de integração contínua.

**Instruções:**
1. Clone o repositório do curso:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-7.git
   ```
2. Navegue até a pasta do módulo 7:
   ```bash
   cd dataeng-modulo-7
   ```
3. Implemente um pipeline Spark com as seguintes etapas:
   - Modularização do código em funções reutilizáveis.
   - Adição de logs para monitorar o progresso do pipeline.
   - Configuração de um arquivo `requirements.txt` para gerenciamento de dependências.
   - Implementação de uma pipeline de CI utilizando GitHub Actions.

**Código do laboratório:**
```python
# Exemplo de script modulo7.py

import logging
from pyspark.sql import SparkSession

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Função modular para carregar dados
def load_data(spark, path):
    logger.info(f"Carregando dados de {path}")
    return spark.read.parquet(path)

# Função modular para processar dados
def process_data(df):
    logger.info("Processando dados...")
    return df.filter(df['value'] > 100)

# Função modular para salvar dados
def save_data(df, path):
    logger.info(f"Salvando dados em {path}")
    df.write.parquet(path)

# Pipeline principal
if __name__ == "__main__":
    spark = SparkSession.builder.appName("dataeng-modulo-7").getOrCreate()
    
    df = load_data(spark, "s3://bucket_name/data/parquet/")
    df_processed = process_data(df)
    save_data(df_processed, "s3://bucket_name/data/processed/")
    
    logger.info("Pipeline concluído com sucesso.")
    spark.stop()
```

### 7.6. Parabéns!
Parabéns por concluir o módulo 7! Agora você está preparado para aplicar boas práticas e padrões de projeto em seus pipelines Apache Spark.

### 7.7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-7/
│
├── README.md
├── modulo7.py
├── requirements.txt
├── .github/
│   └── workflows/
│       └── ci-pipeline.yml
```
