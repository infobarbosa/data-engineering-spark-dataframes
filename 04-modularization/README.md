
# Modularização de Código PySpark
**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### 1. Introdução

##### 1.1. O que é Modularização de Código?
A modularização de código é a prática de dividir um código em módulos menores e mais gerenciáveis. Em projetos PySpark, isso significa organizar o código em funções, classes e arquivos separados, facilitando a manutenção, a reutilização e a testabilidade.

##### 1.2. Benefícios da Modularização em PySpark
- **Reutilização:** Módulos podem ser reutilizados em diferentes partes do projeto ou em projetos futuros.
- **Manutenção:** Facilita a atualização e a correção de bugs, uma vez que as alterações são concentradas em módulos específicos.
- **Testabilidade:** Módulos isolados são mais fáceis de testar individualmente.

#### 2. Estrutura de um Projeto Modular PySpark

##### 2.1. Organização de Pastas e Arquivos
Um exemplo de estrutura de pastas para um projeto PySpark modularizado:
```
dataeng-modulo-modularizacao/
├── README.md
├── main.py
├── utils/
│   ├── __init__.py
│   ├── data_processing.py
│   ├── spark_session.py
├── jobs/
│   ├── __init__.py
│   ├── job_1.py
│   ├── job_2.py
└── config/
    ├── __init__.py
    ├── settings.py
```
- **`main.py`**: Ponto de entrada do projeto.
- **`utils/`**: Contém módulos utilitários, como funções de processamento de dados e configuração da SparkSession.
- **`jobs/`**: Contém diferentes jobs que serão executados.
- **`config/`**: Arquivos de configuração e parâmetros do projeto.

##### 2.2. Exemplo de Modularização

**Exemplo 1: Definindo uma SparkSession em um módulo separado (`spark_session.py`):**
```python
from pyspark.sql import SparkSession

def get_spark_session(app_name="dataeng-pyspark"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark
```

**Exemplo 2: Funções de processamento de dados (`data_processing.py`):**
```python
def process_data(df):
    df_clean = df.dropna()
    df_transformed = df_clean.withColumn("new_column", df_clean["existing_column"] * 2)
    return df_transformed
```

**Exemplo 3: Job principal (`job_1.py`):**
```python
from utils.spark_session import get_spark_session
from utils.data_processing import process_data

def main():
    spark = get_spark_session("Job 1")
    df = spark.read.csv("s3://bucket/data.csv")
    df_transformed = process_data(df)
    df_transformed.show()

if __name__ == "__main__":
    main()
```

#### 3. Laboratório

##### 3.1. Objetivo
Neste laboratório, você modularizará um código PySpark existente, separando a lógica em diferentes módulos e organizando o projeto de maneira estruturada.

##### 3.2. Instruções

1. **Clone o Repositório**  
   Clone o repositório GitHub contendo os scripts de exemplo:
   ```bash
   git clone https://github.com/infobarbosa/dataeng-modulo-modularizacao.git
   cd dataeng-modulo-modularizacao
   ```

2. **Crie Módulos Utilitários**  
   - Mova a configuração da SparkSession para `utils/spark_session.py`.
   - Mova as funções de processamento de dados para `utils/data_processing.py`.

3. **Organize os Jobs**  
   - Organize as funções principais em arquivos dentro de `jobs/`.

4. **Execute o Código Modularizado**  
   Execute o job principal (`job_1.py`) para garantir que a modularização foi bem-sucedida:
   ```bash
   python jobs/job_1.py
   ```

##### 3.3. Exercício 1
- **Desafio:** Adapte a modularização para suportar diferentes ambientes (desenvolvimento, produção) utilizando configurações armazenadas em `config/settings.py`.

#### 4. Atenção aos custos!
Este laboratório pode gerar custos na AWS ao utilizar recursos como a SparkSession. Certifique-se de parar e destruir todos os recursos ao finalizar.

#### 5. Destruição dos recursos
Após concluir o laboratório, siga os passos abaixo para destruir todos os recursos utilizados:
```bash
# Parar a SparkSession e demais recursos
spark.stop()

# Remover dados intermediários, se aplicável
# (Exemplo de remoção de arquivos em S3, buckets, etc.)
```

#### 6. Parabéns!
Você concluiu o módulo sobre Modularização de Código PySpark. A modularização é um conceito crucial para projetos de grande escala e você está pronto para aplicá-la em seus próprios projetos!

