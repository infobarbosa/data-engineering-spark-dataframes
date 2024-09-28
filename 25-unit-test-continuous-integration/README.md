### Módulo 13: Integração Contínua e Testes Automatizados

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### 1. Introdução

Neste módulo, abordaremos a integração contínua (CI) para projetos Spark e como automatizar a execução de testes unitários em pipelines de CI/CD. A prática inclui a configuração de pipelines de CI/CD usando GitHub Actions ou Jenkins, e a execução automatizada de testes unitários em ambientes de integração contínua.

### 2. Parte Teórica

#### 2.1. Integração Contínua para Projetos Spark

- **Conceito de Integração Contínua (CI):** 
  - CI é uma prática de desenvolvimento que envolve a integração frequente do código em um repositório compartilhado, seguido da execução automática de testes.
  
- **Benefícios da CI em Projetos Spark:**
  - **Detecção Precoce de Erros:** Testes automáticos permitem a identificação rápida de problemas ao integrar novo código.
  - **Automatização de Processos Repetitivos:** Facilita a execução de testes, builds e validações.
  - **Melhor Qualidade do Código:** Garantia de que as mudanças no código não introduzam regressões.

- **Ferramentas de CI/CD:**
  - **GitHub Actions:** Plataforma nativa do GitHub para CI/CD, altamente integrada ao repositório de código.
  - **Jenkins:** Ferramenta de automação de código aberto que permite a criação de pipelines personalizáveis.

#### 2.2. Automatização de Testes Unitários com CI/CD

- **Configuração de Pipelines:** Como configurar um pipeline para automatizar a execução de testes unitários sempre que houver novas submissões de código.
  
- **Execução Automatizada de Testes:**
  - **Triggers:** Disparo de execuções de pipeline em eventos como push, pull request, ou agendamentos.
  - **Notificações:** Configuração de notificações automáticas para informar o status dos testes.

### 3. Parte Prática

#### 3.1. Configuração de Pipelines de CI/CD

1. **Configuração com GitHub Actions:**
   - Crie um arquivo YAML `.github/workflows/ci.yml` no repositório.
   - Defina um pipeline básico para rodar testes com pytest em um ambiente PySpark.

   ```yaml
   name: CI Pipeline

   on: [push, pull_request]

   jobs:
     test:
       runs-on: ubuntu-latest

       steps:
       - name: Check out repository
         uses: actions/checkout@v4

       - name: Set up Python
         uses: actions/setup-python@v2
         with:
           python-version: '3.12'

       - name: Install dependencies
         run: |
           python -m pip install --upgrade pip
           pip install -r requirements.txt

       - name: Run tests
         run: |
           pytest tests/
   ```

2. **Configuração com Jenkins:**
   - Instale e configure o Jenkins em uma instância EC2 na AWS.
   - Crie um pipeline no Jenkins para rodar testes unitários, configurando o pipeline usando o seguinte script em um arquivo `Jenkinsfile`.

   ```groovy
   pipeline {
       agent any

       stages {
           stage('Checkout') {
               steps {
                   git 'https://github.com/infobarbosa/spark-testing'
               }
           }
           stage('Install dependencies') {
               steps {
                   sh 'pip install -r requirements.txt'
               }
           }
           stage('Run tests') {
               steps {
                   sh 'pytest tests/'
               }
           }
       }
       post {
           always {
               junit 'tests/reports/*.xml'
               cleanWs()
           }
       }
   }
   ```

#### 3.2. Execução Automatizada de Testes Unitários

1. **Pipeline com GitHub Actions:**
   - Submeta uma mudança ao repositório para disparar a execução do pipeline.
   - Verifique o status do pipeline em **Actions** no GitHub.

2. **Pipeline com Jenkins:**
   - Execute o pipeline manualmente ou configure um trigger para rodá-lo automaticamente ao detectar mudanças no repositório.
   - Analise os resultados dos testes na interface do Jenkins.

3. **Notificações:**
   - Adicione notificações via e-mail ou Slack para monitorar o status dos pipelines e ser alertado em caso de falhas.

---

### 4. Parabéns!
Parabéns por concluir o módulo! Agora você está preparado para integrar a automação de testes unitários em seus projetos Spark utilizando pipelines de CI/CD.

### 5. Destruição dos Recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados:
- Exclua quaisquer instâncias de Jenkins na AWS, se não forem mais necessárias.
- Remova ambientes de desenvolvimento não utilizados na AWS Cloud9.
- Desative pipelines e triggers que não estão em uso.

---

**Estrutura do Repositório no GitHub:**
```
dataeng-modulo-13/
├── README.md
├── .github/
│   └── workflows/
│       └── ci.yml
├── Jenkinsfile
├── requirements.txt
├── tests/
│   └── test_example.py
```

