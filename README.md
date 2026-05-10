# Projeto Prático: Integração entre Kafka, PostgreSQL e Amazon S3

## 1. Visão geral do projeto

Este projeto tem como objetivo demonstrar, de forma prática e didática, como construir uma arquitetura de ingestão de dados em tempo quase real utilizando **Apache Kafka**, **Kafka Connect**, **PostgreSQL**, **ksqlDB** e **Amazon S3**.

A ideia principal é simular um fluxo onde os dados são gerados em uma base PostgreSQL, capturados por um conector JDBC do Kafka Connect, publicados em um tópico Kafka e, posteriormente, enviados para um bucket no Amazon S3 por meio de um Sink Connector.

Além disso, o projeto também utiliza o **ksqlDB** para consultar, transformar e criar novos fluxos de dados em tempo real a partir dos eventos publicados no Kafka.

## 2. Arquitetura da solução

A arquitetura do laboratório pode ser resumida da seguinte forma:

```text
PostgreSQL
   ↓
Kafka Connect JDBC Source Connector
   ↓
Kafka Topic: postgres-customers
   ↓
ksqlDB / Kafka Consumers
   ↓
Kafka Connect S3 Sink Connector
   ↓
Amazon S3
```

### Componentes utilizados

| Componente            | Função no projeto                                         |
| --------------------- | --------------------------------------------------------- |
| PostgreSQL            | Banco relacional usado como fonte de dados                |
| Apache Kafka          | Plataforma de mensageria/event streaming                  |
| Zookeeper             | Coordenação do cluster Kafka no ambiente Confluent 7.0    |
| Kafka Connect         | Framework para integração entre Kafka e sistemas externos |
| JDBC Source Connector | Conector que lê dados do PostgreSQL e publica no Kafka    |
| S3 Sink Connector     | Conector que grava dados dos tópicos Kafka no Amazon S3   |
| Schema Registry       | Gerenciamento de schemas Avro                             |
| ksqlDB                | Engine SQL para processamento de streams em tempo real    |
| REST Proxy            | Interface HTTP para comunicação com o Kafka               |
| Docker Compose        | Orquestração dos containers do laboratório                |
| Amazon S3             | Camada de armazenamento destino dos dados                 |

## 3. Criação de usuário IAM na AWS

Antes de iniciar a integração com o Amazon S3, é recomendável criar um usuário IAM específico para acesso programático.

Esse usuário será utilizado pelo Kafka Connect para autenticar na AWS e gravar arquivos no bucket S3.

### 3.1 Criando o usuário IAM

No console da AWS, acesse:

```text
Security → IAM → Users → Create user
```

Crie um usuário com um nome válido, por exemplo:

```text
kafka-connect-s3-user
```

Como esse usuário será utilizado apenas por aplicações, scripts e conectores, não é necessário habilitar acesso ao console da AWS.

Deixe desmarcada a opção de acesso via Console.

### 3.2 Permissões do usuário

Para este laboratório, podemos anexar permissões diretamente ao usuário.

No ambiente de estudo, podem ser usadas permissões amplas, como:

```text
AmazonS3FullAccess
```

Em alguns laboratórios didáticos, também é comum utilizar permissões administrativas para evitar bloqueios durante os testes. Porém, em um cenário real, essa abordagem não é recomendada.

Em produção, o ideal é aplicar o princípio do menor privilégio, permitindo apenas as ações necessárias, por exemplo:

* Listar buckets específicos;
* Escrever objetos em um bucket específico;
* Ler objetos somente quando necessário;
* Negar acesso a buckets não relacionados ao projeto.

Um exemplo mais seguro seria uma política IAM limitada ao bucket utilizado pelo projeto.

### 3.3 Criando a Access Key

Após criar o usuário, acesse a aba:

```text
Security credentials → Access keys → Create access key
```

Escolha a opção:

```text
Command Line Interface (CLI)
```

Depois, crie a chave de acesso.

Ao final, a AWS exibirá:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

Guarde essas credenciais em um local seguro.

> Nunca suba Access Key ou Secret Key para repositórios GitHub, GitLab ou qualquer outro repositório público.

## 4. Configuração do arquivo de ambiente

No repositório do projeto, crie um arquivo chamado:

```text
connect/.env_kafka_connect
```

Esse arquivo será usado pelo container do Kafka Connect para carregar as credenciais da AWS.

Conteúdo do arquivo:

```env
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
```

Adicione esse arquivo ao `.gitignore`:

```gitignore
connect/.env_kafka_connect
*.env
```

Isso evita que credenciais sensíveis sejam versionadas acidentalmente.

## 5. Instalação e configuração da AWS CLI

A AWS CLI é uma ferramenta de linha de comando que permite interagir com os serviços da AWS diretamente pelo terminal.

Ela será útil para validar se as credenciais estão funcionando corretamente e se o bucket S3 pode ser acessado.

### 5.1 Instalação

Acesse a documentação oficial da AWS CLI e instale a versão correspondente ao seu sistema operacional.

No caso deste laboratório, o ambiente utilizado é Windows.

Após a instalação, valide a versão com o comando:

```bash
aws --version
```

### 5.2 Configuração da CLI

Execute:

```bash
aws configure
```

Informe os seguintes dados:

```text
AWS Access Key ID: sua_access_key
AWS Secret Access Key: sua_secret_key
Default region name: us-east-1
Default output format: deixe em branco ou use json
```

A região `us-east-1` corresponde à região Norte da Virgínia, bastante usada em laboratórios por ter boa disponibilidade de serviços.

O campo `Default output format` define como a AWS CLI exibirá os resultados no terminal. Para este projeto, esse campo não é crítico. Pode ser deixado vazio ou preenchido com `json`.

### 5.3 Validação do acesso

Para verificar se a autenticação está funcionando, execute:

```bash
aws s3 ls
```

Se estiver tudo correto, a CLI listará os buckets S3 disponíveis para o usuário configurado.

## 6. Criação do bucket no Amazon S3

No console da AWS, acesse:

```text
Storage → S3 → Create bucket
```

Crie um bucket do tipo **General Purpose**.

Exemplo de nome:

```text
amzn-s3-kafka-bucket-1
```

O nome do bucket precisa ser globalmente único na AWS. Caso esse nome já esteja em uso, escolha uma variação, por exemplo:

```text
amzn-s3-kafka-bucket-leo-lab-001
```

Para este laboratório, as configurações padrão do bucket são suficientes.

## 7. Provisionamento da arquitetura Kafka com Docker Compose

Para subir toda a arquitetura localmente, utilizaremos Docker e Docker Compose.

O Docker permite executar aplicações em containers. Um container é um ambiente isolado que contém tudo que a aplicação precisa para funcionar: sistema base, bibliotecas, dependências e configurações.

O Docker Compose permite subir vários containers de forma coordenada por meio de um único arquivo `docker-compose.yml`.

Neste projeto, o Compose será responsável por criar os seguintes serviços:

* Zookeeper;
* Kafka Broker;
* Schema Registry;
* Kafka Connect;
* ksqlDB Server;
* ksqlDB CLI;
* Kafka REST Proxy;
* PostgreSQL.

Todos os serviços ficarão conectados na mesma rede Docker chamada `proxynet`.

## 8. Arquivo docker-compose.yml

Abaixo está o arquivo base do Docker Compose utilizado no projeto:

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.0.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

  broker:
    image: confluentinc/cp-kafka:7.0.0
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.0.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

  connect:
    image: connect-custom:1.0.0
    hostname: connect
    container_name: connect
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8083:8083"
    env_file: .env_kafka_connect
    environment:
      CONNECT_BOOTSTRAP_SERVERS: 'broker:29092'
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
      CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

  ksqldb-server:
    image: confluentinc/cp-ksqldb-server:7.0.0
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - connect
    ports:
      - "8088:8088"
    environment:
      KSQL_CONFIG_DIR: "/etc/ksql"
      KSQL_BOOTSTRAP_SERVERS: "broker:29092"
      KSQL_HOST_NAME: ksqldb-server
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_CACHE_MAX_BYTES_BUFFERING: 0
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_CONNECT_URL: "http://connect:8083"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_REPLICATION_FACTOR: 1
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: 'true'
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: 'true'
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

  ksqldb-cli:
    image: confluentinc/cp-ksqldb-cli:7.0.0
    container_name: ksqldb-cli
    depends_on:
      - broker
      - connect
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

  rest-proxy:
    image: confluentinc/cp-kafka-rest:7.0.0
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8082:8082"
    hostname: rest-proxy
    container_name: rest-proxy
    environment:
      KAFKA_REST_HOST_NAME: rest-proxy
      KAFKA_REST_BOOTSTRAP_SERVERS: 'broker:29092'
      KAFKA_REST_LISTENERS: "http://0.0.0.0:8082"
      KAFKA_REST_SCHEMA_REGISTRY_URL: 'http://schema-registry:8081'
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

  postgres:
    image: postgres:13.2
    ports:
      - "5432:5432"
    hostname: postgres
    container_name: postgres
    environment:
      POSTGRES_PASSWORD: postgres
    networks:
      - proxynet
    volumes:
      - "/etc/timezone:/etc/timezone:ro"
      - "/etc/localtime:/etc/localtime:ro"

networks:
  proxynet:
    driver: bridge
```

## 9. Explicação dos principais serviços do Docker Compose

### 9.1 Zookeeper

O Zookeeper é responsável por coordenar alguns metadados do cluster Kafka nesse modelo baseado na versão Confluent 7.0.

Ele auxilia no controle de brokers, metadados e eleição de controller.

Apesar disso, é importante saber que versões mais recentes do Kafka caminham para o uso do **KRaft**, que reduz ou elimina a dependência do Zookeeper.

### 9.2 Kafka Broker

O serviço `broker` representa o Kafka Broker principal do laboratório.

Principais configurações:

```yaml
image: confluentinc/cp-kafka:7.0.0
```

Define a imagem Docker do Kafka fornecida pela Confluent.

```yaml
hostname: broker
container_name: broker
```

Define o nome interno e o nome do container. Isso facilita a comunicação entre os serviços e a execução de comandos via Docker.

Exemplo:

```bash
docker exec -it broker bash
```

```yaml
ports:
  - "29092:29092"
  - "9092:9092"
  - "9101:9101"
```

Essas portas têm finalidades diferentes:

| Porta | Uso                                                |
| ----- | -------------------------------------------------- |
| 29092 | Comunicação entre containers dentro da rede Docker |
| 9092  | Comunicação da máquina local com o Kafka           |
| 9101  | Porta JMX para métricas e monitoramento            |

A configuração mais importante do broker é:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
```

Ela informa aos clientes Kafka quais endereços devem ser usados para conexão.

* Containers dentro do Docker usam `broker:29092`;
* Aplicações rodando na máquina local usam `localhost:9092`.

Essa separação é necessária porque `localhost` dentro de um container aponta para o próprio container, e não para a máquina host.

### 9.3 Schema Registry

O Schema Registry gerencia os schemas usados pelas mensagens Avro.

Ele permite que producers e consumers validem a estrutura das mensagens, reduzindo erros de compatibilidade entre aplicações.

Neste projeto, ele será usado principalmente pelo Kafka Connect e pelo ksqlDB.

### 9.4 Kafka Connect

O Kafka Connect é o componente responsável por integrar o Kafka com sistemas externos.

Neste projeto, ele terá dois papéis principais:

1. Ler dados do PostgreSQL usando JDBC Source Connector;
2. Gravar dados no Amazon S3 usando S3 Sink Connector.

O serviço `connect` utiliza uma imagem customizada chamada:

```yaml
image: connect-custom:1.0.0
```

Essa imagem será criada manualmente para já conter os conectores JDBC e S3 instalados.

### 9.5 ksqlDB

O ksqlDB permite consultar e transformar dados em tópicos Kafka usando uma linguagem SQL-like.

Ele é composto por dois serviços:

* `ksqldb-server`: servidor responsável por executar as queries;
* `ksqldb-cli`: cliente de linha de comando para interagir com o servidor.

Com ele, é possível criar streams, filtrar dados, classificar registros e gerar agregações em tempo real.

### 9.6 REST Proxy

O REST Proxy permite interagir com o Kafka por meio de chamadas HTTP.

Ele é útil quando uma aplicação não quer ou não pode usar diretamente os clientes Kafka nativos.

### 9.7 PostgreSQL

O PostgreSQL será usado como banco de origem.

Neste laboratório, ele será populado com dados de exemplo por meio de um script Python.

Depois disso, o Kafka Connect fará a leitura da tabela e enviará os registros para um tópico Kafka.

## 10. Criação da imagem customizada do Kafka Connect

Antes de subir o Docker Compose, é necessário criar a imagem customizada do Kafka Connect.

Isso acontece porque o serviço `connect` usa a imagem:

```text
connect-custom:1.0.0
```

Essa imagem não existe por padrão no Docker Hub. Ela precisa ser criada localmente.

### 10.1 Criando o Dockerfile

Crie uma pasta chamada:

```text
custom-kafka-connector-image
```

Dentro dela, crie um arquivo chamado:

```text
Dockerfile
```

Conteúdo do arquivo:

```dockerfile
FROM confluentinc/cp-kafka-connect-base:7.0.0

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.4.1 \
    && confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.0.7
```

Essa imagem parte da imagem base do Kafka Connect e instala automaticamente dois conectores:

| Conector           | Finalidade                                       |
| ------------------ | ------------------------------------------------ |
| kafka-connect-jdbc | Ler dados de bancos relacionais, como PostgreSQL |
| kafka-connect-s3   | Gravar dados em buckets Amazon S3                |

Sem essa imagem customizada, seria necessário instalar os conectores manualmente dentro do container, o que tornaria o processo menos reproduzível.

### 10.2 Build da imagem

Entre na pasta onde está o Dockerfile e execute:

```bash
docker build . -t connect-custom:1.0.0
```

Ou, caso utilize Buildx:

```bash
docker buildx build . -t connect-custom:1.0.0
```

Após a conclusão, valide se a imagem foi criada:

```bash
docker images
```

Procure por:

```text
connect-custom   1.0.0
```

## 11. Subindo o ambiente

Com a imagem customizada criada, execute:

```bash
docker-compose up -d
```

O parâmetro `-d` executa os containers em modo detached, ou seja, libera o terminal após a inicialização.

Como as imagens da Confluent são relativamente grandes, a primeira execução pode demorar alguns minutos.

Para verificar os containers ativos:

```bash
docker ps
```

Para visualizar logs de um serviço específico:

```bash
docker logs -f connect
```

## 12. Preparando o PostgreSQL

Após subir o ambiente, conecte-se ao PostgreSQL usando uma ferramenta de sua preferência, como:

* DBeaver;
* DataGrip;
* pgAdmin;
* Beekeeper Studio.

Dados de conexão:

```text
Host: localhost
Porta: 5432
Database: postgres
Usuário: postgres
Senha: postgres
```

Neste projeto, será criado um script Python para gerar dados na tabela `public.customers`.

A tabela deve possuir uma coluna de controle temporal chamada `dt_update`, que será usada pelo conector JDBC para identificar novos registros.

Exemplo simplificado de estrutura:

```sql
CREATE TABLE public.customers (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(150),
    sexo VARCHAR(20),
    telefone VARCHAR(50),
    email VARCHAR(150),
    profissao VARCHAR(100),
    nascimento DATE,
    dt_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 13. Criando o tópico Kafka

Agora vamos criar o tópico Kafka que receberá os dados vindos do PostgreSQL.

Execute:

```bash
docker exec -it broker bash
```

Dentro do container, crie o tópico:

```bash
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --topic postgres-customers
```

Também é possível executar diretamente da máquina host:

```bash
docker exec broker \
  kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1 \
  --topic postgres-customers
```

Como o laboratório possui apenas um broker, o fator de replicação precisa ser `1`.

Em produção, normalmente seria usado um fator de replicação maior, como `3`, para garantir maior resiliência.

## 14. Criando o JDBC Source Connector

O JDBC Source Connector será responsável por ler dados do PostgreSQL e publicar no tópico Kafka.

Crie um arquivo chamado:

```text
connectors/source/connect_jdbc_postgres.config
```

Conteúdo:

```json
{
  "name": "postg-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://postgres:5432/postgres",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "mode": "timestamp",
    "timestamp.column.name": "dt_update",
    "table.whitelist": "public.customers",
    "topic.prefix": "postgres-",
    "validate.non.null": "false",
    "poll.interval.ms": "500"
  }
}
```

### 14.1 Explicação dos principais parâmetros

| Parâmetro             | Explicação                                    |
| --------------------- | --------------------------------------------- |
| connector.class       | Classe do conector JDBC Source                |
| tasks.max             | Número máximo de tarefas paralelas            |
| connection.url        | URL JDBC de conexão com o PostgreSQL          |
| connection.user       | Usuário do banco                              |
| connection.password   | Senha do banco                                |
| mode                  | Modo de captura dos dados                     |
| timestamp.column.name | Coluna usada para identificar novos registros |
| table.whitelist       | Tabela monitorada pelo conector               |
| topic.prefix          | Prefixo usado para criar o nome do tópico     |
| poll.interval.ms      | Intervalo de polling no banco                 |

Como o `topic.prefix` está definido como `postgres-` e a tabela se chama `customers`, o tópico final será:

```text
postgres-customers
```

### 14.2 Registrando o conector

Execute:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/source/connect_jdbc_postgres.config \
  http://localhost:8083/connectors
```

Para verificar se o conector foi criado:

```bash
curl http://localhost:8083/connectors
```

Para verificar o status:

```bash
curl http://localhost:8083/connectors/postg-connector/status
```

## 15. Consumindo os dados do tópico Kafka

Para validar se os dados estão chegando ao tópico, execute:

```bash
docker exec -it broker \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic postgres-customers \
  --from-beginning
```

Esse comando consome as mensagens do tópico desde o início.

Se o conector estiver funcionando, os registros vindos da tabela `public.customers` serão exibidos no terminal.

### Observação importante sobre o modo timestamp

No modo `timestamp`, o JDBC Source Connector identifica novos registros com base na coluna definida em `timestamp.column.name`.

Isso significa que ele captura:

* Registros já existentes na primeira carga;
* Novos registros inseridos posteriormente;
* Registros atualizados, desde que a coluna `dt_update` seja atualizada.

Se um registro for alterado mas a coluna `dt_update` permanecer igual, o Kafka Connect não identificará a mudança.

Para cenários mais robustos de CDC, o ideal seria usar ferramentas como Debezium, que capturam alterações diretamente do log transacional do banco.

## 16. Criando o S3 Sink Connector

Agora vamos configurar um Sink Connector para gravar no Amazon S3 os dados publicados no tópico `postgres-customers`.

Crie o arquivo:

```text
connectors/sink/connect_s3_sink.config
```

Conteúdo:

```json
{
  "name": "customers-s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "keys.format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
    "flush.size": "2",
    "schema.compatibility": "FULL",
    "s3.bucket.name": "NOME-DO-BUCKET",
    "s3.region": "us-east-1",
    "s3.object.tagging": "true",
    "s3.ssea.name": "AES256",
    "topics.dir": "raw-data/kafka",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "tasks.max": "1",
    "topics": "postgres-customers"
  }
}
```

Substitua:

```text
NOME-DO-BUCKET
```

pelo nome real do bucket criado no Amazon S3.

### 16.1 Registrando o conector S3

Execute:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/sink/connect_s3_sink.config \
  http://localhost:8083/connectors
```

Esse conector passará a consumir os eventos do tópico `postgres-customers` e gravá-los no S3 dentro do diretório:

```text
raw-data/kafka/postgres-customers/
```

## 17. Usando o ksqlDB

Para acessar o ksqlDB CLI, execute:

```bash
docker-compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

### 17.1 Listando tópicos

Dentro do ksqlDB, execute:

```sql
SHOW TOPICS;
```

A saída esperada deve conter o tópico:

```text
postgres-customers
```

### 17.2 Verificando conectores

Execute:

```sql
SHOW CONNECTORS;
```

O conector JDBC deve aparecer com status semelhante a:

```text
RUNNING (1/1 tasks RUNNING)
```

### 17.3 Visualizando mensagens do tópico

Execute:

```sql
PRINT 'postgres-customers';
```

Esse comando mostra os eventos publicados no tópico.

## 18. Criando um stream no ksqlDB

Para consultar os dados de forma estruturada, crie um stream a partir do tópico `postgres-customers`:

```sql
CREATE STREAM custstream
WITH (
  KAFKA_TOPIC='postgres-customers',
  VALUE_FORMAT='AVRO'
);
```

Depois, liste as streams:

```sql
SHOW STREAMS;
```

Para consultar os dados:

```sql
SELECT * FROM custstream EMIT CHANGES;
```

Como a tabela pode possuir muitas colunas, uma consulta mais enxuta pode ser melhor:

```sql
SELECT nome, telefone, email, nascimento, dt_update
FROM custstream
EMIT CHANGES;
```

## 19. Criando fluxos derivados em tempo real

Agora vamos criar um stream que filtra apenas pessoas jovens, considerando como jovens aquelas nascidas a partir de `2000-01-01`.

```sql
CREATE STREAM jovens
WITH (
  KAFKA_TOPIC='jovens',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  nome,
  sexo,
  telefone,
  email,
  profissao,
  nascimento,
  dt_update
FROM custstream
WHERE nascimento >= '2000-01-01'
EMIT CHANGES;
```

Esse comando cria automaticamente um novo tópico chamado:

```text
jovens
```

## 20. Classificação por faixa etária

Também podemos criar um stream que classifica os registros entre `JOVEM` e `ADULTO`.

```sql
CREATE STREAM idadeclass
WITH (
  KAFKA_TOPIC='idadeclass',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  nome,
  telefone,
  email,
  profissao,
  CASE
    WHEN nascimento >= '2000-01-01' THEN 'JOVEM'
    ELSE 'ADULTO'
  END AS idadecat,
  dt_update
FROM custstream
EMIT CHANGES;
```

## 21. Criando uma tabela agregada em tempo real

Agora vamos criar uma tabela que conta a quantidade de jovens e adultos em janelas de 30 segundos.

```sql
CREATE TABLE idadecont
WITH (
  KAFKA_TOPIC='idadecont',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  idadecat,
  COUNT(idadecat) AS contagem
FROM idadeclass
WINDOW TUMBLING (SIZE 30 SECONDS)
GROUP BY idadecat
EMIT CHANGES;
```

Essa tabela gera uma agregação contínua dos dados processados.

## 22. Enviando o tópico jovens para o S3

Agora vamos criar um novo Sink Connector para gravar o tópico `jovens` no Amazon S3.

Crie o arquivo:

```text
connectors/sink/s3_sink_jovens.config
```

Conteúdo:

```json
{
  "name": "s3-jovens-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
    "flush.size": "10",
    "schema.compatibility": "FULL",
    "s3.bucket.name": "NOME_DO_BUCKET",
    "s3.region": "REGIAO_AWS",
    "s3.object.tagging": "true",
    "s3.ssea.name": "AES256",
    "topics.dir": "raw-data/kafka",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "tasks.max": "1",
    "topics": "jovens"
  }
}
```

Registre o conector:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/sink/s3_sink_jovens.config \
  http://localhost:8083/connectors
```

## 23. Enviando a contagem agregada para o S3

Crie também um Sink Connector para o tópico `idadecont`.

Arquivo:

```text
connectors/sink/s3_sink_count.config
```

Conteúdo:

```json
{
  "name": "s3-contagem-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "keys.format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
    "flush.size": "10",
    "schema.compatibility": "NONE",
    "s3.bucket.name": "NOME_DO_BUCKET",
    "s3.region": "REGIAO_AWS",
    "s3.object.tagging": "true",
    "s3.ssea.name": "AES256",
    "topics.dir": "raw-data/kafka",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "tasks.max": "1",
    "topics": "idadecont",
    "store.kafka.keys": "true"
  }
}
```

Registre o conector:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/sink/s3_sink_count.config \
  http://localhost:8083/connectors
```

## 24. Validação final

Para validar os conectores no ksqlDB:

```sql
SHOW CONNECTORS;
```

Todos os conectores devem aparecer com status `RUNNING`.

Também é possível verificar pelo Kafka Connect REST API:

```bash
curl http://localhost:8083/connectors
```

E para verificar o status individual:

```bash
curl http://localhost:8083/connectors/customers-s3-sink/status
curl http://localhost:8083/connectors/s3-jovens-sink/status
curl http://localhost:8083/connectors/s3-contagem-sink/status
```

No Amazon S3, os dados devem aparecer no caminho configurado:

```text
raw-data/kafka/
```

Dentro desse diretório, devem existir subpastas para os tópicos gravados, como:

```text
raw-data/kafka/postgres-customers/
raw-data/kafka/jovens/
raw-data/kafka/idadecont/
```

## 25. Pontos de atenção para produção

Este projeto é voltado para laboratório e aprendizado. Para um ambiente produtivo, alguns pontos precisam ser revistos.

### Segurança

* Evitar permissões amplas como `AdministratorAccess`;
* Usar IAM Role sempre que possível;
* Restringir acesso ao bucket S3 específico;
* Usar criptografia e políticas de bucket;
* Nunca versionar credenciais em repositórios.

### Kafka

* Usar múltiplos brokers;
* Configurar replicação maior que 1;
* Habilitar autenticação e criptografia;
* Definir políticas de retenção adequadas;
* Monitorar lag de consumidores.

### Kafka Connect

* Usar mais de uma task quando fizer sentido;
* Monitorar status dos conectores;
* Configurar tratamento de erros e Dead Letter Queue;
* Versionar arquivos de configuração dos conectores;
* Separar conectores source e sink por responsabilidade.

### S3/Data Lake

* Avaliar formatos mais eficientes, como Parquet ou Avro;
* Definir particionamento por data;
* Organizar zonas como bronze, silver e gold;
* Usar catálogo de dados, como AWS Glue Data Catalog;
* Controlar ciclo de vida dos objetos no S3.

### CDC

O JDBC Source Connector em modo `timestamp` é útil para laboratórios e cargas simples, mas não é o modelo mais robusto para CDC.

Para captura real de inserts, updates e deletes, o mais adequado seria utilizar ferramentas como:

* Debezium;
* AWS DMS;
* CDC nativo do banco, quando aplicável.

## 26. Conclusão

Neste projeto, foi construída uma arquitetura completa de ingestão e processamento de dados em tempo quase real utilizando Kafka, Kafka Connect, PostgreSQL, ksqlDB e Amazon S3.

O fluxo demonstrou como:

1. Criar um usuário programático na AWS;
2. Configurar credenciais com segurança;
3. Criar um bucket S3;
4. Subir uma stack Kafka local com Docker Compose;
5. Criar uma imagem customizada do Kafka Connect;
6. Ler dados do PostgreSQL com JDBC Source Connector;
7. Publicar dados em tópicos Kafka;
8. Consultar e transformar dados com ksqlDB;
9. Gravar tópicos Kafka no Amazon S3 com S3 Sink Connector.

Essa arquitetura é uma excelente base para entender pipelines orientados a eventos e pode evoluir para cenários mais avançados de data lake, CDC, arquitetura medalhão e processamento distribuído.
