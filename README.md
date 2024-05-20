# Data Lake Shopper.

# O Projeto

Teste tecnico de engenheiro de dados.
Criação de um datalake ingerindo dados dos sistemas da shopper.
A operação consiste em projetar e implementar uma infraestrutura no ambiente da GCP robusta
que suporte as operações de negócio, desde o registro de produtos até promoções e relatórios de vendas.


## Seções de Wiki

-Visão geral sobre o ambiente GCP e o processo.

-Detalhes sobre a Ingestão FULL 

-Detalhes do Processo ETL no Dataform

-Processo de Monitoramento.

## Tecnologia

- APIs
- Serviços GCP: Composer, Cloud Functions, BigQuery, DataForm, Dataflow, Cloud Storage, Secret Manager.

## Ambientes e Execução
## Componentes

O projeto é hospedado na GCP e utiliza uma ampla gama de serviços disponibilizados pela Google, entre eles:

-Google Cloud Composer: é um serviço de orquestração de fluxos de trabalho totalmente gerenciado, baseado no projeto de código aberto Apache Airflow;
-Google Cloud Functions: utilizado para escrever as funções de ingestão e transformação dos dados provenientes de APIs;
-Google Cloud BigQuery: serviço de banco de dados em nuvem, contendo os schemas e a database principal;
-Google Cloud Dataform: utilizado para a realização da ETL, fazendo a transformação dos dados entre camadas;
-Google Cloud Dataflow: recebe os Jobs criados no Airflow, realizando a conexão entre bancos de origem e destino, criação de tabelas e ingestão;
-Google Cloud Storage: armazena os arquivos de configuração, CSV e arquivos temporários se necessário;
-Google Cloud Secret Manager: contém todas as chaves de APIs e bancos de dados.


## Sistema de Origem

Utilizaremos 3 sistema de origem, sendo banco, API e Arquivos.

- MarketPlace : SQL-Server
- Rede Social : API
- Arquivo CSV : SFTP 


# Execução
## Concepção do Processo ETL

O processo foi concebido não só com o objetivo da construção do datalake, mas também para redução de tempo no processo.

## Encadeamento geral do processo - SQL Server

O processo de ingestão do SQL Server é feito da forma em que, é estabelecida uma conexão com o banco de dados da origem, feito uma seleção nos dados colocados ou alterados mais recentes, mantendo esse controle via a tabela de Change Tracking, depois é enviado para as camadas superiores com base no particionamento e por fim feita a transformação dos dados. Fila de processos em ordem de execução, feita pelo orquestrador:

- Conexão com a origem e destino;
- É feito um SELECT no banco de origem com base no último offset salvo, pegando apenas novos dados e jogando na LAND;
- Scripts do Dataform são executados realizando a ETL;
- Camadas RAW e TRUSTED recebem os dados;
- RAW recebe o dado com particionamento mais recente;
- TRUSTED recebe o dado com tipagem correta e devidamente atualizado;
- Ao término do processo, os dados ficam guardados para utilização da Equipe de data viz.

## Encadeamento geral do processo - SFTP

O processo de ingestão do SFTP é feito da forma em que, os arquivos csv são colocados no bucket do GCP via Apache NiFI (Conectando no SFTP do Cliente), após isso é estabelecida uma conexão com esse bucket do GCP que contém os arquivos, então esses dados são enviados para a LAND, após isso feito o dado é passado para as camadas superiores com base no particionamento e por fim feita a transformação dos dados. Fila de processos em ordem de execução, feita pelo orquestrador:

- Arquivos colocados no bucket do GCP via Apache NiFI;
- Conexão com o bucket do GCP;
- Arquivos são processados e enviados para a LAND do BigQuery;
- Scripts do Dataform são executados realizando a ETL;
- Camadas RAW e TRUSTED recebem os dados;
- RAW recebe o dado com particionamento mais recente;
- TRUSTED recebe o dado com tipagem correta e devidamente atualizado;
- Ao término do processo, os dados ficam guardados para utilização da Equipe de data viz.


## Encadeamento geral do processo - APIs

O processo de ingestão das APIs  é feito via funções do Google Cloud Functions em que, os arquivos pegos na resposta das requisições dessas APIs ou são colocados no bucket do GCP, após isso esse arquivo é processado em uma pasta onde ocorrem as transformações necessárias do tipo do arquivo, na DAG é estabelecida uma conexão com esse bucket do GCP que contém os arquivos com os dados de cada API e esses dados são enviados para a LAND. Por fim o dado é passado para as camadas superiores com base no particionamento e feita a transformação dos dados. Fila de processos em ordem de execução, feita pelo orquestrador:

- Arquivos com dados das APIs colocados no bucket do GCP via Google Cloud Functions;
- Conexão com o bucket do GCP;
- Arquivos são processados e enviados para a LAND do BigQuery;
- Scripts do Dataform são executados realizando a ETL;
- Camadas RAW e TRUSTED recebem os dados;
- RAW recebe o dado com particionamento mais recente;
- TRUSTED recebe o dado com tipagem correta e devidamente atualizado;
- Ao término do processo, os dados ficam guardados para utilização da Equipe de data viz.


## Detalhes da execução do processo FULL

O processo FULL consiste na replicação completa dos dados da origem para o destino. Este processo é feito para todos os sistemas pelo menos uma vez, para poder ter todos os dados contidos naquele banco, porém é um teste tecnico vou usar uma vez para extração, podendo ou nao ser usado para alguns database.

As DAGs que realizam esse processo para ingestão de bancos de dados relacionais, contém o nome dataflow_[sistema de origem]_[tipo de database]_full, e são marcadas com a tag "full".


- DAG > dataflow_marketplace_sqlserver_full
- Já as DAGs para a ingestão full de uma API não são diferentes das DAGs de ingestão incremental, apenas no momento que ocorre a primeira execução não é passado nenhuma data desejada para o início das requisições. Preciso ver a documentação para certificar que não existe janela na extração da API.

## Ingestão FULL em Database

Para começar, estabelecemos as variáveis que serão usadas no código no momento de configurações para determinar o sistema de origem, o tipo de carga e o ambiente no qual a DAG está sendo executada.

Enviroment = variable.get("ENV")
sistema_de_origem = "marketplace"
carga = "full"

Para que seja possível capturar as informações no banco de dados da origem é necessário fazer a conexão com o Google Cloud Secret Manager.

def secret_manager(secret_resource_id):
    try:
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=secret_resource_id)
        secret_value = response.payload.data.decode("UTF-8")
        return secret_value
    except Exception as e:
        logging.error(f"Failed to access secret: {secret_resource_id}, due to: {e}")
        raise e

Essa função recebe a URL de conexão para o Secret Manager, de modo a recuperar as credenciais armazenadas, as quais são essenciais para acessar o banco de dados de origem e obter as informações desejadas.

Em seguida, é necessário saber quais tabelas serão ingeridas nessa DAG, então é feito um SELECT da tabela de controle de apenas as tabelas que contém o status “ativo”, o sistema de origem que foi passado pela variável e o tipo de carga.

Posteriormente, é definido as variáveis que irão conter todas as informações das tabelas retornadas na função anterior, e a partir disso, cria uma lista com os campos de cada tabela.

Posto isso, é feita uma lista com as tabelas da origem para a criação dos Jobs no Google Cloud Dataflow. Finalizando a etapa de configurações no código, definimos as variáveis globais dependentes dos valores nos campos da tabela de controle.


# variaveis globais
project_id = controleetl_campo("projeto")
region = controleetl_campo("projeto_regiao")
tipo = controleetl_campo("tipo")
sistema_origem = controleetl_campo("sistema_origem")
tipo_banco_origem = controleetl_campo("tipo_banco_origem")
max_tasks = controleetl_campo("airflow_max_task")
tipo_carga = controleetl_campo("tipo_carga")
ultimo_offset = controleetl_campo("campo_controle_carga")
tipo_carga_job = tipo_carga[:4]

Posto isso, começa a criação dos Jobs, passando os parâmetros para criação (número de workers, tipo de máquina que será utilizada, tamanho da máquina, entre outros). É definido um ID hash para o Job, cujo nome é montado da seguinte forma [nome do sistema]_[nome do banco de dados da origem]_[nome da tabela]_[full/incr]_[tipo de máquina]_[ID hash do job]. Após a criação bem-sucedida do Job, o seu ID é retornado para possibilitar a verificação do estado da sua execução, caso a criação não seja bem sucedida o ID é retornado como erro.

Para que ocorra a continuidade no processo de ingestão, é necessário fazer a checagem da execução dos Jobs pois assim que todos terminarem, é finalizada a DAG de ingestão full.

A verificação ocorre da seguinte maneira: na função “run” um status “True” é atribuído quando o Job inicia a execução e é utilizado como condição de parada para o laço, em seguida, a função "call_api_status_dataflow" é utilizada para consultar o Google Cloud Dataflow com base no ID do Job, a fim de verificar o status do mesmo. Esse status é dividido em duas categorias: o status de execução (aguardando, em andamento e cancelando) e o status de conclusão (concluído, com falha e cancelado).

# Check states of Dataflow Job
        while status == True:
            try:
                logging.info(f'Ckeck status of Job ID: {job_id}')
                status_job = call_api_status_dataflow(project_id, job_id, region)
                logging.info(f'Status Job Id: {job_id} - {status_job}')

                if status_job in executing_states:
                    pass
                elif status_job in final_states:
                    if status_job == "JOB_STATE_FAILED" or status_job == "JOB_STATE_CANCELLED":
                        status = False
                        raise AirflowFailException("Job failed or was cancelled")
                    else:
                        status = False
                elif status_job == 'ERR':
                    logging.error("Maximum retries reached. Failing the task.")
                    raise AirflowFailException("Maximum retries reached. Job status couldn't be determined.")
                time.sleep(120)
            except Exception as e:
                logging.info(e)
                raise
            # calcula a duracao total da task
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()


Se o status do Job no Dataflow estiver em qualquer uma das categorias de status de execução, ele deverá continuar a execução normalmente e realizar a próxima verificação. No entanto, se o status estiver em alguma das categorias de status de conclusão, a verificação será feita para determinar se o Job foi concluído, cancelado ou se falhou. No caso de cancelamento ou falha, será retornado um erro no job que assumirá uma coloração avermelhada, e o processo de ingestão da tabela será encerrado com a definição do status como “False” e retornará a duração deste Job. Isso pode resultar na conclusão da execução de toda a DAG (caso seja a última tabela a ser ingerida), retornando também o tempo de execução do Job.



## Ingestão FULL em APIs


Para começar, estabelecemos as variáveis que serão usadas no código no momento de configurações para determinar o sistema de origem, o tipo de carga e o ambiente no qual a DAG está sendo executada.

sistema_de_origem = "redesocial"
típo_carga = "full"
env = Variable.get("ENV")

Após a configuração dessas variáveis, uma solicitação é enviada à origem da API para obter um token essencial, com objetivo de termos a autorização das requisições do endpoint em que esta DAG realizará a ingestão de dados. Depois de concluir a autenticação, é chamada a função do Google Cloud Functions que será a responsável por fazer toda a parte de obtenção dos dados até o bucket do Google Cloud Storage.


def invoke_cloud_function(api_uri, endpoint):
    print('****** api_uri:', api_uri)

    request = google.auth.transport.requests.Request()
    print('****** request:', request)

    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(api_uri, request=request)
    print('****** id_token_credentials:', id_token_credentials)

    try:
        body = {"endpoint": endpoint}
        resp = AuthorizedSession(id_token_credentials).post(url=api_uri, json=body)
        print('****** resp:', resp)
        print('****** resp body:', resp.text)
        if resp.status_code == 200:
            return resp
        else:
            raise AirflowFailException("API Job failed. Aborting the Pipeline")

    except Exception as e:
        print('****** Exception:', e)


Quando uma solicitação resulta em um arquivo vazio, este arquivo é preenchido com uma representação em formato JSON contendo informações de erro. Além disso, uma mensagem de erro é impressa para facilitar a compreensão durante a depuração, esclarecendo o que aconteceu.

A função “run“, em resumo, é utilizada para confirmação do recebimento dos dados da API via Google Cloud Functions, e assim confirmação de que a ingestão foi concluída com sucesso. Nessa função, são realizadas verificações para lidar com diferentes cenários: se o retorno estiver vazio, se a requisição não for bem-sucedida ou se for bem-sucedida e contiver dados:

A função verifica se o retorno da requisição está vazio. Se isso ocorrer, é chamada a função "save_empty_to_bucket", conforme explicada anteriormente, e o código continua sua execução.
Se a requisição não for bem-sucedida, a função registra uma mensagem de erro, salva um arquivo vazio e lança uma exceção, o que levará o Airflow a considerar a tarefa como falha.
Em caso de sucesso, o retorno da requisição é salvo em um arquivo JSON no bucket.

## Detalhes do processo ETL no Dataform

O Google Cloud Dataform contém os scripts de transformação de cada camada do sistema, partindo da LAND e indo até a REFINED.

Para acessar o Dataform, é necessário pesquisar "Dataform" na área de pesquisa do GCP. Assim abrindo uma aba com vários repositórios, cada um contendo os scripts de cada sistema correspondente.


## Camada Land para Raw

O processo da transição da camada LAND para a RAW utiliza uma data de particionamento para controlar a inserção de dados, garantindo que somente os dados mais recentes e/ou aqueles que ainda não foram ingeridos sejam incluídos.

Assim, a consulta SQL no BigQuery utiliza scripts SQL para executar instruções de inserção (INSERT) na camada RAW, mantendo a tipagem dos dados como strings e preservando as colunas de "change tracking" (monitoramento de mudanças) de cada registro da camada LAND. Neste cenário, as tabelas não precisam ser criadas antecipadamente no BigQuery; quando é feita a primeira ingestão elas são criadas e populadas com os dados totais contidos na LAND. Dessa forma, se a tabela não existir no BigQuery, será feita uma transição FULL da camada LAND para a RAW, onde a transição por particionamento é apenas iniciada após a primeira ingestão


config {
type: "operations",
hasOutput: true
}
INSERT INTO
  `RAW.marketplace_vendas` (
    produto_id,
    data_venda,
    quantidade,
    valor_total,
    PARTITIONTIME )
WITH
  TABELA AS (
  SELECT
    produto_id,
    data_venda,
    quantidade,
    valor_total,
    _PARTITIONTIME AS PARTITIONTIME
  FROM
    `LAND.marketplace_vendas` ),
  TABELA_B AS(
  SELECT
    MAX(PARTITIONTIME) AS PARTITIONTIME
  FROM
    `RAW.marketplace_vendas` )
SELECT
  a.*
FROM
TABELA a WHERE a.PARTITIONTIME = (SELECT MAX(PARTITIONTIME) FROM TABELA)



## Camada RAW para TRUSTED

Posteriormente, na transferência de dados da camada RAW para a camada TRUSTED de cada sistema, é utilizado um script SQL de "Merge." Esse script faz uso do ID e das informações de alteração de cada registro para verificar se o ID do novo registro está presente. Se o ID for confirmado, um comando de atualização (UPDATE) é executado no registro da camada TRUSTED. Caso o ID não exista, um comando de inserção (INSERT) é utilizado para adicionar esse novo registro à camada TRUSTED.

config {
  type: "table",
  schema: "TRUSTED",
  description: "Tabela criada e gerenciada pelo Dataform",
  dependencies: ["marketplace_raw_vendas"]

}

SELECT
  DISTINCT *
FROM (
  SELECT
        cast(produto_id AS string) AS produto_id,
        cast(data_venda AS TIMESTAMP) AS data_venda,
        cast(quantidade AS int64) AS quantidade,
        cast(valor_total AS FLOAT64) AS valor_total,
  FROM (
    SELECT
      DISTINCT A.*
    FROM
      `RAW.marketplace_vendas` A )
  WHERE
    PARTITIONTIME = (
    SELECT
      MAX(PARTITIONTIME)
    FROM
      `RAW.marketplace_vendas`) )
ORDER BY
  cast(produto_id AS string)



Ficando a mesma logica para a redesocial (API), lembrando que o bigquery suporta array.
então na camada LAND E RAW ficara dentro dos campos *Sentiment, *product e *user seus array.
e na camada TRUSTED sera feita a criação sem destinção.


IMAGEM.



# Processo de monitoramento

## Monitoramento automático de execução.

** Ideia de monitoramento **

Exemplo de uso.
Estou apenas descrevendo possiveis ideias para monitorar o ambiente automáticamente.



DAG:

Criação de dag, Exemplo: A DAG "airflow_monitoring" é executado a cada 10 minutos para verificar o funcionamento com sucesso das DAGs do Airflow.

** Cloud Functions ** :

- A função "function-monitoring-dags-run" é executada a cada hora para monitorar o funcionamento das DAGs do airflow, enviar essa informação para o SQL e disponibilizar isso como visualização no grafana.

A função "function-monitoring-bucket-arquivos_CSV" é executada todos os dias às 7 horas da manhã para verificar se os documentos dos sistemas SFTP estão atualizados no bucket.

** Alertas do Zabbix ** :

- Os alertas ligados aos sistemas SFTP são referentes às datas de atualização dos arquivos existentes nas respectivas pastas do bucket, indicando que o arquivo em questão no alerta não está atualizado no sistema.

-Os alertas ligados às DAGs são referentes ao sucesso ou não das execuções das DAGs, indicando que a DAG em questão no alerta falhou.

-O alerta ligado ao bucket é referente à data de última atualização do bucket que contém os arquivos utilizados para definir as regras e os critérios, indicando que algum arquivo existente nesse bucket foi alterado.

FIM.
