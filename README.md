# Data Lake .

# O Projeto

Teste tecnico de engenheiro de dados.
Criação de um datalake
A operação consiste em projetar e implementar uma infraestrutura no ambiente da GCP robusta
que suporte as operações de negócio, desde o registro de produtos até promoções e relatórios de vendas.


## Seções de Wiki

-Visão geral sobre o ambiente GCP e o processo.

-Detalhes do Processo ETL no Dataform

-Processo de Monitoramento.

## Tecnologia

- APIs
- Serviços GCP: Composer, Cloud Functions, BigQuery, DataForm, Dataflow, Cloud Storage, Secret Manager.


![imagem](https://github.com/pandex7/teste-Tecnico/blob/main/assets/1.png)




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

- Pub/Sub (fila GPS)
- Cloud Functions (Processa GPS e API)
- Arquivo Json : SFTP 


# Execução
## Concepção do Processo ETL

O processo foi concebido não só com o objetivo da construção do datalake, mas também para redução de tempo no processo.


## Encadeamento geral do processo - SFTP

O processo de ingestão do SFTP é feito da forma em que, os arquivos Json são colocados no bucket do GCP via Apache NiFI (Conectando no SFTP do Cliente), após isso é estabelecida uma conexão com esse bucket do GCP que contém os arquivos, então esses dados são enviados para a LAND, após isso feito o dado é passado para as camadas superiores com base no particionamento e por fim feita a transformação dos dados. Fila de processos em ordem de execução, feita pelo orquestrador:

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





## Camada RAW para TRUSTED

Posteriormente, na transferência de dados da camada RAW para a camada TRUSTED de cada sistema, é utilizado um script SQL de "Merge." Esse script faz uso do ID e das informações de alteração de cada registro para verificar se o ID do novo registro está presente. Se o ID for confirmado, um comando de atualização (UPDATE) é executado no registro da camada TRUSTED. Caso o ID não exista, um comando de inserção (INSERT) é utilizado para adicionar esse novo registro à camada TRUSTED.




Ficando a mesma logica para a redesocial (API), lembrando que o bigquery suporta array.
então na camada LAND E RAW ficara dentro dos campos *Sentiment, *product e *user seus array.
e na camada TRUSTED sera feita a criação sem destinção.





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
