# Projeto Dados Eólicos

O projeto é centrado em dados gerados por uma turbina eólica, os quais são continuamente produzidos. As DAGs (Directed Acyclic Graphs) do projeto realizarão um processo básico de ETL, começando pela geração desses dados, passando pela transformação e finalizando com o armazenamento no banco de dados PostgresSQL.

Para verificar a existência desses dados, serão utilizados sensores. Um operador Python será empregado para executar um branch, contendo duas condições. Dependendo do resultado desse branch, um e-mail será enviado utilizando o operador EmailOperator para sinalizar se a temperatura está dentro dos conformes. Alternativamente, será empregado o PostgresOperator, responsável pela criação do banco de dados e pela inserção dos dados.

Resumindo, o fluxo de trabalho consistirá em:

1. Geração contínua de dados pela turbina eólica.
2. Utilização de sensores para verificar a presença desses dados.
3. Execução de operações de ETL para transformação dos dados.
4. Armazenamento dos dados transformados no banco de dados PostgresSQL.
5. Emprego do operador Python para realizar um branch com duas condições.
6. Envio de e-mails utilizando o operador EmailOperator
	-  Se o valor da temperatura for maior ou igual de 24 º, é enviado um email sinalizando crítico.
	- Se for abaixo desse valor, será um email apenas de sinalização casual.
7. Emprego do PostgresOperator para criar o banco de dados e inserir os dados.

### Passo a Passo

Primeiramente será necessário criar uma variável no Airflow que será o caminho onde estão localizado(s) o(s) arquivo(s) que serão como base para o projeto.

1. **Airflow** > Admin > Variables
2. **key**: path_file, **Val**: caminho ex: /opt/airflow/data/data.json  - este caminho serão criados os arquivos da turbina

Crie uma conexão
1. **Airflow** > Connection > Crie uma nova conexão
2. **Connection id**: fs_default, **Connection Type**: File(path), **Host**: Caminho do arquivo: /opt/airflow/data/data.json






