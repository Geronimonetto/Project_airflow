# Projeto Dados Eólicos

O projeto é centrado em dados gerados por uma turbina eólica, os quais são continuamente produzidos. As DAGs (Directed Acyclic Graphs) do projeto realizarão um processo básico de ETL, começando pela geração desses dados, passando pela transformação e finalizando com o armazenamento no banco de dados Postgres.

Para verificar a existência desses dados, serão utilizados sensores. Um operador Python será empregado para executar um branch, contendo duas condições. Dependendo do resultado desse branch, um e-mail será enviado utilizando o operador EmailOperator para sinalizar se o processo foi concluído com êxito ou se ocorreu algum erro durante a execução. Alternativamente, será empregado o PostgresOperator, responsável pela criação do banco de dados e pela inserção dos dados.

Resumindo, o fluxo de trabalho consistirá em:

1. Geração contínua de dados pela turbina eólica.
2. Utilização de sensores para verificar a presença desses dados.
3. Execução de operações de ETL para transformação dos dados.
4. Armazenamento dos dados transformados no banco de dados Postgres.
5. Emprego do operador Python para realizar um branch com duas condições.
6. Envio de e-mails utilizando o operador EmailOperator para sinalizar o resultado do processo (sucesso ou erro).
7. Em caso de erro no branch, emprego do PostgresOperator para criar o banco de dados e inserir os dados.




