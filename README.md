# ing-example

Esse projeto tem alguns objetivos simples:

1. Apresentar uma simples ingestão de dados em uma tabela do hive (você pode encontrar a ingestão implementada aqui - > https://github.com/alexfsdn/ing-example/blob/main/src/main/scala/ingestion/process/ProcessIngestion.scala).

2. Apresentar como podemos criar testes unitários e trabalhar com o spark apenas utilizando memória, assim, sendo necessário adotar o paradigma orientação a objeto e aplicando na prática a injeção de dependência, possibilitando mockar determinados componentes, os quais, acessam recursos externos a nossa aplicação, que, no qual, não é interessante para nossos testes unitários, que tem como objetivo testar o fluxo apenmas (você pode encontrar o teste unitário do ProcessIngestion mencionado acima aqui - > https://github.com/alexfsdn/ing-example/blob/main/src/test/scala/ingestion/process/ProcessIngestionTest.scala). 


3. Apresentar como é possível usar/agregar de conceitos/métodos de engenharia de software na construção de um processo de engenharia de dados.

O que eu usei?

IDE intellij
Java 1.8
Scala-sdk-2.11.12
Spark 2.4.7
SBT
JUnit (invés do Scala Test)
Mockito
Windows 10

Apesar da ideia é demonstrar como podemos fazer uso apenas da memória para construir nossa aplicação de ingestão com Spark, é claro que você pode gerar um jar e testar em um ambiente que tenha o Spark, irá funcionar, eu testei!! Para isso deve olhar para o arquivo https://github.com/alexfsdn/ing-example/blob/main/src/main/resources/application.properties e configurar os parâmetros de acordo com o seu ambiente. E o seu start se encontra aqui -> https://github.com/alexfsdn/ing-example/blob/main/src/main/scala/ingestion/trigger/Trigger.scala aqui conseguirá ver nitidamente os recursos reais do hdfs e do spark. Na aplicação temos implementado o código que lê arquivos no hdfs e temos código do spark que faz ingestão dos dados e outras coisas mais.

Esse readme orienta apenas como testar o ProcessIngestion, porém existe outras classes/processos que envetualmente decidi subir para guardar, esses processos cabe você explorar caso tenha interesse.
