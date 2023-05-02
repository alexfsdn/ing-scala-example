# ing-scala-example

Esse projeto tem alguns objetivos simples:

1. Apresentar uma simples ingestão de dados em uma tabela do hive (você pode encontrar a ingestão implementada aqui - > https://github.com/alexfsdn/ing-example/blob/main/src/main/scala/ingestion/process/ProcessIngestion.scala).


![image](https://user-images.githubusercontent.com/51302698/219902183-154301ef-7e6f-4358-af06-b5824d47e247.png)


2. Apresentar como podemos criar testes unitários em aplicações spark apenas utilizando a memória, assim, sendo necessário adotar o paradigma orientação a objetos e fazer o uso de injeção de dependência, possibilitando mockar determinados componentes, os quais, acessam recursos externos a nossa aplicação, que, no qual, não é interessante para nossos testes unitários, que tem como objetivo testar o fluxo apenas (você pode encontrar o teste unitário do ProcessIngestion mencionado acima aqui - > https://github.com/alexfsdn/ing-example/blob/main/src/test/scala/ingestion/process/ProcessIngestionTest.scala). 


![image](https://user-images.githubusercontent.com/51302698/219902132-54016da0-73cc-4e2d-9230-694e3ce94e72.png)


3. Apresentar como é possível usar/agregar de conceitos/métodos de engenharia de software na construção de um processo de engenharia de dados.

O que eu usei?

IDE intellij
Java 1.8
Scala-sdk-2.11.12
Spark 2.4.7
SBT
JUnit (ao invés do Scala Test)
Mockito
Windows 10

Apesar da ideia desse pequeno projeto é demonstrar como podemos fazer uso apenas da memória para construir nossas aplicação de ingestão com Spark, é claro que você pode gerar um jar e testar em um ambiente que tenha hdfs e Spark, irá funcionar, eu testei!! Para isso deve olhar para o arquivo https://github.com/alexfsdn/ing-example/blob/main/src/main/resources/application.properties e configurar os parâmetros de acordo com o seu ambiente. E o seu start se encontra aqui -> https://github.com/alexfsdn/ing-example/blob/main/src/main/scala/ingestion/trigger/Trigger.scala aqui conseguirá ver nitidamente os recursos reais do hdfs e do spark. Na aplicação tenho implementado o código que lê arquivos no hdfs e temos o código do spark que faz ingestão dos dados, e, outras coisas mais.

Esse readme orienta apenas como testar o ProcessIngestion, porém existe outras classes/processos que envetualmente decidi subir para guardar, esses processos cabe você explorar caso tenha interesse.

ATENÇÃO, CASO OS TESTES COM MOCKS NÃO TE SATISFAZEM VOCÊ PODE OPTAR POR *****HIVE EM MEMÓRIA*****.
PARA ISSO ACESSE O LINK ABAIXO E CONFORME P PATH INDICADO NO PRINT APRESNETA.
NESSES TESTES COM HIVE CRIAMOS TABELAS EM MEMÓRIA, O SPARK FAZ INSERÇÕES E VOCÊ PODE CONSULTAR OS RESULTADOS POSTERIORMENTE.

https://github.com/alexfsdn/ing-scala-example/tree/main/src/test/scala/ingestion/hive

![image](https://user-images.githubusercontent.com/51302698/235801986-d0c38791-71f1-44b6-8e20-989ed7b469a1.png)


ATENÇÃO para um problema improvavél, mas que porvetura possa vir acontecer com alguém:

Para os testes com os mocks, basta passarmos o path a partir da pasta "src", como por exemplo, "src/test/resources/mock_example_20220812.csv", porém se tiver problemas e seu teste não conseguir encontrar o arquivo, você pode fazer de um jeito diferente conforme apresentado aqui -> https://github.com/alexfsdn/ing-example/blob/main/src/test/scala/ingestion/SparkLocalTest.scala

"um  pedaço do código"

![image](https://user-images.githubusercontent.com/51302698/219902209-27964dbf-e315-4a71-a2fa-4a7851c5750f.png)




