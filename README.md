# ing-scala-example

Esse projeto tem 2 objetivos simples:

1. Apresentar como podemos construir um código/fluxo para a ingestão de dados completo fazendo uso apenas da memória do spark (você pode encontrar a ingestão implementada aqui - > https://github.com/alexfsdn/ing-example/blob/main/src/main/scala/ingestion/process/ProcessIngestion.scala).

![image](https://user-images.githubusercontent.com/51302698/219902183-154301ef-7e6f-4358-af06-b5824d47e247.png)


2. Apresentar como podemos criar testes unitários em aplicações spark, garantindo uma boa testabilidade.

   Para conseguir a testabilida do fluxo da ingestão de dados optei em utilizar o paradigma orientação a objetos, assim posso fazer o uso de injeção de dependência, isso nos possibilita mockar determinados componentes que acessam recursos externos a nossa aplicação, que, no caso, não é interessante para nossos testes unitários que tem como objetivo testar o fluxo apenas (você pode encontrar o teste unitário do ProcessIngestion mencionado acima aqui - > https://github.com/alexfsdn/ing-example/blob/main/src/test/scala/ingestion/process/ProcessIngestionTest.scala).

![image](https://user-images.githubusercontent.com/51302698/219902132-54016da0-73cc-4e2d-9230-694e3ce94e72.png)

**ATENÇÃO**
*****HIVE EM MEMÓRIA*****.
NESSE PRÓXIMO TESTE QUE SERÁ APRESENTADO ABAIXO, O QUAL, TESTA A MESMA CLASSE/O MESMO MÉTODO DO TESTE APRESENTANDO ACIMA, FAZ O USO DO HIVE EM MEMÓRIA, OU SEJA, É CRIADO BANCO DE DADOS E TABELAS EM MEMÓRIA, E COM O SPARK CONSEGUIMOS FAZER INSERÇÕES NESSAS TABELAS CRIADAS E CONSULTAR DADOS INSERIDOS POR NÓS.

https://github.com/alexfsdn/ing-scala-example/tree/main/src/test/scala/ingestion/hive

![image](https://user-images.githubusercontent.com/51302698/235801986-d0c38791-71f1-44b6-8e20-989ed7b469a1.png)

OBS: NÃO SE PREOCUPE COM A MEMÓRIA QUE É USADA, POIS APÓS CADA TESTE É EXECUTADO O METODO CLEANUP SINALIZADO NOS TESTES "@After", assim garantido que não enche nossa memória.

IDE Intellij
Java 1.8
Scala-sdk-2.11.12
Spark 2.4.7
SBT
JUnit (ao invés do Scala Test)
Mockito
Windows 10
8GB RAM (MAS 4GB RAM DEVE SER O SUFICIENTE PARA NÃO TER PROBLEMA)

TENDO OS RECURSOS ACIMA, BASTA EXECUTAR GIT CLONE PARA BAIXAR O REPOSITÓRIO NA SUA MÁQUINA E IR NAS CLASSES DE TESTES E EXECUTAR, NÃO PRECISA DE MAIS NADA.

*POR QUE CONSTRUIR INGESTÃO DE DADOS DESSA FORMA SERIA BOM SE POSSO NA MINHA EMPRESA TENHO UM AMBIENTE E POSSO ACESSAR O SPARK-SHELL PARA CONSTRUIR MESMOS CÓDIGOS TESTADOS?**

1. DOCUMENTAÇÃO: OS TESTES SERVEM COMO DOCUMENTAÇÃO, ISSO AJUDARÁ O PROGRAMADOR (PRINCIPALMENTE OS JUNIORS) ENTENDER FACILMENTE O COMPORTAMENTO DO CÓDIGO.
2. DEBUG: O DEBUG É FUNDAMENTAL PARA ENTENDIMENTO COM PRECISÃO EM RELAÇÃO AO COMPORTAMENTO DO PROCESSO DE INGESTÃO, O QUE PODE SER IMPRESCINDÍVEL EM UMA ATUALIZAÇÃO MAIS COMPLICADA. 
3. FÁCIL MANUTENÇÃO E CONFIABILIDADE NA NÃO GERAÇÃO DE BUG NO MOMENTO DA MANUTENÇÃO: UMA APLICAÇÃO COM O PASSAR DO TEMPO GERALMENTE PRECISA TER ATUALIZAÇÕES E COM TESTES MAPEADOS, APÓS O AJUSTE OS TESTE IRÃO GARANTIR QUE O AJUSTE NÃO GEROU UM BUG.
4. AMBIENTE INDISPONÍVEL: INDISPONIBILIDADE DE AMBIENTE NÃO IRÁ SER IMPEDIMENTO PARA COMEÇAR O DESENVOLVIMENTO.

  Apesar da ideia desse pequeno projeto é demonstrar como podemos fazer uso apenas da memória para construir nossas aplicação de ingestão com Spark, é claro que você pode gerar um jar e testar em um ambiente que tenha Hdfs, Spark e Hive, irá funcionar, eu testei!! Para isso deve olhar para o arquivo https://github.com/alexfsdn/ing-example/blob/main/src/main/resources/application.properties e configurar os parâmetros de acordo com o seu ambiente ou criar outro arquivo e apontar no seu spark-submit para que use o seu ".properties" ou ".conf". E o seu start se encontra aqui -> https://github.com/alexfsdn/ing-example/blob/main/src/main/scala/ingestion/trigger/Trigger.scala aqui conseguirá ver nitidamente os recursos reais do hdfs e do spark. Na aplicação tenho implementado o código que lê arquivos no hdfs e temos o código do spark que faz ingestão dos dados, e, outras coisas mais.

Esse readme orienta apenas como testar o ProcessIngestion, porém existe outras classes/processos que você pode explorar.

ATENÇÃO para um problema improvavél, mas que porvetura possa vir acontecer com alguém:

Para os testes com os mocks, basta passarmos o path a partir da pasta "src", como por exemplo, "src/test/resources/mock_example_20220812.csv", porém se tiver problemas e seu teste não conseguir encontrar o arquivo, você pode fazer de um jeito diferente conforme apresentado aqui -> https://github.com/alexfsdn/ing-example/blob/main/src/test/scala/ingestion/SparkLocalTest.scala

"um  pedaço do código"

![image](https://user-images.githubusercontent.com/51302698/219902209-27964dbf-e315-4a71-a2fa-4a7851c5750f.png)

BASICAMENTE GARANTO QUE ELE RASTREIE O PATH COMPLETO ANTES DE PASSAR NO READ DO SPARK, ASSIM NESSAS CIRCUNSTÂNCIAS, ENCONTRAR O NOSSO MOCK PARA OS TESTES.


