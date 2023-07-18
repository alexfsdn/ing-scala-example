name := "ingestion"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.8"
val configVersion = "1.3.3"
val junitVersion = "4.12"
val mockitoVersion = "1.10.19"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
libraryDependencies += "com.typesafe" % "config" % configVersion % Provided

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Test

libraryDependencies += "junit" % "junit" % junitVersion % Test
libraryDependencies += "org.mockito" % "mockito-all" % mockitoVersion % Test
