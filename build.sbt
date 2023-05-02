name := "ingestion"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7" % Provided
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % Provided
libraryDependencies += "com.typesafe" % "config" % "1.3.3" % Provided

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % Test
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7" % Test

libraryDependencies += "junit" % "junit" % "4.12" % Test
libraryDependencies += "org.mockito" % "mockito-all" % "1.10.19" % Test
