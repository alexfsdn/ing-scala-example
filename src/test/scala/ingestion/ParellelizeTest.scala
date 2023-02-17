package ingestion

import org.apache.spark.sql.SparkSession
import org.junit.Test

class ParellelizeTest {

  @Test
  def test(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Range(0, 20))
    println("From local[5]" + rdd.partitions.size)

    val rdd1 = spark.sparkContext.parallelize(Range(0, 20), 6)
    println("parallelize : " + rdd1.partitions.size)

    val rddFromFile = spark.sparkContext.textFile("src/test/resources/test.txt", 10)
    println("TextFile : " + rddFromFile.partitions.size)
  }

}
