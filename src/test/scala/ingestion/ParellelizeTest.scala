package ingestion

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.junit.Test

class ParellelizeTest {

  //@Test
  def test(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val rdd: RDD[Int] = spark.sparkContext.parallelize(Range(0, 20))
    println("From local[5]" + rdd.partitions.size)

    val rdd1: RDD[Int] = spark.sparkContext.parallelize(Range(0, 20), 6)
    println("parallelize : " + rdd1.partitions.size)

    val rddFromFile: RDD[String] = spark.sparkContext.textFile("src/test/resources/test.txt", 10)
    println("TextFile : " + rddFromFile.partitions.size)

    //rdd1.saveAsTextFile("/tmp/partition")

    val rdd2: RDD[Int] = rdd1.repartition(4)
    println("Repartition size : " + rdd2.partitions.size)
    //rdd2.saveAsTextFile("/tmp/re-partition")

    val rdd3: RDD[Int] = rdd1.coalesce(4)
    println("Repartition size : " + rdd3.partitions.size)
    rdd3.saveAsTextFile("/tmp/coalesce")


  }

  @Test
  def testDataFrame(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val df: Dataset[java.lang.Long] = spark.range(0, 20)
    println(df.rdd.partitions.length)

    df.write.mode(SaveMode.Overwrite) csv ("partition.csv")
  }

}
