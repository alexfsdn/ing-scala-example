package ingestion.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.junit.{After, Before, Test}

import java.time.LocalDate

class HiveDataFrameTest {

  private var hiveContext: SQLContext = null;

  @Before
  def setup(): Unit = {
    val conf = new SparkConf().setAppName("App Name example prod")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.some.config.option", "some-value")
      .set("spark.sql.catalogImplementation", "hive")

    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    hiveContext = spark.sqlContext

    hiveContext.sql("DROP DATABASE IF EXISTS databasetest")
    hiveContext.sql("CREATE DATABASE IF NOT EXISTS databasetest")
    hiveContext.sql("USE databasetest")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS databasetest.table_test (name STRING, age STRING, cpf STRING, dat_ref STRING, dat_partition STRING)")
  }

  @After
  def cleanup(): Unit = {
    hiveContext.sql("DROP TABLE IF EXISTS databasetest.table_test")
  }

  @Test
  def saveDataFrameTest(): Unit = {
    val PATH = "src/test/resources/mock_hive_example_20230502.csv"

    val dataFrameExample = hiveContext.read.option("header", "true")
      .option("delimiter", ";").csv(PATH)

    val result = dataFrameExample.withColumn("dat_partition", lit(LocalDate.now().toString)).persist(StorageLevel.MEMORY_ONLY)

    result.show(5, false)

    result.write.format("orc").mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "dynamic")
      .insertInto(s"databasetest.table_test")

    val consult = hiveContext.sql("select * from databasetest.table_test").persist(StorageLevel.MEMORY_ONLY)

    consult.show(5, false)

    assert(consult.count() == result.count())

    result.unpersist()
    consult.unpersist()
  }

}
