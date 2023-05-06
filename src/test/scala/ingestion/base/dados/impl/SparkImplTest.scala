package ingestion.base.dados.impl

import org.apache.spark.sql.{SparkSession}
import org.junit.{After, Before, Test}

class SparkImplTest {

  private var spark: SparkSession = null;
  private val TABLE_NAME = "databasesparkimpl.table_i_spark_impl"

  @Before
  def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .config("spark.sql.catalogImplementation", "hive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("DROP DATABASE IF EXISTS databasesparkimpl")
    spark.sql("CREATE DATABASE IF NOT EXISTS databasesparkimpl")
  }

  @After
  def cleanup(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS  $TABLE_NAME ")
  }

  @Test
  def saveTest(): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $TABLE_NAME (id INT, name STRING, dat_ref STRING)")

    val dados = Seq(
      (1, "Alex", "2022-01-01"),
      (2, "Bruna", "2022-02-01"),
      (3, "Carla", "2022-03-01")
    )
    val df = spark.createDataFrame(dados)
      .toDF("id", "name", "dat_ref")

    val iSpark = new SparkImpl(spark)
    iSpark.save(dataFrame = df, tableName = TABLE_NAME)

    val getDFSaved = iSpark.get(s"select * from $TABLE_NAME limit 3")

    assert(getDFSaved.count() == 3)
  }
}
