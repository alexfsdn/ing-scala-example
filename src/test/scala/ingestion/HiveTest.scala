package ingestion

import org.junit.{After, Before, Test}
import org.apache.spark.sql.{SQLContext, SparkSession}

class HiveTest {

  private var hiveContext: SQLContext = null;

  @Before
  def setup(): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .config("spark.sql.catalogImplementation", "hive")
      .enableHiveSupport()
      .getOrCreate()

    hiveContext = spark.sqlContext

    hiveContext.sql("DROP DATABASE IF EXISTS databasetest")
    hiveContext.sql("CREATE DATABASE IF NOT EXISTS databasetest")
    hiveContext.sql("USE databasetest")
  }

  @After
  def cleanup(): Unit = {
    hiveContext.sql("DROP TABLE IF EXISTS table_test")
  }

  @Test
  def test(): Unit = {
    hiveContext.sql("CREATE TABLE IF NOT EXISTS table_test (id INT, name STRING, dat_ref STRING)")
    hiveContext.sql("INSERT INTO table_test VALUES (1, 'Alex', '20230205')")
    hiveContext.sql("INSERT INTO table_test VALUES (2, 'Bruna', '20230205')")

    val result = hiveContext.sql("SELECT * FROM table_test")

    result.show(2, false)
    assert(result.count() == 2)
  }

}
