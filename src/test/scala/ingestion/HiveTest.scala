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

    hiveContext.sql("CREATE DATABASE IF NOT EXISTS test")
    hiveContext.sql("USE test")
  }

  @After
  def cleanup(): Unit = {
    hiveContext.sql("DROP TABLE IF EXISTS test_table")
  }

  @Test
  def test(): Unit = {
    hiveContext.sql("CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING)")
    hiveContext.sql("INSERT INTO test_table VALUES (1, 'Alex')")
    hiveContext.sql("INSERT INTO test_table VALUES (2, 'Bruna')")
    hiveContext.sql("INSERT INTO test_table VALUES (3, 'Soraya')")
    hiveContext.sql("INSERT INTO test_table VALUES (4, 'Carlos')")
    hiveContext.sql("INSERT INTO test_table VALUES (5, 'Duda')")

    val result = hiveContext.sql("SELECT * FROM test_table")

    result.show(5, false)
    assert(result.count() == 5)
  }

}
