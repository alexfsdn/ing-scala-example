package ingestion.base.dados.impl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
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
    spark.sql(s"DROP DATABASE IF EXISTS databasesparkimpl CASCADE")
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

    val getDFSaved = iSpark.get(s"select * from $TABLE_NAME ")

    assert(getDFSaved.count() == 3)
  }

  @Test
  def getTest(): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $TABLE_NAME (id INT, name STRING, dat_ref STRING)")
    spark.sql(s"INSERT INTO $TABLE_NAME VALUES (1, 'Alex', '20230205')")
    spark.sql(s"INSERT INTO $TABLE_NAME VALUES (2, 'Bruna', '20230205')")

    val getDFSaved = new SparkImpl(spark).get(s"select * from $TABLE_NAME ")
    val onlyAlexDf = getDFSaved.filter(col("name") === lit("Alex"))

    assert(getDFSaved.count() == 2)
    assert(onlyAlexDf.first().getString(1) == "Alex")
  }

  @Test
  def getHiveSchemaTest(): Unit = {
    val columns: Array[String] = Array("id INT, name STRING, cpf STRING, age STRING, cel STRING, dat_ref STRING, v_1 DOUBLE, v_2 DOUBLE, v_3 BIGINT, timestamp STRING, partition_date STRING")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $TABLE_NAME (${columns.mkString(",")})")

    val schema = new SparkImpl(spark).getHiveSchema(TABLE_NAME, "partition_date", "timestamp")

    println(schema)
    assert(schema.size == 9)
  }


  @Test
  def getColListTest(): Unit = {
    val columnsToInsert: Array[String] = Array("id INT, name STRING, dat_ref STRING")
    val columns: Array[String] = Array("id,name,dat_ref")
    val partitions: Array[String] = Array("'20230205','20230204','20230203'")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $TABLE_NAME (${columnsToInsert.mkString(",")})")
    spark.sql(s"INSERT INTO $TABLE_NAME VALUES (1, 'Alex', '20230205')")
    spark.sql(s"INSERT INTO $TABLE_NAME VALUES (2, 'Bruna', '20230205')")
    spark.sql(s"INSERT INTO $TABLE_NAME VALUES (3, 'Carlos', '20230204')")
    spark.sql(s"INSERT INTO $TABLE_NAME VALUES (4, 'Daniela', '20230204')")
    spark.sql(s"INSERT INTO $TABLE_NAME VALUES (5, 'Érica', '20230202')")

    val getDF = new SparkImpl(spark).get(columns, TABLE_NAME, "dat_ref", partitions)

    assert(getDF.count() == 4)
  }

}