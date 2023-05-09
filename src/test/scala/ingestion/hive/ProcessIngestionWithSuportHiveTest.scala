package ingestion.hive

import ingestion.base.config.Config
import ingestion.base.dados.Ihdfs
import ingestion.base.dados.impl.SparkImpl
import ingestion.base.enums.StatusEnums
import ingestion.fake.schema.ExampleBaseInterna
import ingestion.process.ProcessIngestion
import ingestion.util.TodayUtils
import ingestion.util.impl.TodayUtilsImpl
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.{After, Before, Test}
import org.mockito.Mockito.{mock, times, verify, when}
import java.io.File

class ProcessIngestionWithSuportHiveTest {

  private var PATH: String = null
  private var PATH_2: String = null
  private val INGESTIOM_PATH = Config.getInputPath

  private var today: TodayUtils = null
  private var iHdfs: Ihdfs = null
  private var spark: SparkSession = null

  private lazy val INVALID_LINES: String = "_corrupt_record"

  private lazy val DATABASE: String = Config.getDataBase
  private lazy val TABLE: String = Config.getTable
  private lazy val PARTITION_NAME: String = Config.getPartitionName
  private lazy val INTESTION_NAME: String = Config.getIngestionName

  private var hiveContext: SQLContext = null;

  @Before
  def configMocks(): Unit = {
    val directory = new File("src/test/resources/archiving")
    val directoryError = new File("src/test/resources/archiving_error")

    deleteRecursive(directory)
    deleteRecursive(directoryError)

    def deleteRecursive(file: File): Unit = {
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursive)
      }
      file.delete()
    }

    val conf = new SparkConf().setAppName("App Name example prod")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.some.config.option", "some-value")
      .set("spark.sql.catalogImplementation", "hive")

    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    hiveContext = spark.sqlContext

    hiveContext.sql(s"DROP TABLE IF EXISTS $DATABASE.$TABLE")
    hiveContext.sql(s"DROP DATABASE IF EXISTS $DATABASE")
    hiveContext.sql(s"CREATE DATABASE IF NOT EXISTS $DATABASE")
    hiveContext.sql(s"USE $DATABASE")
    hiveContext.sql(s"CREATE TABLE IF NOT EXISTS $DATABASE.$TABLE (name STRING, age STRING, cpf STRING, dat_ref STRING, $INTESTION_NAME STRING, $PARTITION_NAME STRING)")

    PATH = "src/test/resources/mock_example_20220812.csv"
    PATH_2 = "src/test/resources/mock_example_20220813.csv"

    val deltaSchema = ExampleBaseInterna.exampleTableInternalSchema
    val schema = deltaSchema.add(INVALID_LINES, StringType, true)

    println(s"Hive-Schema... ${schema.toString()}")

    today = new TodayUtilsImpl
  }

  @After
  def cleanup(): Unit = {
    hiveContext.sql(s"DROP TABLE IF EXISTS $DATABASE.$TABLE")
  }

  @Test def processSuccess(): Unit = {
    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(PATH)
    dataFrameExample.show(10, false)

    val dataFrameExample2 = spark.read.option("header", "true").option("delimiter", ";").csv(PATH_2)
    dataFrameExample2.show(10, false)

    iHdfs = mock(classOf[Ihdfs])

    //val pathOne = PATH.replace("\\", "/")
    //val pathTwo = PATH_2.replace("\\", "/")

    when(iHdfs.lsAll(INGESTIOM_PATH)).thenReturn(List(PATH, PATH_2))
    when(iHdfs.exist(PATH)).thenReturn(true)
    when(iHdfs.exist(PATH_2)).thenReturn(true)

    val sparkImpl = new SparkImpl(spark)
    val statusList = new ProcessIngestion(sparkImpl, iHdfs, today).run()

    val status = StatusEnums.validStatus(statusList)

    assert(status == StatusEnums.SUCCESS.id)
    verify(iHdfs, times(1)).exist(PATH)
    verify(iHdfs, times(1)).exist(PATH_2)
  }
}
