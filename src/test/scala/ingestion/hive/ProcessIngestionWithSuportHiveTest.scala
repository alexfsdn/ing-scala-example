package ingestion.hive

import ingestion.base.config.Config
import ingestion.base.dados.Ihdfs
import ingestion.base.dados.impl.SparkImpl
import ingestion.base.enums.StatusEnums
import ingestion.base.services.SparkSessionServices
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

    spark = SparkSessionServices.devLocalEnableHiveSupport

    hiveContext = spark.sqlContext

    cleanup()
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
    hiveContext.sql(s"DROP DATABASE IF EXISTS $DATABASE CASCADE")
  }

  @Test def processSuccess(): Unit = {
    iHdfs = mock(classOf[Ihdfs])

    when(iHdfs.lsAll(INGESTIOM_PATH)).thenReturn(List(PATH, PATH_2))
    when(iHdfs.exist(PATH)).thenReturn(true)
    when(iHdfs.exist(PATH_2)).thenReturn(true)

    val sparkImpl = new SparkImpl(spark)
    val statusList = new ProcessIngestion(sparkImpl, iHdfs, today, true).run()

    val status = StatusEnums.validStatus(statusList)

    assert(status == StatusEnums.SUCCESS.id)
    verify(iHdfs, times(1)).exist(PATH)
    verify(iHdfs, times(1)).exist(PATH_2)
  }
}
