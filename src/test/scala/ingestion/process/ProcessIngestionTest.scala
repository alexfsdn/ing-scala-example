package ingestion.process

import ingestion.base.config.Config
import ingestion.base.dados.{ISpark, Ihdfs}
import ingestion.base.enums.StatusEnums
import ingestion.base.services.SparkSessionServices
import ingestion.fake.SparkImplFake
import ingestion.fake.schema.ExampleBaseInterna
import ingestion.util.TodayUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.junit.{Before, Test}
import org.mockito.Mockito.{mock, times, verify, when}

class ProcessIngestionTest {

  private var PATH: String = null
  private var PATH_2: String = null
  private val INGESTIOM_PATH = Config.getInputPath

  private var today: TodayUtils = null
  private var iHdfs: Ihdfs = null
  private var spark: SparkSession = null

  private lazy val INVALID_LINES: String = "_corrupt_record"

  @Before
  def configMocks(): Unit = {
    PATH = "src/test/resources/mock_example_20220812.csv"
    PATH_2 = "src/test/resources/mock_example_20220813.csv"

    val deltaSchema = ExampleBaseInterna.exampleTableInternalSchema
    val schema = deltaSchema.add(INVALID_LINES, StringType, true)

    println(s"Hive-Schema... ${schema.toString()}")

    today = mock(classOf[TodayUtils])

    when(today.getTodayOnlyNumbers()).thenReturn("20220812")
    when(today.getTodayWithHours()).thenReturn("20220812T162015")
    when(today.getToday()).thenReturn("20220812")

    spark = new SparkSessionServices().devLocal
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

    val statusList = new ProcessIngestion(new SparkImplFake(spark), iHdfs, today).run()

    val status = StatusEnums.validStatus(statusList)

    assert(status == StatusEnums.SUCCESS.id)
    verify(iHdfs, times(1)).exist(PATH)
    verify(iHdfs, times(1)).exist(PATH_2)
  }

  @Test def processNoData(): Unit = {
    iHdfs = mock(classOf[Ihdfs])

    //val pathOne = PATH.replace("\\", "/")
    //val pathTwo = PATH_2.replace("\\", "/")

    when(iHdfs.lsAll(INGESTIOM_PATH)).thenReturn(List())

    val statusList = new ProcessIngestion(new SparkImplFake(spark), iHdfs, today).run()

    val status = StatusEnums.validStatus(statusList)

    assert(status == StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id)
    verify(iHdfs, times(0)).exist(PATH)
  }
}
