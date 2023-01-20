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

import java.io.File

class PlayList2022Test {

  private var PATH: String = null
  private var PATH_2: String = null
  private val INGESTIOM_PATH = Config.getInputPath

  private var today: TodayUtils = null
  private var iSpark: ISpark = null
  private var iHdfs: Ihdfs = null
  private var spark: SparkSession = null

  private lazy val INVALID_LINES: String = "_corrupt_record"

  @Before
  def configMocks(): Unit = {
    var file = new File("src/test/resources/mock_example_20220812.csv")
    var fileAux = new File(file.getAbsolutePath)
    PATH = fileAux.getAbsolutePath

    file = new File("src/test/resources/mock_example_playlist_music_20220812.csv")
    fileAux = new File(file.getAbsolutePath)
    PATH_2 = fileAux.getAbsolutePath

    today = mock(classOf[TodayUtils])

    when(today.getTodayOnlyNumbers()).thenReturn("20220812")
    when(today.getTodayWithHours()).thenReturn("20220812T162015")
    when(today.getToday()).thenReturn("20220812")

    spark = new SparkSessionServices().devLocal
  }

  @Test def processSuccess(): Unit = {
    val dataFrameExample = spark.read.option("header", "true").option("delimiter", ";").csv(PATH)
    val dataFrameExample2 = spark.read.option("header", "true").option("delimiter", ";").csv(PATH_2)

    dataFrameExample.createOrReplaceTempView("user")
    dataFrameExample2.createOrReplaceTempView("playlist")

    spark.sql("select * from user").show()
    spark.sql("select * from playlist").show()

    new PlayList2022(new SparkImplFake(spark), today).run()

  }

}
