package ingestion.hive

import ingestion.base.dados.impl.SparkImpl
import ingestion.base.enums.StatusEnums
import ingestion.base.services.SparkSessionServices
import ingestion.fake.SparkImplFake
import ingestion.process.PlayListTheYear
import ingestion.util.impl.ValidParamUtilsImpl
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.junit.{After, Before, Test}
import org.mockito.Mockito.{mock, times, verify, when}

class PlayListTheYearWithSuportHiveTest {

  private var PATH: String = null
  private var PATH_2: String = null

  private var spark: SparkSession = null

  private var hiveContext: SQLContext = null;

  @Before
  def configMocks(): Unit = {

    spark = SparkSessionServices.devLocalEnableHiveSupport

    hiveContext = spark.sqlContext

    cleanup()
    hiveContext.sql(s"CREATE DATABASE IF NOT EXISTS music")
    hiveContext.sql(s"USE music")
    hiveContext.sql(s"CREATE TABLE IF NOT EXISTS music.playlist (user_id STRING,list_name STRING,style STRING,dat_ref STRING,number_music STRING)")
    hiveContext.sql(s"CREATE TABLE IF NOT EXISTS music.user (name STRING, age STRING, cpf STRING, dat_ref STRING)")
    hiveContext.sql(s"CREATE TABLE IF NOT EXISTS " +
      s"music.play_list_the_year " +
      s"(" +
      s"name STRING," +
      s"cpf STRING," +
      s"style STRING," +
      s"number_all_music_by_style STRING," +
      s"number_playlist_used_by_style STRING, " +
      s"perfil STRING, " +
      s"time_stamp STRING, " +
      s"year_month STRING) " +
      s"PARTITIONED BY (dat_partition STRING)"
    )

    PATH = "src/test/resources/mock_example_20220812.csv"
    PATH_2 = "src/test/resources/mock_example_playlist_music_20220812.csv"

  }

  @After
  def cleanup(): Unit = {
    hiveContext.sql("DROP DATABASE IF EXISTS music CASCADE")
  }

  @Test def Success(): Unit = {
    val userDF = hiveContext.read.option("header", "true").option("delimiter", ";").csv(PATH)
    val playListDF = hiveContext.read.option("header", "true").option("delimiter", ";").csv(PATH_2)

    userDF.write.format("orc").mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "dynamic")
      .insertInto(s"music.user")

    playListDF.write.format("orc").mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "dynamic")
      .insertInto(s"music.playlist")

    hiveContext.sql("select * from music.user").show(10, false)
    hiveContext.sql("select * from music.playlist").show(10, false)

    val userTable = "music.user"
    val playListTable = "music.playlist"
    val tableNameIngestion = "music.play_list_the_year"
    val year = "2022"
    val labelPartition = "dat_partition"

    val valid = new ValidParamUtilsImpl

    val iSpark = new SparkImpl(spark)

    val status: Int = new PlayListTheYear(iSpark, valid, true).run(userTable, playListTable, tableNameIngestion, year, labelPartition)

    val finalTableDF = hiveContext.sql("select * from music.play_list_the_year ")
    assert(status == StatusEnums.SUCCESS.id)
    assert(finalTableDF.count() > 0)

    println("select * from music.play_list_the_year ")
    finalTableDF.show(20, false)

    new PlayListTheYear(iSpark, valid, true).run(userTable, playListTable, tableNameIngestion, "2023", labelPartition)

    println("show partitions music.play_list_the_year ")
    hiveContext.sql("show partitions music.play_list_the_year ").show(10, false)

  }
}
