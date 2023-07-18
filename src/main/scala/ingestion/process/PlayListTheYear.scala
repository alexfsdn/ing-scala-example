package ingestion.process

import ingestion.base.dados.ISpark
import ingestion.base.enums.StatusEnums
import ingestion.util.impl.TodayUtilsImpl
import ingestion.util.{TodayUtils, ValidParamUtils}
import org.apache.spark.sql.functions.{avg, col, collect_set, count, countDistinct, current_date, current_timestamp, date_format, first, last, lit, max, mean, min, substring, sum, sumDistinct, when}
import org.apache.spark.storage.StorageLevel
import org.joda.time.LocalDateTime

class PlayListTheYear(iSpark: ISpark, validParamUtils: ValidParamUtils) {
  private var TABLE_NAME_INGESTION = ""

  private var USER_TABLE = ""
  private var PLAY_LIST_TABLE = ""

  private var YEAR = ""
  private var LABEL_PARTITION = ""

  def run(userTable: String, playListTable: String, tableNameIngestion: String, year: String, labelPartition: String): Int = {
    println("Starting... ".concat(getClass.getSimpleName))

    var status = StatusEnums.FAILURE.id

    try {

      println("Validing parameters... ")
      if (!validParamUtils.dataBaseTableValid(userTable)) throw new IllegalArgumentException("Invalid parameter, check: if the value userTable is null or do not contain a period between the database and the table")
      if (!validParamUtils.dataBaseTableValid(playListTable)) throw new IllegalArgumentException("Invalid parameter, check: if the value playListTable is null or do not contain a period between the database and the table")
      if (!validParamUtils.dataBaseTableValid(tableNameIngestion)) throw new IllegalArgumentException("Invalid parameter, check: if the value tableNameIngestion is null or do not contain a period between the database and the table")
      if (!validParamUtils.isEmpty(year)) throw new IllegalArgumentException("Invalid parameter, check: if the value year is null or do not contain a period between the database and the table")
      if (!validParamUtils.isEmpty(labelPartition)) throw new IllegalArgumentException("Invalid parameter, check: if the value labelPartition is null")
      println("The parameters are corrects... ")

      TABLE_NAME_INGESTION = tableNameIngestion
      USER_TABLE = userTable
      PLAY_LIST_TABLE = playListTable
      YEAR = year
      LABEL_PARTITION = labelPartition

      println(s"TABLE_NAME_INGESTION=$TABLE_NAME_INGESTION")
      println(s"USER_TABLE=$USER_TABLE")
      println(s"PLAY_LIST_TABLE=$PLAY_LIST_TABLE")

      println("Starting... ".concat(getClass.getSimpleName))

      status = process()

    } catch {
      case _: IllegalArgumentException =>
        status = StatusEnums.FAILURE.id
    }

    status
  }

  def process(): Int = {
    try {

    println(s"consulting $USER_TABLE...")
    val dfUser = iSpark.get(s"select * from $USER_TABLE").persist(StorageLevel.MEMORY_ONLY_SER)


    println(s"consulting $PLAY_LIST_TABLE...")
    val dfPlayList = iSpark.get(s"select * from $PLAY_LIST_TABLE").persist(StorageLevel.MEMORY_ONLY_SER)

    println(s"building dfFinal ...")
    val dfJoin = dfUser.as("u").join(dfPlayList.as("p"))
      .where(col(s"u.cpf") === col(s"p.user_id")
        and substring(col(s"p.dat_ref"), 0, 4) === lit(YEAR))
      .groupBy(
        col("u.name").as("name"),
        col("u.cpf").as("cpf"),
        col("p.style").as("style"),
        substring(col("p.dat_ref"), 0, 4).as("year_month"))
      .agg(sum(col("p.number_music")).as("number_all_music_by_style"),
        count(col("p.number_music")).as("number_playlist_used_by_style")).persist(StorageLevel.MEMORY_ONLY_SER)

    dfUser.unpersist()
    dfPlayList.unpersist()

    val dfPerfil = dfJoin.withColumn("perfil", when(col("number_all_music_by_style")
      .between(1, 30), lit("Nesse estilo você é do tipo que se conecta com as músicas que você escuta"))
      .when(col("number_all_music_by_style") > 31, lit("Nesse estilo você é do tipo, quanto mais variedade melhor")).otherwise("----"))
      .select(
        col("name"),
        col("cpf"),
        col("style"),
        col("number_all_music_by_style"),
        col("number_playlist_used_by_style"),
        col("perfil"),
        col("year_month"))

    dfJoin.unpersist()

    println(s"building partitionName and ingestionTimeStamp ...")
    val partitionName = TodayUtilsImpl.getToday()

    val timeStamp = TodayUtilsImpl.getTodayWithHours()
    val tableNameTmp = "tableNameTMP_".concat(timeStamp)

    val dfToSave = dfPerfil
      .withColumn("time_stamp", lit(current_timestamp()))
      .withColumn(LABEL_PARTITION, lit(partitionName))
      .select(
        col("name"),
        col("cpf"),
        col("style"),
        col("number_all_music_by_style"),
        col("number_playlist_used_by_style"),
        col("perfil"),
        col("time_stamp"),
        col("year_month"))

    dfToSave.createTempView(tableNameTmp)

    println(s"saving dfPerfil int the $TABLE_NAME_INGESTION ...")


    val columns = Array(
      "name",
      "cpf",
      "style",
      "number_all_music_by_style",
      "number_playlist_used_by_style",
      "perfil",
      "time_stamp",
      "year_month")

    iSpark.save(columns, TABLE_NAME_INGESTION, tableNameTmp, LABEL_PARTITION, YEAR)

    println(s"dfToSave saved successfully ...")

    dfPerfil.unpersist()


      } catch {
        case _: NullPointerException =>
          return StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id
        case _: Exception =>
          return StatusEnums.FAILURE.id
      }

    StatusEnums.SUCCESS.id
  }
}