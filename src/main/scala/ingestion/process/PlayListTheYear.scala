package ingestion.process

import ingestion.base.dados.ISpark
import ingestion.base.enums.StatusEnums
import ingestion.util.{TodayUtils, ValidParamUtils}
import org.apache.spark.sql.functions.{col, lit, substring}

class PlayListTheYear(iSpark: ISpark, today: TodayUtils, validParamUtils: ValidParamUtils) {
  private var TABLE_NAME_INGESTION = ""

  private var USER_TABLE = ""
  private var PLAY_LIST_TABLE = ""

  private var YEAR = ""

  def run(userTable: String, playListTable: String, tableNameIngestion: String, year: String): Int = {
    println("Starting... ".concat(getClass.getSimpleName))

    var status = StatusEnums.FAILURE.id

    try {

      println("Validing parameters... ")
      if (!validParamUtils.dataBaseTableValid(userTable)) throw new IllegalArgumentException("Invalid parameter, check: if the value userTable are null or do not contain a period between the database and the table")
      if (!validParamUtils.dataBaseTableValid(playListTable)) throw new IllegalArgumentException("Invalid parameter, check: if the value playListTable are null or do not contain a period between the database and the table")
      if (!validParamUtils.dataBaseTableValid(tableNameIngestion)) throw new IllegalArgumentException("Invalid parameter, check: if the value tableNameIngestion are null or do not contain a period between the database and the table")
      if (!validParamUtils.dataBaseTableValid(year)) throw new IllegalArgumentException("Invalid parameter, check: if the value year are null or do not contain a period between the database and the table")
      println("The parameters are corrects... ")

      TABLE_NAME_INGESTION = tableNameIngestion
      USER_TABLE = userTable
      PLAY_LIST_TABLE = playListTable
      YEAR = year

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
      val dfUser = iSpark.get(s"select * from $USER_TABLE")

      println(s"consulting $PLAY_LIST_TABLE...")
      val dfPlayList = iSpark.get(s"select * from $PLAY_LIST_TABLE")

      println(s"building dfFinal ...")
      val dfFinal = dfUser.as("u").join(dfPlayList.as("p"))
        .where(col(s"u.cpf") === col(s"p.user_id")
          and substring(col(s"p.dat_ref"), 0, 4) === lit(YEAR))
        .select(
          col("u.name"),
          col("u.cpf"),
          col("p.list"),
          substring(col("p.dat_ref"), 0, 4).as("dat_ref_ano")
        )

      println(s"building partitionName and ingestionTimeStamp ...")
      val partitionName = today.getToday()
      val ingestionTimeStamp = today.getToday()

      println(s"building dfToSave ...")
      val dfToSave = dfFinal
        .withColumn("timestamp", lit(ingestionTimeStamp))
        .withColumn("dat_ref", lit(partitionName))
        .select(col("name"),
          col("cpf"),
          col("list"),
          col("dat_ref_ano"),
          col("timestamp"),
          col("dat_ref")).persist()


      println(s"saving dfToSave int the $TABLE_NAME_INGESTION ...")
      iSpark.save(dfToSave, TABLE_NAME_INGESTION)

      println(s"dfToSave saved successfully ...")

      dfToSave.unpersist()

    } catch {
      case _: NullPointerException =>
        return StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id
      case _: Exception =>
        return StatusEnums.FAILURE.id
    }

    StatusEnums.SUCCESS.id
  }
}