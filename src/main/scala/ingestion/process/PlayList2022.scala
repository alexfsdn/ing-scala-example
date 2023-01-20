package ingestion.process

import ingestion.base.dados.{ISpark}
import ingestion.base.enums.StatusEnums
import ingestion.util.{TodayUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, substring}
import org.apache.spark.sql.types.{StringType, StructType}

class PlayList2022(iSpark: ISpark, today: TodayUtils) {


  def run(): Unit = {

    println("Starting...")

    process()

  }

  def process(): Int = {
    try {


      val dfUser = iSpark.get("select * from user")
      val dfPlayList = iSpark.get("select * from playlist")

      val dfFinal = dfUser.as("u").join(dfPlayList.as("p"))
        .where(col(s"u.cpf") === col(s"p.user_id")
          and substring(col(s"p.dat_ref"), 0, 4) === lit("2022"))
        .select(
          col("u.name"),
          col("u.cpf"),
          col("p.list"),
          substring(col("p.dat_ref"), 0, 4).as("dat_ref_ano")
        )

      val partitionName = today.getToday()
      val ingestionTimeStamp = today.getToday()


      val dfToSave = dfFinal
        .withColumn("timestamp", lit(ingestionTimeStamp))
        .withColumn("dat_ref", lit(partitionName))
        .select(col("name"),
          col("cpf"),
          col("list"),
          col("dat_ref_ano"),
          col("timestamp"),
          col("dat_ref")).persist()

      iSpark.save(dfToSave, "plays_2022")

      dfToSave.unpersist()

    } catch {
      case _: NullPointerException =>
        return StatusEnums.THERE_IS_NOT_DATA_TO_PROCESS.id
      case _: Exception =>
        return StatusEnums.FAILURE.id
    }

    StatusEnums.SUCCESS.id
  }

  private def export(dataFrame: DataFrame, format: String, pathFile: String): Unit = {
    iSpark.exportFile(dataFrame, format, pathFile)
  }


  private def getHiveSchema(tableName: String, partitionName: String, timestampName: String, invalidLines: String): StructType = {
    iSpark.getHiveSchema(tableName, partitionName, timestampName).add(invalidLines, StringType, true)
  }

  def getFile(pathFile: String, format: String, parameters: Map[String, String], schema: StructType): DataFrame = {
    iSpark.getFile(pathFile, format, parameters, schema)
  }

  def getInvalidLines(df: DataFrame, invalidLinesColName: String): DataFrame = {
    df.filter(col(invalidLinesColName).isNotNull)
      .select(col(invalidLinesColName))
  }

  def getValidLines(df: DataFrame, invalidLinesColName: String): DataFrame = {
    df.filter(col(invalidLinesColName).isNull).drop(col(invalidLinesColName))
  }
}