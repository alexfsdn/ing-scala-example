package ingestion.base.config

object Tables {

  private lazy val database: String = Config.getDataBase
  private lazy val table: String = Config.getTable

  lazy val TABLE_INGESTION: String = database.concat(".").concat(table)

}
