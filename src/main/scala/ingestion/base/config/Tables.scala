package ingestion.base.config

object Tables {

  private lazy val database: String = Config.getDataBase
  private lazy val table: String = Config.getTable

  private lazy val databaseControl: String = Config.getDataBaseControl
  private lazy val tableControl: String = Config.getTableControl

  lazy val TABLE_INGESTION: String = database.concat(".").concat(table)
  lazy val TABLE_CONTROL: String = databaseControl.concat(".").concat(tableControl)

}
