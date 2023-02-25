package ingestion.base.config

object Tables {

  //LAZY: The compiler does not immediately evaluate the bound expression of a lazy val.
  // It evaluates the variable only on its first access.
  //Upon initial access, the compiler evaluates the expression and stores the result in the lazy val.
  // Whenever we access this val at a later stage, no execution happens, and the compiler returns the result.
  private lazy val database: String = Config.getDataBase
  private lazy val table: String = Config.getTable

  lazy val TABLE_INGESTION: String = database.concat(".").concat(table)

}
