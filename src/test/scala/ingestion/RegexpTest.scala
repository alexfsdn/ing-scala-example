package ingestion

import org.junit.Test
import ingestion.base.services.SparkSessionServices
import org.apache.spark.sql.SQLContext

class RegexpTest {

  private var hiveContext: SQLContext = null

  @Test
  def test(): Unit = {
    val spark = SparkSessionServices.devLocalEnableHiveSupport

    hiveContext = spark.sqlContext

    cleanup()

    hiveContext.sql("CREATE DATABASE IF NOT EXISTS testDB")
    hiveContext.sql("USE testDB")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS testDB.testTable( " +
      "cpf string, " +
      "telefone string)")

    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '111.111.111-11', '(11) 91111-1111'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '222.222.222-22', '(22)092222-2222'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '333.333.333-33', '(33)0933333-3333'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '444.444.444-44', '(44)232-33233'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '555.555.555-55', '(44)0000000000000098232-1234'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '666.666.666-66', '0000000000000098232-1234'")

    hiveContext.sql("select " +
      "cpf, " +
      "regexp_replace(cpf, '[^0-9]', '') as CPF_TRATADO, telefone, " +
      "CASE " +
      " WHEN length(regexp_replace(telefone, '[^0-9]', '')) > 11 THEN " +
      "  concat(" +
      "  regexp_replace(substr(regexp_replace(telefone, '[^0-9]', ''), 1, 2), '^0+', ''), " +
      "  regexp_replace(substr(regexp_replace(telefone, '[^0-9]', ''), 3, length(telefone)), '^0+', '')" +
      ") " +
      "ELSE " +
      "  regexp_replace(regexp_replace(telefone, '[^0-9]', ''), '^0+', '')" +
      "END AS TELEFONE_TRATADO " +
      "from testDB.testTable").show(10, false)

    cleanup()
  }

  def cleanup(): Unit = {
    hiveContext.sql("DROP DATABASE IF EXISTS testDB CASCADE")
  }
}
