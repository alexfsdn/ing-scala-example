package ingestion

import ingestion.base.services.SparkSessionServices
import org.apache.spark.sql.SQLContext
import org.junit.Test

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

    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '111.111.111-11     ', '(11) 91111-1119'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '111.111.111-11     ', '(10) 91111-1119'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '222.222.    222-22', '(22)092222-2229'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '       333.333.333-33', '(33)0933333-3339'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '444.444.444-44', '(44)232-33239'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '555.555.555-55', '(44)0000000000000098232-1239'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '71.625.816/0001-59', '(44)0000000000000098232-1239'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '01.625.816/0001-59', '0000000000000098232-1239'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '760.822.690-04', '(44)0000000000000098232-1239'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '072.284.280-56', '0(44)0000000000000098232-1239'")
    hiveContext.sql("INSERT INTO TABLE testDB.testTable SELECT '072.284.280-56', '00000000000000000000(44)0000000000000098232-1239'")

    hiveContext.sql("select " +
      "cpf, " +
      "regexp_replace(cpf, '[^0-9]', '') as CPF_TRATADO, telefone, " +
      "CASE " +
      " WHEN length(regexp_replace(telefone, '[^0-9]', '')) > 11 THEN " +
      "  concat(" +
      "  substr(regexp_replace(regexp_replace(telefone, '[^0-9]', ''),'^0+', ''), 1, 2), " +
      "  regexp_replace(substr(regexp_replace(regexp_replace(telefone, '[^0-9]', ''),'^0+', ''), 3, length(telefone)), '^0+', '')" +
      ") " +
      "ELSE " +
      "  regexp_replace(regexp_replace(telefone, '[^0-9]', ''), '^0+', '')" +
      "END AS TELEFONE_TRATADO " +
      "from testDB.testTable").show(20, false)

    /*
          +---------------------+--------------+------------------------------------------------+----------------+
          |cpf                  |CPF_TRATADO   |telefone                                        |TELEFONE_TRATADO|
          +---------------------+--------------+------------------------------------------------+----------------+
          |       333.333.333-33|33333333333   |(33)0933333-3339                                |339333333339    |
          |072.284.280-56       |07228428056   |00000000000000000000(44)0000000000000098232-1239|44982321239     |
          |555.555.555-55       |55555555555   |(44)0000000000000098232-1239                    |44982321239     |
          |111.111.111-11       |11111111111   |(11) 91111-1119                                 |11911111119     |
          |01.625.816/0001-59   |01625816000159|0000000000000098232-1239                        |982321239       |
          |71.625.816/0001-59   |71625816000159|(44)0000000000000098232-1239                    |44982321239     |
          |760.822.690-04       |76082269004   |(44)0000000000000098232-1239                    |44982321239     |
          |111.111.111-11       |11111111111   |(10) 91111-1119                                 |10911111119     |
          |222.222.    222-22   |22222222222   |(22)092222-2229                                 |22922222229     |
          |072.284.280-56       |07228428056   |0(44)0000000000000098232-1239                   |44982321239     |
          |444.444.444-44       |44444444444   |(44)232-33239                                   |4423233239      |
          +---------------------+--------------+------------------------------------------------+----------------+

     */

  }

  def cleanup(): Unit = {
    hiveContext.sql("DROP DATABASE IF EXISTS testDB CASCADE")
  }
}