package ingestion.hive

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.{After, Before, Test}

class HiveTableExternalTest {

  private var hiveContext: SQLContext = null;

  @Before
  def setup(): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .config("spark.sql.catalogImplementation", "hive")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("regex_replace", (input: String, pattern: String, replacement: String) => {
      input.replaceAll(pattern, replacement)
    })

    hiveContext = spark.sqlContext

    hiveContext.sql("CREATE DATABASE IF NOT EXISTS test_db")
    hiveContext.sql("USE test_db")
    hiveContext.sql("CREATE EXTERNAL TABLE test_db.processo " +
      "(`CPF` string, " +
      "`TELEFONE` string) " +
      "PARTITIONED BY (dat_ref string) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
      "LOCATION '/src/test/resources/data/raw/referencia/teste/test_db/processo' " +
      "TBLPROPERTIES ('transactional'='false')")

    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo PARTITION(dat_ref) VALUES ('111.111.111-11', '(11) 91111-1111', '20230601')")
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo PARTITION(dat_ref) VALUES ('222.222.222-22', '(22) 922222222', '20230602')")

    hiveContext.sql("CREATE EXTERNAL TABLE test_db.processo_v2 " +
      "(`CPF` string, " +
      "`CPF_TRATADO` string, " +
      "`TELEFONE` string, " +
      "`TELEFONE_TRATADO` string ) " +
      "PARTITIONED BY (dat_ref string) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
      "LOCATION '/src/test/resources/data/raw/referencia/teste/test_db/processo_v2' " +
      "TBLPROPERTIES ('transactional'='false')")


  }

  @After
  def cleanup(): Unit = {
    hiveContext.sql("DROP TABLE IF EXISTS test_db.processo")
    hiveContext.sql("DROP TABLE IF EXISTS test_db.processo_v2")
  }


  @Test
  def testWithRegexReplace(): Unit = {

    //inserindo mais novos dados para esse teste específico
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo PARTITION(dat_ref) VALUES ('333.333.333-33', '(13) 093333-3333', '20230603')")
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo PARTITION(dat_ref) VALUES ('444.444.444-44', '(14) 0944444444', '20230604')")
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo PARTITION(dat_ref) VALUES ('555.555.555-55', '0955555555', '20230605')")
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo PARTITION(dat_ref) VALUES ('669.669.669-66', '95555-5555', '20230606')")
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo PARTITION(dat_ref) VALUES ('777.777.777-77', 'null', '20230607')")

    //consultando a tabela de origem para ver como os dados estão
    hiveContext.sql("SELECT * FROM test_db.processo").show(10, false)

    /*
          +--------------+----------------+--------+
          |CPF           |TELEFONE        |dat_ref |
          +--------------+----------------+--------+
          |333.333.333-33|(13) 093333-3333|20230603|
          |111.111.111-11|(11) 91111-1111 |20230601|
          |444.444.444-44|(14) 0944444444 |20230604|
          |222.222.222-22|(22) 922222222  |20230602|
          |555.555.555-55|0955555555      |20230605|
          |669.669.669-66|95555-5555      |20230606|
          |777.777.777-77|null            |20230607|
          +--------------+----------------+--------+

     */

    //USANDO A TABELA DE ORIGEM PARA POPULAR A SUA TABELA CORRESPONDENTE V2
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo_v2 partition (dat_ref) " +
      " SELECT CPF, " +
      "regexp_replace(CPF, '[^0-9]', '') as CPF_TRATADO, " +
      "TELEFONE, " +
      "CASE WHEN length(regexp_replace(TELEFONE, '[^0-9]', '')) > 11 " +
      "THEN concat(substr(regexp_replace(TELEFONE, '[^0-9]', ''), 1, 2), " +
      "regexp_replace(substr(regexp_replace(TELEFONE, '[^0-9]', ''), 3, 11), '^0+', '')) " +
      "ELSE regexp_replace(regexp_replace(TELEFONE, '[^0-9]', ''), '^0+', '')" +
      "END AS TELEFONE_TRATADO, " +
      "dat_ref " +
      "FROM test_db.processo")

    //consultando a tabela de V2 para ver como os dados TRATADOS
    hiveContext.sql("SELECT * FROM test_db.processo_v2").show(10, false)

    /*
            +--------------+-----------+----------------+----------------+--------+
            |CPF           |CPF_TRATADO|TELEFONE        |TELEFONE_TRATADO|dat_ref |
            +--------------+-----------+----------------+----------------+--------+
            |333.333.333-33|33333333333|(13) 093333-3333|13933333333     |20230603|
            |111.111.111-11|11111111111|(11) 91111-1111 |11911111111     |20230601|
            |444.444.444-44|44444444444|(14) 0944444444 |14944444444     |20230604|
            |222.222.222-22|22222222222|(22) 922222222  |22922222222     |20230602|
            |555.555.555-55|55555555555|0955555555      |955555555       |20230605|
            |669.669.669-66|66966966966|95555-5555      |955555555       |20230606|
            |777.777.777-77|77777777777|null            |                |20230607|
            +--------------+-----------+----------------+----------------+--------+
     */

    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230601')")
    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230602')")
    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230603')")
    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230604')")
    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230605')")
    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230606')")
    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230607')")

    hiveContext.sql("ALTER TABLE test_db.processo_v2 DROP IF EXISTS PARTITION (dat_ref='20230601')")
    hiveContext.sql("ALTER TABLE test_db.processo_v2 DROP IF EXISTS PARTITION (dat_ref='20230602')")
    hiveContext.sql("ALTER TABLE test_db.processo_v2 DROP IF EXISTS PARTITION (dat_ref='20230603')")
    hiveContext.sql("ALTER TABLE test_db.processo_v2 DROP IF EXISTS PARTITION (dat_ref='20230604')")
    hiveContext.sql("ALTER TABLE test_db.processo_v2 DROP IF EXISTS PARTITION (dat_ref='20230605')")
    hiveContext.sql("ALTER TABLE test_db.processo_v2 DROP IF EXISTS PARTITION (dat_ref='20230606')")
    hiveContext.sql("ALTER TABLE test_db.processo_v2 DROP IF EXISTS PARTITION (dat_ref='20230607')")


  }

  @Test
  def test(): Unit = {
    //consultando a tabela de origem para ver como os dados estão
    hiveContext.sql("SELECT * FROM test_db.processo").show(10, false)

    //USANDO A TABELA DE ORIDEM PARA POPULAR A SUA TABELA CORRESPONDENTE V2
    hiveContext.sql("INSERT OVERWRITE TABLE test_db.processo_v2 partition (dat_ref) " +
      " SELECT CPF, " +
      "REPLACE(REPLACE(CPF, '-', ''), '.', '') as CPF_TRATADO, " +
      "TELEFONE, " +
      "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TELEFONE, '-', ''), '.', ''), ' ', ''),'(', ''),')', '') as TELEFONE_TRATADO, " +
      "dat_ref " +
      "FROM test_db.processo")

    //consultando a tabela de V2 para ver como os dados TRATADOS
    hiveContext.sql("SELECT * FROM test_db.processo_v2").show(10, false)

    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230601')")
    hiveContext.sql("ALTER TABLE test_db.processo DROP IF EXISTS PARTITION (dat_ref='20230602')")

    hiveContext.sql("ALTER TABLE test_db.processo_2 DROP IF EXISTS PARTITION (dat_ref='20230601')")
    hiveContext.sql("ALTER TABLE test_db.processo_2 DROP IF EXISTS PARTITION (dat_ref='20230602')")
  }

  @Test
  def testCreateView(): Unit = {
    //consultando a tabela de origem para ver como os dados estão
    hiveContext.sql("SELECT * FROM test_db.processo").show(10, false)

    //USANDO A TABELA DE ORIDEM PARA POPULAR A SUA TABELA CORRESPONDENTE V2
    hiveContext.sql("" +
      "CREATE VIEW view_processo AS " +
      " SELECT CPF, " +
      "REPLACE(REPLACE(CPF, '-', ''), '.', '') as CPF_TRATADO, " +
      "TELEFONE, " +
      "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TELEFONE, '-', ''), '.', ''), ' ', ''),'(', ''),')', '') as TELEFONE_TRATADO, " +
      "dat_ref " +
      "FROM test_db.processo")

    //consultando a tabela de V2 para ver como os dados TRATADOS
    hiveContext.sql("SELECT * FROM view_processo").show(10, false)

  }

}
