package ingestion.fake.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ExampleBaseInterna {

  def exampleTableInternalSchema: StructType = StructType(Array
  (
    StructField(ExampleBaseInternaEnums.name.toString, StringType, true),
    StructField(ExampleBaseInternaEnums.age.toString, StringType, true),
    StructField(ExampleBaseInternaEnums.cpf.toString, StringType, true),
    StructField(ExampleBaseInternaEnums.dat_ref.toString, StringType, true),
    StructField(ExampleBaseInternaEnums.timestamp_ing.toString, StringType, true),
    StructField(ExampleBaseInternaEnums.dt_ing.toString, StringType, true)
  ))

}
