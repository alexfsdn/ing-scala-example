package ingestion.model

case class ExampleDataFrame(
                             name: String = null
                             , age: String = null
                             , cpf: String = null
                             , datRef: String = null
                           ) extends Serializable {
}
