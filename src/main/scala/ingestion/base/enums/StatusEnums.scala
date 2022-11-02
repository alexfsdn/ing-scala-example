package ingestion.base.enums

object StatusEnums extends Enumeration {
  type StatusEnums = Value
  val SUCESS, FAILURE, THERE_IS_NOT_DATA_TO_PROCESS = Value
}
