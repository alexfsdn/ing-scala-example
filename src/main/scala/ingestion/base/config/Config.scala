package ingestion.base.config

import com.typesafe.config.{Config, ConfigFactory}

object Config {

  private val config: Config = {
    try {

      ConfigFactory.load()
    } catch {
      case ex: Exception =>
        println("Falha ao carregar config")
        println(ex.getMessage)
        throw ex
    }
  }

  def getFileName: String = config.getString("file_name")

}
