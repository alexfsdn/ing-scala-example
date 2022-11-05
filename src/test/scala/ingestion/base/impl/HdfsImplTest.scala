package ingestion.base.impl

import ingestion.base.config.Config
import org.junit.Test

class HdfsImplTest {

  @Test def test(): Unit = {
    println("HdfsServicesImplTest test...")

    implicit val formats = org.json4s.DefaultFormats
    val p = Config.getUrlHdfs
    val p2 = p.replace("'", "")
    println(p)
    println(p2)
  }
}
