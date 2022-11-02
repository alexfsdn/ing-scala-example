package ingestion

import org.junit.Test

import scala.collection.mutable.ListBuffer

class ListBufferTest {

  @Test def test(): Unit = {
    println("ListBufferTest test...")

    val allFiles = new ListBuffer[String]

    val list = Array("A", "B", "C", "D", "E", "F")

    list.foreach(a => {
      allFiles.append(a)
    })

    allFiles.foreach(a => {
      println(a)
    })

    assert(allFiles.size == 6)

  }
}
