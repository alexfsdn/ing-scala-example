package ingestion.base.dados.impl

import ingestion.base.dados.Ihdfs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.io.FileNotFoundException
import scala.collection.mutable.ListBuffer

class HdfsImpl extends Ihdfs with Serializable {

  val CONF = new Configuration()

  /** *
   *
   * @param path
   * @return
   */
  override def lsAll(path: String): List[String] = {
    val src: Path = new Path(path)
    val fs = FileSystem.get(src.toUri, CONF)
    val allFiles = new ListBuffer[String]

    try {
      val files = fs.listFiles(new Path(path), true)
      while (files.hasNext) {
        val pathFull = files.next().getPath
        allFiles.append(pathFull.toString)
      }
    } catch {
      case _: FileNotFoundException =>
        return null
    }
    allFiles.toList
  }

  /** *
   *
   * @param origin
   * @param destiny
   * @return
   */
  override def mv(origin: String, destiny: String): Boolean = {
    val originPath: Path = new Path(origin)
    val destinyPath: Path = new Path(destiny)

    val srcOrigin = FileSystem.get(originPath.toUri, CONF)
    val srcDestiny = FileSystem.get(destinyPath.toUri, CONF)

    try {

      FileUtil.copy(srcOrigin, originPath, srcDestiny, destinyPath, true, CONF)

    } catch {
      case _: FileNotFoundException => return false
    }
    true
  }

  /** *
   *
   * @param path
   * @return
   */
  override def mkdir(path: String): Boolean = {
    false
  }

  /** *
   *
   * @param path
   * @return
   */
  override def exist(path: String): Boolean = {
    val src: Path = new Path(path)
    val fs = FileSystem.get(src.toUri, CONF)

    try {
      fs.exists(new Path(path))
    } catch {
      case _: FileNotFoundException => false
    }
  }
}