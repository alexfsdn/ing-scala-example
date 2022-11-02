package ingestion.base.dados

trait Ihdfs {

  /** *
   *
   * @param path
   * @return
   */
  def lsAll(path: String): List[String]

  /** *
   *
   * @param origin
   * @param destiny
   * @return
   */
  def mv(origin: String, destiny: String): Boolean

  /** *
   *
   * @param path
   * @return
   */
  def mkdir(path: String): Boolean

  /** *
   *
   * @param path
   * @return
   */
  def exist(path: String): Boolean

}
