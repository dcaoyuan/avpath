package wandou

import org.apache.avro.generic.IndexedRecord

package object avpath {

  def select(data: IndexedRecord, path: String): List[Any] = {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.select(data, ast) map (_.value)
  }

  def update(data: IndexedRecord, path: String, value: Any) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.update(data, ast, value)
  }

  def updateJson(data: IndexedRecord, path: String, value: String) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.updateJson(data, ast, value)
  }

  /**
   * Applied on array/map only
   */
  def insert(data: IndexedRecord, path: String, value: Any) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.insert(data, ast, value)
  }

  /**
   * Applied on array/map only
   */
  def insertJson(data: IndexedRecord, path: String, value: String) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.insertJson(data, ast, value)
  }

  /**
   * Applied on array/map only
   */
  def insertAll(data: IndexedRecord, path: String, values: List[_]) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.insertAll(data, ast, values)
  }

  /**
   * Applied on array/map only
   */
  def insertAllJson(data: IndexedRecord, path: String, value: String) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.insertAllJson(data, ast, value)
  }

  /**
   * Applied on array/map elements only
   */
  def delete(data: IndexedRecord, path: String) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.delete(data, ast)
  }

  /**
   * Applied on array/map only
   */
  def clear(data: IndexedRecord, path: String) {
    val p = new Parser()
    val ast = p.parse(path)
    Evaluator.clear(data, ast)
  }

  object Key {
    def unapply(x: Key) = Some(x.fullPath)

    val Empty = Key(null)
  }

  implicit final class Key(val path: String) extends Serializable {
    private var _parent = Key.Empty
    private var _fullPath: String = path

    def fullPath = _fullPath

    protected def parent = _parent

    protected def parent_=(parent: Key) {
      _fullPath = (if (parent == Key.Empty) "" else parent.fullPath + "/") + path
    }

    def /(path: String) = {
      val subPath = new Key(path)
      subPath.parent = this
      subPath
    }

    override def toString = _fullPath

    override def equals(other: Any): Boolean = other match {
      case that: Key =>
        _parent == that._parent &&
          path == that.path
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(_parent, path)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

}
