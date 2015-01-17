package wandou

import org.apache.avro.generic.IndexedRecord
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import wandou.avpath.Evaluator.Ctx

package object avpath {

  def select(data: IndexedRecord, path: String): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.select(data, ast)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  def update(data: IndexedRecord, path: String, value: Any): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.update(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  def updateJson(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.updateJson(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insert(data: IndexedRecord, path: String, value: Any): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.insert(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertJson(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.insertJson(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAll(data: IndexedRecord, path: String, values: java.util.Collection[_]): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.insertAll(data, ast, values)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def insertAllJson(data: IndexedRecord, path: String, value: String): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.insertAllJson(data, ast, value)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map elements only
   */
  def delete(data: IndexedRecord, path: String): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.delete(data, ast)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Applied on array/map only
   */
  def clear(data: IndexedRecord, path: String): Try[List[Ctx]] = {
    val p = new Parser()
    try {
      val ast = p.parse(path)
      val ctxs = Evaluator.clear(data, ast)
      Success(ctxs)
    } catch {
      case ex: Throwable => Failure(ex)
    }
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
