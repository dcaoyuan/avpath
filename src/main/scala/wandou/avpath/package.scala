package wandou

package object avpath {

  trait Command extends Serializable {
    def id: String
  }

  final case class Select(id: String, path: String) extends Command
  final case class Update(id: String, path: String, value: Any) extends Command
  final case class UpdateJson(id: String, path: String, value: String) extends Command
  final case class Insert(id: String, path: String, value: Any) extends Command
  final case class InsertJson(id: String, path: String, value: String) extends Command
  final case class InsertAll(id: String, path: String, values: List[_]) extends Command
  final case class InsertAllJson(id: String, path: String, values: String) extends Command
  final case class Delete(id: String, path: String) extends Command
  final case class Clear(id: String, path: String) extends Command

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
