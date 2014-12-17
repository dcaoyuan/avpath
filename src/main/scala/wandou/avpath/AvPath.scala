package wandou.avpath

import org.apache.avro.generic.IndexedRecord

object AvPath {

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

}
