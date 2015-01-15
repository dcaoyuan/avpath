package wandou.avpath

import wandou.avro.FromJson
import wandou.avpath.Parser.ComparionExprSyntax
import wandou.avpath.Parser.ConcatExprSyntax
import wandou.avpath.Parser.LiteralSyntax
import wandou.avpath.Parser.LogicalExprSyntax
import wandou.avpath.Parser.MapKeysSyntax
import wandou.avpath.Parser.MathExprSyntax
import wandou.avpath.Parser.ObjPredSyntax
import wandou.avpath.Parser.PathSyntax
import wandou.avpath.Parser.PosPredSyntax
import wandou.avpath.Parser.SelectorSyntax
import wandou.avpath.Parser.SubstSyntax
import wandou.avpath.Parser.Syntax
import wandou.avpath.Parser.UnaryExprSyntax
import wandou.avpath.Parser.PosSyntax
import java.nio.ByteBuffer
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import scala.collection.mutable

object Evaluator {
  sealed trait Target
  final case class TargetRecord(record: IndexedRecord, field: Schema.Field) extends Target
  final case class TargetArray(array: java.util.Collection[_], idx: Int, arraySchema: Schema) extends Target
  final case class TargetMap(map: java.util.Map[String, _], key: String, mapSchema: Schema) extends Target

  object Op {
    def fromCode(code: Int) = code match {
      case 0 => Select
      case 1 => Update
      case 2 => Delete
      case 3 => Clear
      case 4 => Insert
      case 5 => InsertAll
    }
  }
  sealed trait Op { def code: Int }
  case object Select extends Op { def code = 0 }
  case object Update extends Op { def code = 1 }
  case object Delete extends Op { def code = 2 }
  case object Clear extends Op { def code = 3 }
  case object Insert extends Op { def code = 4 }
  case object InsertAll extends Op { def code = 5 }

  final case class Ctx(value: Any, schema: Schema, topLevelField: Schema.Field, target: Option[Target] = None)

  def select(root: IndexedRecord, ast: PathSyntax): List[Ctx] = {
    evaluatePath(ast, List(Ctx(root, root.getSchema, null)), true)
  }

  def update(root: IndexedRecord, ast: PathSyntax, value: Any): List[Ctx] = {
    operate(root, ast, Update, value, isJsonValue = false)
  }

  def updateJson(root: IndexedRecord, ast: PathSyntax, value: String): List[Ctx] = {
    operate(root, ast, Update, value, isJsonValue = true)
  }

  def insert(root: IndexedRecord, ast: PathSyntax, value: Any): List[Ctx] = {
    operate(root, ast, Insert, value, isJsonValue = false)
  }

  def insertJson(root: IndexedRecord, ast: PathSyntax, value: String): List[Ctx] = {
    operate(root, ast, Insert, value, isJsonValue = true)
  }

  def insertAll(root: IndexedRecord, ast: PathSyntax, values: List[_]): List[Ctx] = {
    operate(root, ast, InsertAll, values, isJsonValue = false)
  }

  def insertAllJson(root: IndexedRecord, ast: PathSyntax, values: String): List[Ctx] = {
    operate(root, ast, InsertAll, values, isJsonValue = true)
  }

  def delete(root: IndexedRecord, ast: PathSyntax): List[Ctx] = {
    operate(root, ast, Delete, null, false)
  }

  def clear(root: IndexedRecord, ast: PathSyntax): List[Ctx] = {
    operate(root, ast, Clear, null, false)
  }

  private def targets(ctxs: List[Ctx]) = ctxs.flatMap(_.target)

  private def operate(root: IndexedRecord, ast: PathSyntax, op: Op, value: Any, isJsonValue: Boolean): List[Ctx] = {
    val ctxs = select(root, ast)

    op match {
      case Select =>

      case Delete =>
        val processingArrs = new mutable.HashMap[java.util.Collection[_], (Schema, List[Int])]()
        targets(ctxs) foreach {
          case _: TargetRecord => // cannot apply delete on record

          case TargetArray(arr, idx, schema) =>
            processingArrs += arr -> (schema, idx :: processingArrs.getOrElse(arr, (schema, List[Int]()))._2)

          case TargetMap(map, key, _) =>
            map.remove(key)
        }

        for ((arr, (schema, toRemoved)) <- processingArrs) {
          arrayRemove(arr, toRemoved)
        }

      case Clear =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: java.util.Collection[_] => arr.clear
              case map: java.util.Map[_, _]     => map.clear
              case _                            => // ?
            }

          case _: TargetArray =>
          // why you be here, for Insert, you should op on record's arr field directly

          case _: TargetMap   =>
          // why you be here, for Insert, you should op on record's map field directly
        }

      case Update =>
        targets(ctxs) foreach {
          case TargetRecord(rec, null) =>
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], rec.getSchema, false) else value
            value1 match {
              case v: IndexedRecord => replace(rec, v)
              case _                => // log.error 
            }

          case TargetRecord(rec, field) =>
            val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
            rec.put(field.pos, value1)

          case TargetArray(arr, idx, arrSchema) =>
            getElementType(arrSchema) match {
              case Some(elemSchema) =>
                val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], elemSchema, false) else value
                (elemSchema.getType, value1) match {
                  case (Type.BOOLEAN, v: Boolean)               => arrayUpdate(arr, idx, v)
                  case (Type.INT, v: Int)                       => arrayUpdate(arr, idx, v)
                  case (Type.LONG, v: Long)                     => arrayUpdate(arr, idx, v)
                  case (Type.FLOAT, v: Float)                   => arrayUpdate(arr, idx, v)
                  case (Type.DOUBLE, v: Double)                 => arrayUpdate(arr, idx, v)
                  case (Type.BYTES, v: ByteBuffer)              => arrayUpdate(arr, idx, v)
                  case (Type.STRING, v: CharSequence)           => arrayUpdate(arr, idx, v)
                  case (Type.RECORD, v: IndexedRecord)          => arrayUpdate(arr, idx, v)
                  case (Type.ENUM, v: GenericEnumSymbol)        => arrayUpdate(arr, idx, v)
                  case (Type.FIXED, v: GenericFixed)            => arrayUpdate(arr, idx, v)
                  case (Type.ARRAY, v: java.util.Collection[_]) => arrayUpdate(arr, idx, v)
                  case (Type.MAP, v: java.util.Map[_, _])       => arrayUpdate(arr, idx, v)
                  case _                                        => // todo
                }

              case None =>
            }

          case TargetMap(map, key, mapSchema) =>
            getValueType(mapSchema) match {
              case Some(valueSchema) =>
                val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], valueSchema, false) else value
                map.asInstanceOf[java.util.Map[String, Any]].put(key, value1)

              case None =>
            }
        }

      case Insert =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: java.util.Collection[_] =>
                getElementType(field.schema) match {
                  case Some(elemSchema) =>
                    val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], elemSchema, false) else value
                    (elemSchema.getType, value1) match {
                      case (Type.BOOLEAN, v: Boolean)               => arrayInsert(arr, v)
                      case (Type.INT, v: Int)                       => arrayInsert(arr, v)
                      case (Type.LONG, v: Long)                     => arrayInsert(arr, v)
                      case (Type.FLOAT, v: Float)                   => arrayInsert(arr, v)
                      case (Type.DOUBLE, v: Double)                 => arrayInsert(arr, v)
                      case (Type.BYTES, v: ByteBuffer)              => arrayInsert(arr, v)
                      case (Type.STRING, v: CharSequence)           => arrayInsert(arr, v)
                      case (Type.RECORD, v: IndexedRecord)          => arrayInsert(arr, v)
                      case (Type.ENUM, v: GenericEnumSymbol)        => arrayInsert(arr, v)
                      case (Type.FIXED, v: GenericFixed)            => arrayInsert(arr, v)
                      case (Type.ARRAY, v: java.util.Collection[_]) => arrayInsert(arr, v)
                      case (Type.MAP, v: java.util.Map[_, _])       => arrayInsert(arr, v)
                      case _                                        => // todo
                    }

                  case None =>
                }

              case map: java.util.Map[_, _] =>
                val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
                value1 match {
                  case (k: String, v) =>
                    map.asInstanceOf[java.util.Map[String, Any]].put(k, v)
                  case kvs: java.util.Map[_, _] =>
                    val entries = kvs.entrySet.iterator
                    if (entries.hasNext) {
                      val entry = entries.next // should contains only one entry
                      map.asInstanceOf[java.util.Map[String, Any]].put(entry.getKey.asInstanceOf[String], entry.getValue)
                    }
                  case _ =>
                }

              case _ => // can only insert to array/map field
            }

          case _: TargetArray =>
          // why you be here, for Insert, you should op on record's arr field directly

          case _: TargetMap   =>
          // why you be here, for Insert, you should op on record's map field directly
        }

      case InsertAll =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: java.util.Collection[_] =>
                getElementType(field.schema) match {
                  case Some(elemSchema) =>
                    val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
                    value1 match {
                      case xs: List[_] =>
                        xs foreach { x =>
                          (elemSchema.getType, x) match {
                            case (Type.BOOLEAN, v: Boolean)               => arrayInsert(arr, v)
                            case (Type.INT, v: Int)                       => arrayInsert(arr, v)
                            case (Type.LONG, v: Long)                     => arrayInsert(arr, v)
                            case (Type.FLOAT, v: Float)                   => arrayInsert(arr, v)
                            case (Type.DOUBLE, v: Double)                 => arrayInsert(arr, v)
                            case (Type.BYTES, v: ByteBuffer)              => arrayInsert(arr, v)
                            case (Type.STRING, v: CharSequence)           => arrayInsert(arr, v)
                            case (Type.RECORD, v: IndexedRecord)          => arrayInsert(arr, v)
                            case (Type.ENUM, v: GenericEnumSymbol)        => arrayInsert(arr, v)
                            case (Type.FIXED, v: GenericFixed)            => arrayInsert(arr, v)
                            case (Type.ARRAY, v: java.util.Collection[_]) => arrayInsert(arr, v)
                            case (Type.MAP, v: java.util.Map[_, _])       => arrayInsert(arr, v)
                            case _                                        => // todo
                          }
                        }

                      case xs: java.util.Collection[_] =>
                        val itr = xs.iterator
                        while (itr.hasNext) {
                          (elemSchema.getType, itr.next) match {
                            case (Type.BOOLEAN, v: Boolean)               => arrayInsert(arr, v)
                            case (Type.INT, v: Int)                       => arrayInsert(arr, v)
                            case (Type.LONG, v: Long)                     => arrayInsert(arr, v)
                            case (Type.FLOAT, v: Float)                   => arrayInsert(arr, v)
                            case (Type.DOUBLE, v: Double)                 => arrayInsert(arr, v)
                            case (Type.BYTES, v: ByteBuffer)              => arrayInsert(arr, v)
                            case (Type.STRING, v: CharSequence)           => arrayInsert(arr, v)
                            case (Type.RECORD, v: IndexedRecord)          => arrayInsert(arr, v)
                            case (Type.ENUM, v: GenericEnumSymbol)        => arrayInsert(arr, v)
                            case (Type.FIXED, v: GenericFixed)            => arrayInsert(arr, v)
                            case (Type.ARRAY, v: java.util.Collection[_]) => arrayInsert(arr, v)
                            case (Type.MAP, v: java.util.Map[_, _])       => arrayInsert(arr, v)
                            case _                                        => // todo

                          }
                        }
                      case _ => // ?
                    }
                  case _ =>
                }

              case map: java.util.Map[_, _] =>
                val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
                value1 match {
                  case xs: List[_] =>
                    xs foreach {
                      case (k: String, v) => map.asInstanceOf[java.util.Map[String, Any]].put(k, v)
                      case _              => // ?
                    }

                  case xs: java.util.Map[_, _] =>
                    val itr = xs.entrySet.iterator
                    while (itr.hasNext) {
                      val entry = itr.next
                      entry.getKey match {
                        case k: String => map.asInstanceOf[java.util.Map[String, Any]].put(k, entry.getValue)
                        case _         => // ?
                      }
                    }

                  case _ => // ?
                }
            }

          case _: TargetArray =>
          // why you be here, for Insert, you should op on record's arr field directly

          case _: TargetMap   =>
          // why you be here, for Insert, you should op on record's map field directly
        }
    }

    ctxs
  }

  private def arrayUpdate[T](arr: java.util.Collection[_], idx: Int, value: T) {
    if (idx >= 0) {
      arr match {
        case xs: java.util.List[_] =>
          if (idx < arr.size) {
            xs.asInstanceOf[java.util.List[T]].set(idx, value)
          }
        case _ =>
          val values = arr.iterator
          var i = 0
          val newTail = new java.util.ArrayList[T]()
          newTail.add(value)
          while (values.hasNext && i <= idx) {
            val value = values.next.asInstanceOf[T]
            if (i >= idx) {
              arr.remove(value)
              newTail.add(value)
            }
            i += 1
          }
          arr.asInstanceOf[java.util.Collection[T]].addAll(newTail)
      }
    }
  }

  private def arrayInsert[T](arr: java.util.Collection[_], value: T) {
    arr.asInstanceOf[java.util.Collection[T]].add(value)
  }

  private def arrayRemove[T](arr: java.util.Collection[T], _toRemoved: List[Int]) {
    var toRemoved = _toRemoved.sortBy(-_)
    while (toRemoved != Nil) {
      arr.remove(toRemoved.head)
      toRemoved = toRemoved.tail
    }
  }

  private def replace(dst: IndexedRecord, src: IndexedRecord) {
    if (dst.getSchema == src.getSchema) {
      val fields = dst.getSchema.getFields.iterator
      while (fields.hasNext) {
        val f = fields.next
        val v = src.get(f.pos)
        val t = f.schema.getType
        if (v != null && (t != Type.ARRAY || !v.asInstanceOf[java.util.Collection[_]].isEmpty) && (t != Type.MAP || !v.asInstanceOf[java.util.Map[_, _]].isEmpty)) {
          dst.put(f.pos, v)
        }
      }
    }
  }

  private def evaluatePath(path: PathSyntax, _ctx: List[Ctx], isTopLevel: Boolean): List[Ctx] = {

    val parts = path.parts
    val len = parts.length
    var i = 0
    var isResArray = true
    var ctx = _ctx
    while (i < len) {
      parts(i) match {
        case x: SelectorSyntax =>
          x.selector match {
            case "."  => ctx = evaluateSelector(x, ctx, isTopLevel)
            case ".." => evaluateDescendantSelector(x, ctx)
          }
          isResArray = true

        case x: ObjPredSyntax =>
          ctx = evaluateObjectPredicate(x, ctx)

        case x: PosPredSyntax =>
          ctx = evaluatePosPredicate(x, ctx) match {
            case List()          => List()
            case List(Left(v))   => List(v)
            case List(Right(xs)) => List(xs.toList).flatten
          }

        case x: MapKeysSyntax =>
          ctx = evaluateMapValues(x, ctx)
          isResArray = true

        case x: ConcatExprSyntax =>
          ctx = evaluateConcatExpr(x, ctx)
          isResArray = true

        case _ => // should not happen

      }
      i += 1
    }

    ctx
  }

  private def evaluateSelector(sel: SelectorSyntax, ctxs: List[Ctx], isTopLevel: Boolean): List[Ctx] = {
    var res = List[Ctx]()
    sel.prop match {
      case null =>
        // select record itself
        ctxs foreach {
          case Ctx(rec: IndexedRecord, schema, topLevelField, _) =>
            res ::= Ctx(rec, schema, null, Some(TargetRecord(rec, null)))
          case _ => // should be rec
        }
      case "*" =>
        ctxs foreach {
          case Ctx(rec: IndexedRecord, schema, topLevelField, _) =>
            val fields = rec.getSchema.getFields.iterator
            while (fields.hasNext) {
              val field = fields.next
              val value = rec.get(field.pos)
              res ::= Ctx(value, field.schema, if (topLevelField == null) field else topLevelField, Some(TargetRecord(rec, field)))
            }
          case Ctx(arr: java.util.Collection[_], schema, topLevelField, _) =>
            val values = arr.iterator
            var j = 0
            while (values.hasNext) {
              val value = values.next
              getElementType(schema) match {
                case Some(elemType) => res ::= Ctx(value, elemType, topLevelField, Some(TargetArray(arr, j, schema)))
                case _              =>
              }
              j += 1
            }
          case _ => // TODO map
        }

      case fieldName =>
        ctxs foreach {
          case Ctx(rec: IndexedRecord, schema, topLevelField, _) =>
            val field = rec.getSchema.getField(fieldName)
            res ::= Ctx(rec.get(field.pos), field.schema, if (topLevelField == null) field else topLevelField, Some(TargetRecord(rec, field)))
          case _ => // should be rec
        }
    }

    res.reverse
  }

  // TODO
  private def evaluateDescendantSelector(sel: SelectorSyntax, baseCtx: Any) = {
  }

  private def evaluateObjectPredicate(expr: ObjPredSyntax, ctx: List[Ctx]): List[Ctx] = {
    var res = List[Ctx]()
    ctx foreach {
      case currCtx @ Ctx(rec: IndexedRecord, schema, _, _) =>
        evaluateExpr(expr.arg, currCtx) match {
          case true => res ::= currCtx
          case _    =>
        }

      case currCtx @ Ctx(arr: java.util.Collection[_], schema, topLevelField, _) =>
        val values = arr.iterator
        var i = 0
        while (values.hasNext) {
          val value = values.next
          getElementType(schema) match {
            case Some(elemType) =>
              val elemCtx = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
              evaluateExpr(expr.arg, elemCtx) match {
                case true => res ::= elemCtx
                case _    =>
              }
            case _ =>
          }
          i += 1
        }
      case _ => // and map?
    }

    res.reverse
  }

  private def evaluatePosPredicate(item: PosPredSyntax, ctx: List[Ctx]): List[Either[Ctx, Array[Ctx]]] = {
    val posExpr = item.arg

    var res = List[Either[Ctx, Array[Ctx]]]()
    ctx foreach {
      case currCtx @ Ctx(arr: java.util.Collection[_], schema, topLevelField, _) =>
        posExpr match {
          case PosSyntax(LiteralSyntax("*"), _, _) =>
            val values = arr.iterator
            val n = arr.size
            val elems = Array.ofDim[Ctx](n)
            var i = 0
            while (values.hasNext) {
              val value = values.next
              getElementType(schema) match {
                case Some(elemType) => elems(i) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                case _              =>
              }
              i += 1
            }
            res ::= Right(elems)

          case PosSyntax(idxSyn: Syntax, _, _) =>
            // idx
            evaluateExpr(posExpr.idx, currCtx) match {
              case idx: Int =>
                val i = theIdx(idx, arr.size)
                val value = arr match {
                  case xs: java.util.List[_] =>
                    xs.get(i)
                  case _ =>
                    val values = arr.iterator
                    var j = 0
                    while (j < i) { values.next }
                    values.next
                }
                getElementType(schema) match {
                  case Some(elemType) => res ::= Left(Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema))))
                  case _              =>
                }
              case _ =>
            }

          case PosSyntax(null, fromIdxSyn: Syntax, toIdxSyn: Syntax) =>
            evaluateExpr(fromIdxSyn, currCtx) match {
              case fromIdx: Int =>
                evaluateExpr(toIdxSyn, currCtx) match {
                  case toIdx: Int =>
                    getElementType(schema) match {
                      case Some(elemType) =>
                        val n = arr.size
                        val from = theIdx(fromIdx, n)
                        val to = theIdx(toIdx, n)
                        val elems = Array.ofDim[Ctx](to - from + 1)
                        arr match {
                          case xs: java.util.List[_] =>
                            var i = from
                            while (i <= to) {
                              elems(i - from) = Ctx(xs.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                              i += 1
                            }
                          case _ =>
                            val values = arr.iterator
                            var i = 0
                            while (values.hasNext && i <= to) {
                              val value = values.next
                              if (i >= from) {
                                elems(i) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                              }
                              i += 1
                            }
                        }
                        res ::= Right(elems)

                      case _ =>
                    }
                  case _ =>
                }

              case _ =>
            }

          case PosSyntax(null, fromIdxSyn: Syntax, null) =>
            evaluateExpr(fromIdxSyn, currCtx) match {
              case fromIdx: Int =>
                getElementType(schema) match {
                  case Some(elemType) =>
                    val n = arr.size
                    val from = theIdx(fromIdx, n)
                    val elems = Array.ofDim[Ctx](n - from)
                    arr match {
                      case xs: java.util.List[_] =>
                        var i = from
                        while (i < n) {
                          elems(i - from) = Ctx(xs.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                          i += 1
                        }
                      case _ =>
                        val values = arr.iterator
                        var i = 0
                        while (values.hasNext && i < n) {
                          val value = values.next
                          if (i >= from) {
                            elems(i - from) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                          }
                          i += 1
                        }
                    }
                    res ::= Right(elems)

                  case _ =>
                }
              case _ =>
            }

          case PosSyntax(null, null, toIdxSyn: Syntax) =>
            evaluateExpr(toIdxSyn, currCtx) match {
              case toIdx: Int =>
                getElementType(schema) match {
                  case Some(elemType) =>
                    val n = arr.size
                    val to = theIdx(toIdx, n)
                    val elems = Array.ofDim[Ctx](to + 1)
                    arr match {
                      case xs: java.util.List[_] =>
                        var i = 0
                        while (i <= to && i < n) {
                          elems(i) = Ctx(xs.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                          i += 1
                        }
                      case _ =>
                        val values = arr.iterator
                        var i = 0
                        while (values.hasNext && i < n) {
                          val value = values.next
                          if (i <= to) {
                            elems(i) = Ctx(value, elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                          }
                          i += 1
                        }
                    }
                    res ::= Right(elems)

                  case _ =>
                }
              case _ =>
            }

        }

      case _ => // should be array
    }
    res.reverse
  }

  private def theIdx(i: Int, size: Int) = {
    if (i >= 0) {
      if (i >= size)
        size - 1
      else
        i
    } else {
      size + i
    }
  }

  private def evaluateMapValues(syntax: MapKeysSyntax, ctx: List[Ctx]): List[Ctx] = {
    val keys = syntax.keys
    var res = List[Ctx]()
    ctx foreach {
      case currCtx @ Ctx(map: java.util.Map[_, _], schema, topLevelField, _) =>
        getValueType(schema) match {
          case Some(valueSchema) =>
            val values = keys map (key => Ctx(map.get(key), valueSchema, topLevelField, Some(TargetMap(map.asInstanceOf[java.util.Map[String, Any]], key, schema))))
            res :::= values
          case _ =>
        }
      case _ => // should be map
    }

    res.reverse
  }

  private def evaluateExpr(expr: Syntax, ctx: Ctx): Any = {
    expr match {
      case x: PathSyntax =>
        evaluatePath(x, List(ctx), false).head.value

      case x: ComparionExprSyntax =>
        evaluateComparisonExpr(x, ctx)

      case x: MathExprSyntax =>
        evaluateMathExpr(x, ctx)

      case x: LogicalExprSyntax =>
        evaluateLogicalExpr(x, ctx)

      case x: UnaryExprSyntax =>
        evaluateUnaryExpr(x, ctx)

      case x: LiteralSyntax[_] =>
        x.value

      case x: SubstSyntax => // TODO 

      case _              => // should not happen
    }
  }

  private def evaluateComparisonExpr(expr: ComparionExprSyntax, ctx: Ctx) = {
    val dest: Array[Ctx] = Array() // to be dropped
    val leftArg = expr.args(0)
    val rightArg = expr.args(1)

    val v1 = evaluateExpr(leftArg, ctx)
    val v2 = evaluateExpr(rightArg, ctx)

    val isLeftArgPath = leftArg.isInstanceOf[PathSyntax]
    val isRightArgLiteral = rightArg.isInstanceOf[LiteralSyntax[_]]

    binaryOperators(expr.op)(v1, v2)
  }

  private def writeCondition(op: String, v1Expr: Any, v2Expr: Any) = {
    binaryOperators(op)(v1Expr, v2Expr)
  }

  private def evaluateLogicalExpr(expr: LogicalExprSyntax, ctx: Ctx): Boolean = {
    val args = expr.args

    expr.op match {
      case "&&" =>
        args.foldLeft(true) { (acc, arg) =>
          acc && convertToBool(arg, evaluateExpr(arg, ctx))
        }

      case "||" =>
        args.foldLeft(false) { (acc, arg) =>
          acc || convertToBool(arg, evaluateExpr(arg, ctx))
        }
    }
  }

  private def evaluateMathExpr(expr: MathExprSyntax, ctx: Ctx) = {
    val args = expr.args

    val v1 = evaluateExpr(args(0), ctx)
    val v2 = evaluateExpr(args(1), ctx)

    binaryOperators(expr.op)(
      convertToSingleValue(args(0), v1),
      convertToSingleValue(args(1), v2))
  }

  private def evaluateUnaryExpr(expr: UnaryExprSyntax, ctx: Ctx) = {
    val arg = expr.arg
    val value = evaluateExpr(expr.arg, ctx)

    expr.op match {
      case "!" =>
        !convertToBool(arg, value)

      case "-" =>
        value match {
          case x: Int    => -x
          case x: Long   => -x
          case x: Float  => -x
          case x: Double => -x
          case _         => // error
        }
    }
  }

  private def evaluateConcatExpr(expr: ConcatExprSyntax, ctx: List[Ctx]): List[Ctx] = {

    var res = List[Ctx]()
    val args = expr.args
    val len = args.length
    var i = 0
    while (i < len) {
      val argVar = evaluatePath(args(i).asInstanceOf[PathSyntax], ctx, false)
      res :::= argVar
      i += 1
    }

    res.reverse
  }

  private def escapeStr(s: String): String = {
    s
  }

  private def inlineAppendToArray(res: Any, value: Any, tmpArr: Any, len: Any) {
  }

  private def inlinePushToArray(res: Any, value: Any) {
    //    body += (res, ".length?", res, ".push(", value, ") :", res, "[0] =", value)
  }

  private def convertToBool(arg: Syntax, value: Any): Boolean = {
    arg match {
      case x: LogicalExprSyntax =>
        value match {
          case x: Boolean => x
          case _          => false
        }

      case x: LiteralSyntax[_] =>
        value match {
          case x: Boolean => x
          case _          => false
        }

      case x: PathSyntax =>
        value match {
          case x: Boolean => x
          case _          => false
        }

      case _ =>
        value match {
          case x: Boolean => x
          case _          => false
        }
    }
  }

  private def convertToSingleValue(arg: Syntax, value: Any): Any = {
    arg match {
      case x: LiteralSyntax[_] => value
      case x: PathSyntax =>
        value match {
          case h :: xs => h
          case _       => value
        }
      case _ => value match {
        case h :: xs => h
        case _       => value
      }
    }
  }

  private def binaryOperators(op: String)(v1: Any, v2: Any) = {
    op match {
      case "===" =>
        v1 == v2

      case "==" =>
        (v1, v2) match {
          case (s1: String, s2: String) => s1.toLowerCase == s2.toLowerCase
          case _                        => v1 == v2
        }

      case ">=" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue >= c2.doubleValue
          case _                        => false
        }

      case ">" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue > c2.doubleValue
          case _                        => false
        }

      case "<=" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue <= c2.doubleValue
          case _                        => false
        }

      case "<" =>
        (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
          case (c1: Number, c2: Number) => c1.doubleValue < c2.doubleValue
          case _                        => false
        }

      case "!==" =>
        v1 != v2

      case "!=" =>
        v1 != v2

      case "^==" =>
        (v1, v2) match {
          case (s1: String, s2: String) => s1.startsWith(s2)
          case _                        => false
        }

      case "^=" =>
        (v1, v2) match {
          case (s1: String, s2: String) => s1.toLowerCase.startsWith(s2.toLowerCase)
          case _                        => false
        }

      case "$==" =>
        (v1, v2) match {
          case (s1: String, s2: String) => s1.endsWith(s2)
          case _                        => false
        }

      case "$=" =>
        (v1, v2) match {
          case (s1: String, s2: String) => s1.toLowerCase.endsWith(s2.toLowerCase)
          case _                        => false
        }

      case "*==" =>
        (v1, v2) match {
          case (s1: String, s2: String) => s1.contains(s2)
          case _                        => false
        }

      case "*=" =>
        (v1, v2) match {
          case (s1: String, s2: String) => s1.toLowerCase.contains(s2.toLowerCase)
          case _                        => false
        }

      case "+" =>
        (v1, v2) match {
          case (c1: Int, c2: Int)       => c1 + c2
          case (c1: Int, c2: Long)      => c1 + c2
          case (c1: Int, c2: Float)     => c1 + c2
          case (c1: Int, c2: Double)    => c1 + c2
          case (c1: Long, c2: Int)      => c1 + c2
          case (c1: Long, c2: Long)     => c1 + c2
          case (c1: Long, c2: Float)    => c1 + c2
          case (c1: Long, c2: Double)   => c1 + c2
          case (c1: Float, c2: Int)     => c1 + c2
          case (c1: Float, c2: Long)    => c1 + c2
          case (c1: Float, c2: Float)   => c1 + c2
          case (c1: Float, c2: Double)  => c1 + c2
          case (c1: Double, c2: Int)    => c1 + c2
          case (c1: Double, c2: Long)   => c1 + c2
          case (c1: Double, c2: Float)  => c1 + c2
          case (c1: Double, c2: Double) => c1 + c2
          case _ =>
            (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
              case (c1: Number, c2: Number) => c1.doubleValue + c2.doubleValue
              case _                        => Double.NaN
            }
        }

      case "-" =>
        (v1, v2) match {
          case (c1: Int, c2: Int)       => c1 - c2
          case (c1: Int, c2: Long)      => c1 - c2
          case (c1: Int, c2: Float)     => c1 - c2
          case (c1: Int, c2: Double)    => c1 - c2
          case (c1: Long, c2: Int)      => c1 - c2
          case (c1: Long, c2: Long)     => c1 - c2
          case (c1: Long, c2: Float)    => c1 - c2
          case (c1: Long, c2: Double)   => c1 - c2
          case (c1: Float, c2: Int)     => c1 - c2
          case (c1: Float, c2: Long)    => c1 - c2
          case (c1: Float, c2: Float)   => c1 - c2
          case (c1: Float, c2: Double)  => c1 - c2
          case (c1: Double, c2: Int)    => c1 - c2
          case (c1: Double, c2: Long)   => c1 - c2
          case (c1: Double, c2: Float)  => c1 - c2
          case (c1: Double, c2: Double) => c1 - c2
          case _ =>
            (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
              case (c1: Number, c2: Number) => c1.doubleValue - c2.doubleValue
              case _                        => Double.NaN
            }
        }

      case "*" =>
        (v1, v2) match {
          case (c1: Int, c2: Int)       => c1 * c2
          case (c1: Int, c2: Long)      => c1 * c2
          case (c1: Int, c2: Float)     => c1 * c2
          case (c1: Int, c2: Double)    => c1 * c2
          case (c1: Long, c2: Int)      => c1 * c2
          case (c1: Long, c2: Long)     => c1 * c2
          case (c1: Long, c2: Float)    => c1 * c2
          case (c1: Long, c2: Double)   => c1 * c2
          case (c1: Float, c2: Int)     => c1 * c2
          case (c1: Float, c2: Long)    => c1 * c2
          case (c1: Float, c2: Float)   => c1 * c2
          case (c1: Float, c2: Double)  => c1 * c2
          case (c1: Double, c2: Int)    => c1 * c2
          case (c1: Double, c2: Long)   => c1 * c2
          case (c1: Double, c2: Float)  => c1 * c2
          case (c1: Double, c2: Double) => c1 * c2
          case _ =>
            (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
              case (c1: Number, c2: Number) => c1.doubleValue * c2.doubleValue
              case _                        => Double.NaN
            }
        }

      case "/" =>
        (v1, v2) match {
          case (c1: Int, c2: Int)       => c1 / c2
          case (c1: Int, c2: Long)      => c1 / c2
          case (c1: Int, c2: Float)     => c1 / c2
          case (c1: Int, c2: Double)    => c1 / c2
          case (c1: Long, c2: Int)      => c1 / c2
          case (c1: Long, c2: Long)     => c1 / c2
          case (c1: Long, c2: Float)    => c1 / c2
          case (c1: Long, c2: Double)   => c1 / c2
          case (c1: Float, c2: Int)     => c1 / c2
          case (c1: Float, c2: Long)    => c1 / c2
          case (c1: Float, c2: Float)   => c1 / c2
          case (c1: Float, c2: Double)  => c1 / c2
          case (c1: Double, c2: Int)    => c1 / c2
          case (c1: Double, c2: Long)   => c1 / c2
          case (c1: Double, c2: Float)  => c1 / c2
          case (c1: Double, c2: Double) => c1 / c2
          case _ =>
            (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
              case (c1: Number, c2: Number) => c1.doubleValue / c2.doubleValue
              case _                        => Double.NaN
            }
        }

      case "%" =>
        (v1, v2) match {
          case (c1: Int, c2: Int)       => c1 % c2
          case (c1: Int, c2: Long)      => c1 % c2
          case (c1: Int, c2: Float)     => c1 % c2
          case (c1: Int, c2: Double)    => c1 % c2
          case (c1: Long, c2: Int)      => c1 % c2
          case (c1: Long, c2: Long)     => c1 % c2
          case (c1: Long, c2: Float)    => c1 % c2
          case (c1: Long, c2: Double)   => c1 % c2
          case (c1: Float, c2: Int)     => c1 % c2
          case (c1: Float, c2: Long)    => c1 % c2
          case (c1: Float, c2: Float)   => c1 % c2
          case (c1: Float, c2: Double)  => c1 % c2
          case (c1: Double, c2: Int)    => c1 % c2
          case (c1: Double, c2: Long)   => c1 % c2
          case (c1: Double, c2: Float)  => c1 % c2
          case (c1: Double, c2: Double) => c1 % c2
          case _ =>
            (v1.asInstanceOf[AnyRef], v2.asInstanceOf[AnyRef]) match {
              case (c1: Number, c2: Number) => c1.doubleValue % c2.doubleValue
              case _                        => Double.NaN
            }
        }
    }
  }

  /**
   * For array
   */
  private def getElementType(schema: Schema): Option[Schema] = {
    schema.getType match {
      case Type.ARRAY => Option(schema.getElementType)
      case Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: Option[Schema] = None
        while (unions.hasNext && res == None) {
          val union = unions.next
          if (union.getType == Type.ARRAY) {
            res = Some(union.getElementType)
          }
        }
        res
      case _ => None
    }
  }

  /**
   * For map
   */
  private def getValueType(schema: Schema): Option[Schema] = {
    schema.getType match {
      case Type.MAP => Option(schema.getValueType)
      case Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: Option[Schema] = None
        while (unions.hasNext && res == None) {
          val union = unions.next
          if (union.getType == Type.MAP) {
            res = Some(union.getValueType)
          }
        }
        res
      case _ => None
    }
  }
}
