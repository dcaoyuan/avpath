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
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import scala.collection.mutable

object Evaluator {
  sealed trait Target
  final case class TargetRecord(record: GenericData.Record, field: Schema.Field) extends Target
  final case class TargetArray(array: GenericData.Array[_], idx: Int, arraySchema: Schema) extends Target
  final case class TargetMap(map: java.util.Map[String, AnyRef], key: String, mapSchema: Schema) extends Target

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
        val processingArrs = new mutable.HashMap[GenericData.Array[_], (Schema, List[Int])]()
        targets(ctxs) foreach {
          case _: TargetRecord => // cannot apply delete on record

          case TargetArray(arr, idx, schema) =>
            processingArrs += arr -> (schema, idx :: processingArrs.getOrElse(arr, (schema, List[Int]()))._2)

          case TargetMap(map, key, _) =>
            map.asInstanceOf[java.util.Map[String, AnyRef]].remove(key)
        }

        for ((arr, (schema, toRemoved)) <- processingArrs) {
          arrayRemove(arr, toRemoved)
        }

      case Clear =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: GenericData.Array[_] => arr.clear
              case map: java.util.Map[_, _]  => map.clear
              case _                         => // ?
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
              case v: GenericData.Record => replace(rec, v)
              case _                     => // log.error 
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
                map.asInstanceOf[java.util.Map[String, AnyRef]].put(key, value1.asInstanceOf[AnyRef])

              case None =>
            }
        }

      case Insert =>
        targets(ctxs) foreach {
          case TargetRecord(rec, field) =>
            rec.get(field.pos) match {
              case arr: GenericData.Array[_] =>
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
                  case (k: String, v: Any) =>
                    map.asInstanceOf[java.util.Map[String, AnyRef]].put(k, v.asInstanceOf[AnyRef])
                  case kvs: java.util.Map[_, _] =>
                    val entries = kvs.entrySet.iterator
                    if (entries.hasNext) {
                      val entry = entries.next // should contains only one entry
                      map.asInstanceOf[java.util.Map[String, AnyRef]].put(entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[AnyRef])
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
              case arr: GenericData.Array[_] =>
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
                        import scala.collection.JavaConversions._
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

                      case _ => // ?
                    }
                  case _ =>
                }

              case map: java.util.Map[_, _] =>
                val value1 = if (isJsonValue) FromJson.fromJsonString(value.asInstanceOf[String], field.schema, false) else value
                value1 match {
                  case xs: List[_] =>
                    xs foreach {
                      case (k: String, v: Any) => map.asInstanceOf[java.util.Map[String, AnyRef]].put(k, v.asInstanceOf[AnyRef])
                      case _                   => // ?
                    }

                  case xs: java.util.Map[_, _] =>
                    import scala.collection.JavaConversions._
                    xs foreach {
                      case (k: String, v: Any) => map.asInstanceOf[java.util.Map[String, AnyRef]].put(k, v.asInstanceOf[AnyRef])
                      case _                   => // ?
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

  private def arrayUpdate[T](arr: GenericData.Array[_], idx: Int, value: T) {
    if (idx >= 0 && idx < arr.size) {
      arr.asInstanceOf[GenericData.Array[T]].set(idx, value)
    }
  }

  private def arrayInsert[T](arr: GenericData.Array[_], value: T) {
    arr.asInstanceOf[GenericData.Array[T]].add(value)
  }

  private def arrayRemove[T](arr: GenericData.Array[T], _toRemoved: List[Int]) {
    val xs = new java.util.ArrayList[T]()
    var toRemoved = _toRemoved.sortBy(-_)
    while (toRemoved != Nil) {
      arr.remove(toRemoved.head)
      toRemoved = toRemoved.tail
    }
  }

  private def replace(dst: GenericData.Record, src: GenericData.Record) {
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

  private def evaluateSelector(sel: SelectorSyntax, ctx: List[Ctx], isTopLevel: Boolean): List[Ctx] = {
    var res = List[Ctx]()
    sel.prop match {
      case null =>
        // select record itself
        ctx foreach {
          case Ctx(rec: GenericData.Record, schema, topLevelField, _) =>
            res ::= Ctx(rec, schema, null, Some(TargetRecord(rec, null)))

          case _ => // should be rec
        }
      case "*" =>
        ctx foreach {
          case Ctx(rec: GenericData.Record, schema, topLevelField, _) =>
            val fields = getFields(schema)
            var n = fields.size
            var j = 0
            while (j < n) {
              val field = fields.get(j)
              res ::= Ctx(rec.get(j), field.schema, if (topLevelField == null) field else topLevelField, Some(TargetRecord(rec, field)))
              j += 1
            }
          case Ctx(arr: GenericData.Array[_], schema, topLevelField, _) =>
            var n = arr.size
            var j = 0
            while (j < n) {
              getElementType(schema) match {
                case Some(elemType) => res ::= Ctx(arr.get(j), elemType, topLevelField, Some(TargetArray(arr, j, schema)))
                case _              =>
              }
              j += 1
            }
          case _ => // TODO map
        }

      case fieldName =>
        ctx foreach {
          case Ctx(rec: GenericData.Record, schema, topLevelField, _) =>
            getField(schema, fieldName) match {
              case Some(field) => res ::= Ctx(rec.get(field.pos), field.schema, if (topLevelField == null) field else topLevelField, Some(TargetRecord(rec, field)))
              case _           =>
            }

          case _ => // should be rec
        }
    }

    res.reverse
  }

  private def evaluateDescendantSelector(sel: SelectorSyntax, baseCtx: Any) = {
  }

  private def evaluateObjectPredicate(expr: ObjPredSyntax, ctx: List[Ctx]): List[Ctx] = {
    var res = List[Ctx]()
    ctx foreach {
      case currCtx @ Ctx(rec: GenericData.Record, schema, _, _) =>
        evaluateExpr(expr.arg, currCtx) match {
          case true => res ::= currCtx
          case _    =>
        }

      case currCtx @ Ctx(arr: GenericData.Array[_], schema, topLevelField, _) =>
        var i = 0
        while (i < arr.size) {
          getElementType(schema) match {
            case Some(elemType) =>
              val elemCtx = Ctx(arr.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
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
      case currCtx @ Ctx(arr: GenericData.Array[_], schema, topLevelField, _) =>
        posExpr match {
          case PosSyntax(LiteralSyntax("*"), _, _) =>
            val n = arr.size
            val elems = Array.ofDim[Ctx](n)
            var i = 0
            while (i < n) {
              getElementType(schema) match {
                case Some(elemType) => elems(i) = Ctx(arr.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
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
                getElementType(schema) match {
                  case Some(elemType) => res ::= Left(Ctx(arr.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema))))
                  case _              =>
                }
              case _ =>
            }

          case PosSyntax(null, fromIdxSyn: Syntax, toIdxSyn: Syntax) =>
            evaluateExpr(fromIdxSyn, currCtx) match {
              case fromIdx: Int =>
                evaluateExpr(toIdxSyn, currCtx) match {
                  case toIdx: Int =>
                    val n = arr.size
                    val from = theIdx(fromIdx, n)
                    val to = theIdx(toIdx, n)
                    val elems = Array.ofDim[Ctx](to - from + 1)
                    var i = from
                    while (i <= to) {
                      getElementType(schema) match {
                        case Some(elemType) => elems(i - from) = Ctx(arr.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                        case _              =>
                      }
                      i += 1
                    }
                    res ::= Right(elems)

                  case _ =>
                }

              case _ =>
            }

          case PosSyntax(null, fromIdxSyn: Syntax, null) =>
            evaluateExpr(fromIdxSyn, currCtx) match {
              case fromIdx: Int =>
                val n = arr.size
                val from = theIdx(fromIdx, n)
                val elems = Array.ofDim[Ctx](n - from)
                var i = from
                while (i < n) {
                  getElementType(schema) match {
                    case Some(elemType) => elems(i - from) = Ctx(arr.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                    case _              =>
                  }
                  i += 1
                }
                res ::= Right(elems)

              case _ =>
            }

          case PosSyntax(null, null, toIdxSyn: Syntax) =>
            evaluateExpr(toIdxSyn, currCtx) match {
              case toIdx: Int =>
                val n = arr.size
                val to = theIdx(toIdx, n)
                val elems = Array.ofDim[Ctx](to + 1)
                var i = 0
                while (i <= to && i < n) {
                  getElementType(schema) match {
                    case Some(elemType) => elems(i) = Ctx(arr.get(i), elemType, topLevelField, Some(TargetArray(arr, i, schema)))
                    case _              =>
                  }
                  i += 1
                }
                res ::= Right(elems)

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
            val values = keys map (key => Ctx(map.get(key), valueSchema, topLevelField, Some(TargetMap(map.asInstanceOf[java.util.Map[String, AnyRef]], key, schema))))
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

  private def getField(schema: Schema, key: String): Option[Schema.Field] = {
    schema.getType match {
      case Schema.Type.RECORD => Option(schema.getField(key))
      case Schema.Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: Option[Schema.Field] = None
        while (unions.hasNext && res == None) {
          val union = unions.next
          if (union.getType == Schema.Type.ARRAY) {
            res = Option(union.getField(key))
          }
        }
        res
      case _ => None
    }
  }

  private def getFields(schema: Schema): java.util.List[Schema.Field] = {
    schema.getType match {
      case Schema.Type.RECORD => schema.getFields
      case Schema.Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: java.util.List[Schema.Field] = null
        while (unions.hasNext && res == null) {
          val union = unions.next
          if (union.getType == Schema.Type.ARRAY) {
            res = union.getFields
          }
        }
        res
      case _ => java.util.Collections.emptyList[Schema.Field]
    }
  }

  private def getElementType(schema: Schema): Option[Schema] = {
    schema.getType match {
      case Schema.Type.ARRAY => Option(schema.getElementType)
      case Schema.Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: Option[Schema] = None
        while (unions.hasNext && res == None) {
          val union = unions.next
          if (union.getType == Schema.Type.ARRAY) {
            res = Some(union.getElementType)
          }
        }
        res
      case _ => None
    }
  }

  private def getValueType(schema: Schema): Option[Schema] = {
    schema.getType match {
      case Schema.Type.MAP => Option(schema.getValueType)
      case Schema.Type.UNION =>
        val unions = schema.getTypes.iterator
        var res: Option[Schema] = None
        while (unions.hasNext && res == None) {
          val union = unions.next
          if (union.getType == Schema.Type.MAP) {
            res = Some(union.getValueType)
          }
        }
        res
      case _ => None
    }
  }
}
