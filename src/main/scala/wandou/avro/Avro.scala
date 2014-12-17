package wandou.avro

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.ByteBuffer
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic._
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectDatumReader
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.collection.JavaConverters._

/**
 *
 */
object Avro {

  def avroEncode[T](value: T, schema: Schema): Try[Array[Byte]] = encode[T](value, schema)

  private def encode[T](value: T, schema: Schema): Try[Array[Byte]] = {
    var out: ByteArrayOutputStream = null
    try {
      out = new ByteArrayOutputStream()

      val encoder = EncoderFactory.get.binaryEncoder(out, null)

      val writer = new GenericDatumWriter[T](schema)
      writer.write(value, encoder)
      encoder.flush()

      Success(out.toByteArray)
    } catch {
      case ex: Throwable => Failure(ex)
    } finally {
      if (out != null) try { out.close } catch { case _: Throwable => }
    }
  }

  def avroDecode[T](bytes: Array[Byte], schema: Schema, specified: Boolean = false, other: T = null.asInstanceOf[T]): Try[T] = decode[T](bytes, schema, other, specified)

  private def decode[T](bytes: Array[Byte], schema: Schema, other: T, toReflect: Boolean): Try[T] = {
    var in: InputStream = null
    try {
      in = new ByteArrayInputStream(bytes)

      val decoder = DecoderFactory.get.binaryDecoder(in, null)

      val reader = if (toReflect) new ReflectDatumReader[T](schema) else new GenericDatumReader[T](schema)
      val value = reader.read(other, decoder)

      Success(value)
    } catch {
      case ex: Throwable => Failure(ex)
    } finally {
      if (in != null) try { in.close } catch { case _: Throwable => }
    }
  }

  def replace(dst: Record, src: Record) {
    if (dst.getSchema == src.getSchema) {
      dst.getSchema.getFields.asScala.foreach { f =>
        val v = src.get(f.name)
        val t = f.schema.getType
        if (v != null && (t != Type.ARRAY || !v.asInstanceOf[java.util.Collection[_]].isEmpty) && (t != Type.MAP || !v.asInstanceOf[java.util.Map[_, _]].isEmpty)) {
          dst.put(f.name, v)
        }
      }
    }
  }

  /*
   * TODO recursily processing for complex type.
   * TODO add UT
   */
  def put(record: Record, key: String, value: Any) {
    val field = record.getSchema.getField(key)
    val fieldSchema = field.schema
    fieldSchema.getType match {
      case Type.ARRAY =>
        handleArrayType(record, key, value, fieldSchema)

      case Type.UNION =>
        val unionFiledSchema = fieldSchema.getTypes.get(0)
        unionFiledSchema.getType match {
          case Type.ARRAY =>
            handleArrayType(record, key, value, unionFiledSchema)
          case _ =>
            record.put(key, value)
        }

      case _ =>
        record.put(key, value)
    }
  }

  private def handleArrayType(record: Record, key: String, value: Any, arraySchema: Schema) {
    arraySchema.getElementType.getType match {
      case Type.BOOLEAN => addToArray(record, key, value.asInstanceOf[Boolean], arraySchema)
      case Type.INT     => addToArray(record, key, value.asInstanceOf[Int], arraySchema)
      case Type.LONG    => addToArray(record, key, value.asInstanceOf[Long], arraySchema)
      case Type.FLOAT   => addToArray(record, key, value.asInstanceOf[Float], arraySchema)
      case Type.DOUBLE  => addToArray(record, key, value.asInstanceOf[Double], arraySchema)
      case Type.BYTES   => addToArray(record, key, value.asInstanceOf[ByteBuffer], arraySchema)
      case Type.STRING  => addToArray(record, key, value.asInstanceOf[CharSequence], arraySchema)
      case Type.RECORD  => addToArray(record, key, value.asInstanceOf[IndexedRecord], arraySchema)
      case Type.ENUM    => addToArray(record, key, value.asInstanceOf[GenericEnumSymbol], arraySchema)
      case Type.ARRAY   => addToArray(record, key, value.asInstanceOf[java.util.Collection[_]], arraySchema)
      case Type.MAP     => addToArray(record, key, value.asInstanceOf[java.util.Map[_, _]], arraySchema)
      case Type.FIXED   => addToArray(record, key, value.asInstanceOf[GenericFixed], arraySchema)
      case _            => // todo
    }
  }

  private def addToArray[T](record: Record, key: String, value: T, arraySchema: Schema) {
    val elems = record.get(key) match {
      case null =>
        val xs = new GenericData.Array[T](0, arraySchema)
        record.put(key, xs)
        xs
      case xs: GenericData.Array[_] => xs.asInstanceOf[GenericData.Array[T]]
    }
    elems.add(value.asInstanceOf[T])
  }

  /**
   * Only support array field only
   */
  def limitToSize(record: Record, key: String, size: Int) {
    val field = record.getSchema.getField(key)
    limitToSize(record, field, size)
  }

  def limitToSize(record: Record, field: Schema.Field, size: Int) {
    val fieldSchema = field.schema
    fieldSchema.getType match {
      case Type.ARRAY =>
        fieldSchema.getElementType.getType match {
          case Type.INT     => arrayLimitToSize[Int](record, field, size, fieldSchema)
          case Type.LONG    => arrayLimitToSize[Long](record, field, size, fieldSchema)
          case Type.FLOAT   => arrayLimitToSize[Float](record, field, size, fieldSchema)
          case Type.DOUBLE  => arrayLimitToSize[Double](record, field, size, fieldSchema)
          case Type.BOOLEAN => arrayLimitToSize[Boolean](record, field, size, fieldSchema)
          case Type.BYTES   => arrayLimitToSize[ByteBuffer](record, field, size, fieldSchema)
          case Type.STRING  => arrayLimitToSize[CharSequence](record, field, size, fieldSchema)
          case Type.FIXED   => arrayLimitToSize[GenericFixed](record, field, size, fieldSchema)
          case Type.RECORD  => arrayLimitToSize[IndexedRecord](record, field, size, fieldSchema)
          case Type.ENUM    => arrayLimitToSize[GenericEnumSymbol](record, field, size, fieldSchema)
          case Type.MAP     => arrayLimitToSize[java.util.Map[_, _]](record, field, size, fieldSchema)
          case Type.ARRAY   => arrayLimitToSize[java.util.Collection[_]](record, field, size, fieldSchema)
          case _            => // todo
        }
      case _ =>
    }
  }

  private def arrayLimitToSize[T](record: Record, field: Schema.Field, size: Int, elemSchema: Schema) = {
    record.get(field.name) match {
      case null =>
      case arr: GenericData.Array[_] =>
        val l = arr.size
        if (l > size) {
          val ori = arr.asInstanceOf[GenericData.Array[T]]
          val xs = new GenericData.Array[T](size, elemSchema)
          var i = l - size
          while (i < l) {
            xs.add(ori.get(i))
            i += 1
          }

          record.put(field.name, xs)
        }
    }
  }

}
