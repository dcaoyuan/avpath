package wandou

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

package object avro {

  def avroEncode[T](value: T, schema: Schema): Try[Array[Byte]] = encode[T](value, schema)
  def avroDecode[T](bytes: Array[Byte], schema: Schema, specified: Boolean = false, other: T = null.asInstanceOf[T]): Try[T] = decode[T](bytes, schema, other, specified)

  def jsonEncode(value: Any, schema: Schema): Try[String] =
    try {
      val json = ToJson.toAvroJsonString(value, schema)
      Success(json)
    } catch {
      case ex: Throwable => Failure(ex)
    }

  def jsonDecode(json: String, schema: Schema, specified: Boolean = false): Try[_] =
    try {
      val value = FromJson.fromJsonString(json, schema, specified)
      Success(value)
    } catch {
      case ex: Throwable => Failure(ex)
    }

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
   * TODO map field
   */
  def toLimitedSize(record: Record, key: String, size: Int): Option[GenericData.Array[_]] = {
    val field = record.getSchema.getField(key)
    toLimitedSize(record, field, size)
  }

  def toLimitedSize(record: Record, field: Schema.Field, size: Int): Option[GenericData.Array[_]] = {
    val fieldSchema = field.schema
    fieldSchema.getType match {
      case Type.ARRAY =>
        val values = record.get(field.pos)
        val xs = fieldSchema.getElementType.getType match {
          case Type.INT     => toLimitedSize[Int](values.asInstanceOf[GenericData.Array[Int]], size, fieldSchema)
          case Type.LONG    => toLimitedSize[Long](values.asInstanceOf[GenericData.Array[Long]], size, fieldSchema)
          case Type.FLOAT   => toLimitedSize[Float](values.asInstanceOf[GenericData.Array[Float]], size, fieldSchema)
          case Type.DOUBLE  => toLimitedSize[Double](values.asInstanceOf[GenericData.Array[Double]], size, fieldSchema)
          case Type.BOOLEAN => toLimitedSize[Boolean](values.asInstanceOf[GenericData.Array[Boolean]], size, fieldSchema)
          case Type.BYTES   => toLimitedSize[ByteBuffer](values.asInstanceOf[GenericData.Array[ByteBuffer]], size, fieldSchema)
          case Type.STRING  => toLimitedSize[CharSequence](values.asInstanceOf[GenericData.Array[CharSequence]], size, fieldSchema)
          case Type.FIXED   => toLimitedSize[GenericFixed](values.asInstanceOf[GenericData.Array[GenericFixed]], size, fieldSchema)
          case Type.RECORD  => toLimitedSize[IndexedRecord](values.asInstanceOf[GenericData.Array[IndexedRecord]], size, fieldSchema)
          case Type.ENUM    => toLimitedSize[GenericEnumSymbol](values.asInstanceOf[GenericData.Array[GenericEnumSymbol]], size, fieldSchema)
          case Type.MAP     => toLimitedSize[java.util.Map[_, _]](values.asInstanceOf[GenericData.Array[java.util.Map[_, _]]], size, fieldSchema)
          case Type.ARRAY   => toLimitedSize[java.util.Collection[_]](values.asInstanceOf[GenericData.Array[java.util.Collection[_]]], size, fieldSchema)
          case _            => values.asInstanceOf[GenericData.Array[_]] // todo
        }
        Some(xs)
      case _ =>
        None
    }
  }

  /**
   * @return an unchanged array or a new array, The original values will never be changed
   */
  def toLimitedSize[T](values: GenericData.Array[T], size: Int, fieldSchema: Schema): GenericData.Array[T] = {
    val l = values.size
    if (l > size) {
      val xs = new GenericData.Array[T](size, fieldSchema)
      var i = l - size
      while (i < l) {
        xs.add(values.get(i))
        i += 1
      }
      xs
    } else {
      values
    }
  }

}
