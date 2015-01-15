package wandou

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.ByteBuffer
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectDatumReader
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.collection.JavaConverters._

/**
 * For generic presentation:
 * Schema records are implemented as GenericRecord.
 * Schema enums are implemented as GenericEnumSymbol.
 * Schema arrays are implemented as Collection.
 * Schema maps are implemented as Map.
 * Schema fixed are implemented as GenericFixed.
 * Schema strings are implemented as CharSequence.
 * Schema bytes are implemented as ByteBuffer.
 * Schema ints are implemented as Integer.
 * Schema longs are implemented as Long.
 * Schema floats are implemented as Float.
 * Schema doubles are implemented as Double.
 * Schema booleans are implemented as Boolean.
 *
 * For specific presentation:
 * Record, enum, and fixed schemas generate Java class definitions.
 * All other types are mapped as in the generic API.
 */
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

  def newGenericArray(capacity: Int, schema: Schema): GenericData.Array[_] = {
    schema.getElementType.getType match {
      case Type.BOOLEAN => new GenericData.Array[Boolean](capacity, schema)
      case Type.INT     => new GenericData.Array[Int](capacity, schema)
      case Type.LONG    => new GenericData.Array[Long](capacity, schema)
      case Type.FLOAT   => new GenericData.Array[Float](capacity, schema)
      case Type.DOUBLE  => new GenericData.Array[Double](capacity, schema)
      case Type.BYTES   => new GenericData.Array[ByteBuffer](capacity, schema)
      case Type.STRING  => new GenericData.Array[CharSequence](capacity, schema)
      case Type.RECORD  => new GenericData.Array[IndexedRecord](capacity, schema)
      case Type.ENUM    => new GenericData.Array[GenericEnumSymbol](capacity, schema)
      case Type.ARRAY   => new GenericData.Array[java.util.Collection[_]](capacity, schema)
      case Type.MAP     => new GenericData.Array[java.util.Map[_, _]](capacity, schema)
      case Type.FIXED   => new GenericData.Array[GenericFixed](capacity, schema)
      case _            => new GenericData.Array[Any](capacity, schema)
    }
  }

  def addGenericArray(array: GenericData.Array[_], value: Any) {
    array.getSchema.getElementType.getType match {
      case Type.BOOLEAN => array.asInstanceOf[GenericData.Array[Boolean]].add(value.asInstanceOf[Boolean])
      case Type.INT     => array.asInstanceOf[GenericData.Array[Int]].add(value.asInstanceOf[Int])
      case Type.LONG    => array.asInstanceOf[GenericData.Array[Long]].add(value.asInstanceOf[Long])
      case Type.FLOAT   => array.asInstanceOf[GenericData.Array[Float]].add(value.asInstanceOf[Float])
      case Type.DOUBLE  => array.asInstanceOf[GenericData.Array[Double]].add(value.asInstanceOf[Double])
      case Type.BYTES   => array.asInstanceOf[GenericData.Array[ByteBuffer]].add(value.asInstanceOf[ByteBuffer])
      case Type.STRING  => array.asInstanceOf[GenericData.Array[CharSequence]].add(value.asInstanceOf[CharSequence])
      case Type.RECORD  => array.asInstanceOf[GenericData.Array[IndexedRecord]].add(value.asInstanceOf[IndexedRecord])
      case Type.ENUM    => array.asInstanceOf[GenericData.Array[GenericEnumSymbol]].add(value.asInstanceOf[GenericEnumSymbol])
      case Type.ARRAY   => array.asInstanceOf[GenericData.Array[java.util.Collection[_]]].add(value.asInstanceOf[java.util.Collection[_]])
      case Type.MAP     => array.asInstanceOf[GenericData.Array[java.util.Map[_, _]]].add(value.asInstanceOf[java.util.Map[_, _]])
      case Type.FIXED   => array.asInstanceOf[GenericData.Array[GenericFixed]].add(value.asInstanceOf[GenericFixed])
      case _            => //TODO array.asInstanceOf[GenericData.Array[_]].add(value)
    }
  }

  /**
   * Only support array field
   * TODO map field
   */
  def toLimitedSize(record: IndexedRecord, key: String, size: Int): Option[java.util.Collection[_]] = {
    val field = record.getSchema.getField(key)
    toLimitedSize(record, field, size)
  }

  def toLimitedSize(record: IndexedRecord, field: Schema.Field, size: Int): Option[java.util.Collection[_]] = {
    val fieldSchema = field.schema
    fieldSchema.getType match {
      case Type.ARRAY =>
        val values = record.get(field.pos)
        val xs = fieldSchema.getElementType.getType match {
          case Type.INT     => toLimitedSize[Int](values.asInstanceOf[java.util.Collection[Int]], size, fieldSchema)
          case Type.LONG    => toLimitedSize[Long](values.asInstanceOf[java.util.Collection[Long]], size, fieldSchema)
          case Type.FLOAT   => toLimitedSize[Float](values.asInstanceOf[java.util.Collection[Float]], size, fieldSchema)
          case Type.DOUBLE  => toLimitedSize[Double](values.asInstanceOf[java.util.Collection[Double]], size, fieldSchema)
          case Type.BOOLEAN => toLimitedSize[Boolean](values.asInstanceOf[java.util.Collection[Boolean]], size, fieldSchema)
          case Type.BYTES   => toLimitedSize[ByteBuffer](values.asInstanceOf[java.util.Collection[ByteBuffer]], size, fieldSchema)
          case Type.STRING  => toLimitedSize[CharSequence](values.asInstanceOf[java.util.Collection[CharSequence]], size, fieldSchema)
          case Type.FIXED   => toLimitedSize[GenericFixed](values.asInstanceOf[java.util.Collection[GenericFixed]], size, fieldSchema)
          case Type.RECORD  => toLimitedSize[IndexedRecord](values.asInstanceOf[java.util.Collection[IndexedRecord]], size, fieldSchema)
          case Type.ENUM    => toLimitedSize[GenericEnumSymbol](values.asInstanceOf[java.util.Collection[GenericEnumSymbol]], size, fieldSchema)
          case Type.MAP     => toLimitedSize[java.util.Map[_, _]](values.asInstanceOf[java.util.Collection[java.util.Map[_, _]]], size, fieldSchema)
          case Type.ARRAY   => toLimitedSize[java.util.Collection[_]](values.asInstanceOf[java.util.Collection[java.util.Collection[_]]], size, fieldSchema)
          case _            => values.asInstanceOf[java.util.Collection[_]] // todo
        }
        Some(xs)
      case _ =>
        None
    }
  }

  /**
   * @return an unchanged array or a new array, The original values will never be changed
   */
  def toLimitedSize[T](values: java.util.Collection[T], size: Int, fieldSchema: Schema): java.util.Collection[T] = {
    val l = values.size
    if (l > size) {
      val xs = values.getClass.newInstance
      val arr = values.toArray.asInstanceOf[Array[T]]
      var i = l - size
      while (i < l) {
        xs.add(arr(i))
        i += 1
      }
      xs
    } else {
      values
    }
  }

}
