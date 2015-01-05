package wandou.avro

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

object DefaultJsonNode {

  private val DEFAULT_JSON_NODES = {
    val mapper = new ObjectMapper()
    val record = mapper.readTree("{}")
    val enum = mapper.readTree("\"\"")
    val array = mapper.readTree("[]")
    val map = mapper.readTree("{}")
    val union = mapper.readTree("null")
    val fixed = mapper.readTree("\"\"")
    val string = mapper.readTree("\"\"")
    val bytes = mapper.readTree("\"\"")
    val intValue = mapper.readTree("0")
    val longValue = mapper.readTree("0")
    val floatValue = mapper.readTree("0.0")
    val doubleValue = mapper.readTree("0.0")
    val booleanValue = mapper.readTree("false")
    val nullValue = mapper.readTree("null")
    (record, enum, array, map, union, fixed, string, bytes, intValue, longValue, floatValue, doubleValue, booleanValue, nullValue)
  }

  val RECORD = DEFAULT_JSON_NODES._1
  val ENUM = DEFAULT_JSON_NODES._2
  val ARRAY = DEFAULT_JSON_NODES._3
  val MAP = DEFAULT_JSON_NODES._4
  val UNION = DEFAULT_JSON_NODES._5
  val FIXED = DEFAULT_JSON_NODES._6
  val STRING = DEFAULT_JSON_NODES._7
  val BYTES = DEFAULT_JSON_NODES._8
  val INT = DEFAULT_JSON_NODES._9
  val LONG = DEFAULT_JSON_NODES._10
  val FLOAT = DEFAULT_JSON_NODES._11
  val DOUBLE = DEFAULT_JSON_NODES._12
  val BOOLEAN = DEFAULT_JSON_NODES._13
  val NULL = DEFAULT_JSON_NODES._14

  def of(field: Field): JsonNode = {
    import Schema.Type
    field.schema.getType match {
      case Type.RECORD  => RECORD
      case Type.ENUM    => ENUM
      case Type.ARRAY   => ARRAY
      case Type.MAP     => MAP
      case Type.UNION   => UNION
      case Type.FIXED   => FIXED
      case Type.STRING  => STRING
      case Type.BYTES   => BYTES
      case Type.INT     => INT
      case Type.LONG    => LONG
      case Type.FLOAT   => FLOAT
      case Type.DOUBLE  => DOUBLE
      case Type.BOOLEAN => BOOLEAN
      case Type.NULL    => NULL
    }
  }
}