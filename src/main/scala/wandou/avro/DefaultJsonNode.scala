package wandou.avro

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type
import org.codehaus.jackson.JsonNode

object DefaultJsonNode {
  import FromJson.mapper

  val typeToNode = Map(
    Type.RECORD -> mapper.readTree("{}"),
    Type.ENUM -> mapper.readTree("\"\""),
    Type.ARRAY -> mapper.readTree("[]"),
    Type.MAP -> mapper.readTree("{}"),
    Type.UNION -> mapper.readTree("null"),
    Type.FIXED -> mapper.readTree("\"\""),
    Type.STRING -> mapper.readTree("\"\""),
    Type.BYTES -> mapper.readTree("\"\""),
    Type.INT -> mapper.readTree("0"),
    Type.LONG -> mapper.readTree("0"),
    Type.FLOAT -> mapper.readTree("0.0"),
    Type.DOUBLE -> mapper.readTree("0.0"),
    Type.BOOLEAN -> mapper.readTree("false"),
    Type.NULL -> mapper.readTree("null"))

  def nodeOf(field: Field): JsonNode = typeToNode(field.schema.getType)
}