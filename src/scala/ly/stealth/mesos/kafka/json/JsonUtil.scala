package ly.stealth.mesos.kafka.json

import com.fasterxml.jackson.annotation.JsonInclude
import java.text.SimpleDateFormat
import java.util.TimeZone
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(KafkaObjectModel)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  private[this] def dateFormat = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("UTC-0"))
    format
  }
  mapper.setDateFormat(dateFormat)

  def toJson(value: Any): String = mapper.writeValueAsString(value)
  def toJsonBytes(value: Any): Array[Byte] = mapper.writeValueAsBytes(value)
  def fromJson[T](str: String)(implicit m: Manifest[T]): T = mapper.readValue[T](str)
  def fromJson[T](bytes: Array[Byte])(implicit m: Manifest[T]) = mapper.readValue[T](bytes)
}
