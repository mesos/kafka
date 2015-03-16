package ly.stealth.mesos.kafka

import java.util.regex.Pattern
import java.util
import scala.util.parsing.json.JSON
import java.io.{IOException, OutputStream, InputStream}

object Util {
  def parseMap(s: String, entrySep: String = ",", valueSep: String = "="): util.LinkedHashMap[String, String] = {
    val result = new util.LinkedHashMap[String, String]()
    if (s == null) return result

    for (entry <- s.split(entrySep))
      if (entry.trim() != "") {
        val pair = entry.split(valueSep)
        if (pair.length == 2) result.put(pair(0).trim(), pair(1).trim())
        else throw new IllegalArgumentException(s)
      }

    result
  }

  def parseJson(json: String): Map[String, Object] = {
    val node: Map[String, Object] = JSON.parseFull(json).getOrElse(null).asInstanceOf[Map[String, Object]]
    if (node == null) throw new IllegalArgumentException("Failed to parse json: " + json)
    node
  }

  def copyAndClose(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](16 * 1024)
    var actuallyRead = 0

    try {
      while (actuallyRead != -1) {
        actuallyRead = in.read(buffer)
        if (actuallyRead != -1) out.write(buffer, 0, actuallyRead)
      }
    } finally {
      try { in.close() }
      catch { case ignore: IOException => }

      try { out.close() }
      catch { case ignore: IOException => }
    }
  }

  class Period(s: String) {
    private var _value: Long = 0
    private var _unit: String = null
    private var _ms: Long = 0

    parse()
    private def parse() {
      if (s.isEmpty) throw new IllegalArgumentException(s)

      var unitIdx = s.length - 1
      if (s.endsWith("ms")) unitIdx -= 1

      try { _value = java.lang.Long.valueOf(s.substring(0, unitIdx)) }
      catch { case e: IllegalArgumentException => throw new IllegalArgumentException(s) }
      _unit = s.substring(unitIdx)

      _ms = value
      if (_unit == "ms") _ms *= 1
      else if (_unit == "s") _ms *= 1000
      else if (_unit == "m") _ms *= 60 * 1000
      else if (_unit == "h") _ms *= 60 * 60 * 1000
      else if (_unit == "d") _ms *= 24 * 60 * 60 * 1000
      else throw new IllegalArgumentException(s)
    }

    def value: Long = _value
    def unit: String = _unit
    def ms: Long = _ms

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Period]) return false
      obj.asInstanceOf[Period]._ms == _ms
    }

    override def hashCode: Int = _ms.asInstanceOf[Int]
    override def toString: String = _value + _unit
  }


  class Wildcard(s: String) {
    private val _value: String = s
    private var _pattern: Pattern = null
    compilePattern()

    private def compilePattern() {
      var regex: String = "^"
      var token: String = ""

      for (c <- _value.toCharArray) {
        if (c == '*' || c == '?') {
          regex += Pattern.quote(token)
          token = ""
          regex += (if (c == '*') ".*" else ".")
        } else
          token += c
      }

      if (token != "") regex += Pattern.quote(token)
      regex += "$"

      _pattern = Pattern.compile(regex)
    }

    def matches(value: String): Boolean = _pattern.matcher(value).find()
    def value: String = _value

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Wildcard]) return false
      obj.asInstanceOf[Wildcard]._value == _value
    }

    override def hashCode: Int = _value.hashCode
    override def toString: String = _value
  }
}
