package ly.stealth.mesos.kafka

import java.text.{ParseException, DecimalFormat}

class Period(s: String) {
  private var _value: Long = 0
  private var _unit: String = null
  private var _ms: Long = 0

  parse()
  def parse() {
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