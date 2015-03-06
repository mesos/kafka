package ly.stealth.mesos.kafka

import java.text.{ParseException, DecimalFormat}

class Period(s: String) {
  private var value: Long = 0
  private var unit: String = null
  private var ms: Long = 0

  try {
    var unitIdx = s.length - 1
    if (s.endsWith("ms")) unitIdx -= 1

    value = java.lang.Long.valueOf(s.substring(0, unitIdx))
    unit = s.substring(unitIdx)

    ms = value
    if (unit == "ms") ms *= 1
    else if (unit == "s") ms *= 1000
    else if (unit == "m") ms *= 60 * 1000
    else if (unit == "h") ms *= 60 * 60 * 1000
    else if (unit == "d") ms *= 24 * 60 * 60 * 1000
    else throw new IllegalArgumentException("Invalid period " + value)
  } catch {
    case e: ParseException => throw new IllegalArgumentException("Invalid period " + value)
  }

  def getValue: Long = value
  def getUnit: String = unit
  def toMs: Long = ms

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Period]) return false
    obj.asInstanceOf[Period].ms == ms
  }

  override def hashCode: Int = ms.asInstanceOf[Int]

  override def toString: String = value + unit
}