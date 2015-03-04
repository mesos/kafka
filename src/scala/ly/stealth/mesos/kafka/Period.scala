package ly.stealth.mesos.kafka

import java.text.{ParseException, DecimalFormat}

class Period(_value: String) {
  private val value: String = _value
  private var ms: Long = 0L

  try {
    ms = new DecimalFormat().parse(value).longValue

    if (value.endsWith("ms")) ms *= 1
    else if (value.endsWith("s")) ms *= 1000
    else if (value.endsWith("m")) ms *= 60 * 1000
    else if (value.endsWith("h")) ms *= 60 * 60 * 1000
    else if (value.endsWith("d")) ms *= 24 * 60 * 60 * 1000
    else if (value.endsWith("w")) ms *= 7 * 24 * 60 * 60 * 1000
    else throw new IllegalArgumentException("invalid period " + value)
  } catch {
    case e: ParseException => throw new IllegalArgumentException("invalid period " + value)
  }

  def getValue: String = value
  def toMs: Long = ms

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Period]) return false
    obj.asInstanceOf[Period].ms == ms
  }

  override def hashCode: Int = toMs.asInstanceOf[Int]
  override def toString: String = value
}