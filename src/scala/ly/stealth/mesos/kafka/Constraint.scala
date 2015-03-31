/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.kafka

import java.util.regex.PatternSyntaxException

class Constraint(_value: String) {
  var _condition: Constraint.Condition = null

  def value: String = _value
  def condition: Constraint.Condition = _condition

  this.parse
  private def parse: Unit = {
    if (_value.startsWith("like:")) _condition = new Constraint.Like(_value.substring("like:".length))
    else if (_value.startsWith("unlike:")) _condition = new Constraint.Like(_value.substring("unlike:".length), _negated = true)
    else if (_value.startsWith("same")) {
      val tail = _value.substring("same".length)

      var count: Int = 1
      if (tail.startsWith(":"))
        try { count = Integer.valueOf(tail.substring(1)) }
        catch { case e: NumberFormatException => throw new IllegalArgumentException(s"invalid condition ${_value}") }

      _condition = new Constraint.Same(count)
    }
    else if (_value == "unique") _condition = new Constraint.Unique()
    else throw new IllegalArgumentException("unsupported condition " + _value)
  }

  def matches(value: String, values: Array[String] = Array()): Boolean = condition.matches(value, values)

  override def hashCode(): Int = _value.hashCode
  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Constraint]) return false
    value == obj.asInstanceOf[Constraint].value
  }

  override def toString: String = _value
}

object Constraint {
  abstract class Condition {
    def matches(value: String, values: Array[String]): Boolean
  }

  class Like(_regex: String, _negated: Boolean = false) extends Condition {
    var _pattern: java.util.regex.Pattern = null
    try { _pattern = java.util.regex.Pattern.compile("^" + _regex + "$") }
    catch { case e: PatternSyntaxException => throw new IllegalArgumentException(s"invalid $name: $e.getMessage") }

    def regex = _regex
    def negated = _negated
    def pattern = _pattern

    private def name: String = if (!negated) "like" else "unlike"

    def matches(value: String, values: Array[String] = Array()): Boolean = _negated ^ _pattern.matcher(value).find()
    override def toString: String = s"$name:$regex"
  }

  class Same(_variants: Int) extends Condition {
    def variants: Int = _variants

    def matches(value: String, values: Array[String]): Boolean = {
      val counts: Map[String, Int] = values.groupBy("" + _).mapValues(_.size)
      if (counts.size < _variants) return !counts.contains(value)

      val minCount = counts.values.reduceOption(_ min _).getOrElse(0)
      counts.getOrElse(value, 0) == minCount
    }

    override def toString: String = "same" + (if (_variants > 1) ":" + _variants else "")
  }

  class Unique extends Condition {
    def matches(value: String, values: Array[String]): Boolean = !values.contains(value)
    override def toString: String = "unique"
  }
}
