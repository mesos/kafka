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
    if (!_value.startsWith("#"))
      _condition = new Constraint.Pattern(_value)
    else {
      if (_value.startsWith("#same")) {
        val tail = _value.substring("#same".length)

        var count: Int = 1
        if (tail.startsWith(":"))
          try { count = Integer.valueOf(tail.substring(1)) }
          catch { case e: NumberFormatException => throw new IllegalArgumentException(s"invalid condition ${_value}") }

        _condition = new Constraint.Same(count)
      }
      else if (_value == "#unique") _condition = new Constraint.Unique()
      else if (_value.startsWith("#regex:")) _condition = new Constraint.Regex(_value.substring("#regex:".length))
      else throw new IllegalArgumentException("unsupported condition " + _value)
    }
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

  class Pattern(_value: String) extends Condition {
    var _negated: Boolean = false
    var _pattern: java.util.regex.Pattern = null

    def value: String = _value
    def negated: Boolean = _negated
    def pattern: java.util.regex.Pattern = _pattern

    this.parse
    private def parse: Unit = {
      var i = 0

      if (_value.length > i && _value.charAt(i) == '!') {
        _negated = true
        i += 1
      }

      _pattern = java.util.regex.Pattern.compile(regex(_value.substring(i)))
    }

    private[kafka] def regex(s: String): String = {
      var regex: String = "^"
      var token: String = ""
      var escaped: Boolean = false

      for (c <- s.toCharArray) {
        if (c == '\\' && !escaped)
          escaped = true
        else if ((c == '*' || c == '?') && !escaped) {
          if (token != "") {
            regex += java.util.regex.Pattern.quote(token)
            token = ""
          }
          regex += (if (c == '*') ".*" else ".")
        }
        else if ((c == '!' || c == '#') && !escaped)
          throw new IllegalArgumentException(s"unescaped $c in pattern")
        else {
          token += c
          escaped = false
        }
      }

      if (escaped) throw new IllegalArgumentException("unterminated \\ in pattern")

      if (token != "") regex += java.util.regex.Pattern.quote(token)
      regex + "$"
    }


    def matches(value: String, values: Array[String] = Array()): Boolean = _pattern.matcher(value).find() ^ negated
    override def toString: String = _value
  }

  class Same(_variants: Int) extends Condition {
    def variants: Int = _variants

    def matches(value: String, values: Array[String]): Boolean = {
      if (values.length < _variants) return !values.contains(value)

      val counts: Map[String, Int] = values.groupBy("" + _).mapValues(_.size)
      val minCount = counts.values.reduceOption(_ min _).getOrElse(0)

      counts.getOrElse(value, 0) == minCount
    }

    override def toString: String = "#same" + (if (_variants > 1) ":" + _variants else "")
  }

  class Unique extends Condition {
    def matches(value: String, values: Array[String]): Boolean = !values.contains(value)
    override def toString: String = "#unique"
  }
  
  class Regex(_value: String) extends Condition {
    var _pattern: java.util.regex.Pattern = null
    try { _pattern = java.util.regex.Pattern.compile("^" + _value + "$") }
    catch { case e: PatternSyntaxException => throw new IllegalArgumentException("invalid #regex: " + e.getMessage) }

    def value = _value
    def pattern = _pattern
    
    def matches(value: String, values: Array[String] = Array()): Boolean = _pattern.matcher(value).find()
    override def toString: String = "#regex:" + _value
  }
}
