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
    else if (_value == "unique") _condition = new Constraint.Unique()
    else if (_value.startsWith("cluster")) {
      val tail = _value.substring("cluster".length)
      val value = if (tail.startsWith(":")) tail.substring(1) else null
      _condition = new Constraint.Cluster(value)
    } else if (_value.startsWith("groupBy")) {
      val tail = _value.substring("groupBy".length)

      var groups: Int = 1
      if (tail.startsWith(":"))
        try { groups = Integer.valueOf(tail.substring(1)) }
        catch { case e: NumberFormatException => throw new IllegalArgumentException(s"invalid condition ${_value}") }

      _condition = new Constraint.GroupBy(groups)
    }
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

  class Unique extends Condition {
    def matches(value: String, values: Array[String]): Boolean = !values.contains(value)
    override def toString: String = "unique"
  }

  class Cluster(_value: String = null) extends Condition {
    def value: String = _value

    def matches(value: String, values: Array[String]): Boolean =
      if (_value != null) value == _value else values.isEmpty || values(0) == value
    override def toString: String = "cluster" + (if (_value != null) ":" + _value else "")
  }

  class GroupBy(_groups: Int = 1) extends Condition {
    def groups: Int = _groups

    def matches(value: String, values: Array[String]): Boolean = {
      val counts: Map[String, Int] = values.groupBy("" + _).mapValues(_.size)
      if (counts.size < _groups) return !counts.contains(value)

      val minCount = counts.values.reduceOption(_ min _).getOrElse(0)
      counts.getOrElse(value, 0) == minCount
    }

    override def toString: String = "groupBy" + (if (_groups > 1) ":" + _groups else "")
  }
}
