package ly.stealth.mesos.kafka

class Constraint(s: String) {
  var _condition: Constraint.Condition = null
  def condition: Constraint.Condition = _condition

  this.parse
  private def parse: Unit = {
    if (!s.startsWith("#"))
      _condition = new Constraint.Pattern(s)
    else {
      if (s == "#same") _condition = new Constraint.Same()
      else if (s == "#unique") _condition = new Constraint.Unique()
      else if (s.startsWith("#regex:")) _condition = new Constraint.Regex(s.substring("#regex:".length))
      else if (s.startsWith("#group")) {
        val arg = s.substring("#group".length)
        val count: Int = if (arg.startsWith(":")) Integer.valueOf(arg.substring(1)) else 1
        _condition = new Constraint.Group(count)
      }
      else throw new IllegalArgumentException("unsupported condition " + s)
    }
  }

  def matches(value: String, values: Array[String] = Array()): Boolean = condition.matches(value, values)

  override def toString: String = s
}

object Constraint {
  abstract class Condition {
    def matches(value: String, values: Array[String]): Boolean
  }

  class Pattern(s: String) extends Condition {
    var _negated: Boolean = false
    var _pattern: java.util.regex.Pattern = null

    def negated: Boolean = _negated
    def pattern: java.util.regex.Pattern = _pattern

    this.parse
    private def parse: Unit = {
      var i = 0

      if (s.length > i && s.charAt(i) == '!') {
        _negated = true
        i += 1
      }

      _pattern = java.util.regex.Pattern.compile(regex(s.substring(i)))
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
          throw new IllegalArgumentException(s"unescaped $c inside expression")
        else {
          token += c
          escaped = false
        }
      }

      if (escaped) throw new IllegalArgumentException("unterminated escaping")

      if (token != "") regex += java.util.regex.Pattern.quote(token)
      regex + "$"
    }


    def matches(value: String, values: Array[String] = Array()): Boolean = _pattern.matcher(value).find()

    override def toString: String = s
  }

  class Same extends Condition {
    def matches(value: String, values: Array[String]): Boolean = values.isEmpty || values(0) == value
    override def toString: String = "#same"
  }

  class Unique extends Condition {
    def matches(value: String, values: Array[String]): Boolean = !values.contains(value)
    override def toString: String = "#unique"
  }
  
  class Regex(s: String) extends Condition {
    val _pattern = java.util.regex.Pattern.compile("^" + s + "$")
    def pattern = _pattern
    
    def matches(value: String, values: Array[String] = Array()): Boolean = _pattern.matcher(value).find()
    override def toString: String = "#regex:" + s
  }

  class Group(groups: Int) extends Condition {
    def matches(value: String, values: Array[String]): Boolean = {
      val counts: Map[String, Int] = values.groupBy("" + _).mapValues(_.size)
      val minCount = counts.values.reduceOption(_ min _).getOrElse(0)

      // implementation approach taken from
      // https://github.com/mesosphere/marathon/blob/master/src/main/scala/mesosphere/mesos/Constraints.scala

      // Return true if any of these are also true:
      // a) this offer matches the smallest grouping when there are >= minimum groupings
      // b) the constraint value from the offer is not yet in the grouping
      val count = counts.getOrElse(value, 0)
      count == 0 || (counts.size >= groups && count == minCount)
    }
  }
}
