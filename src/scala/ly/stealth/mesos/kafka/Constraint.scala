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


    def matches(value: String, values: Array[String]): Boolean = _pattern.matcher(value).find()

    override def toString: String = s
  }

  class Same extends Condition {
    def matches(value: String, values: Array[String]): Boolean = values.isEmpty || values(0) == value
  }

  class Unique extends Condition {
    def matches(value: String, values: Array[String]): Boolean = !values.contains(value)
  }
}
