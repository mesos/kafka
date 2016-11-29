package ly.stealth.mesos.kafka.http.api

import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.StatusType
import net.elodina.mesos.util.{Constraint, Strings}
import scala.collection.JavaConverters._
import scala.collection.mutable

class StringMap(value: String) extends mutable.HashMap[String, String] {
  this ++= Strings.parseMap(value).asScala
}

class ConstraintMap(value: String) extends mutable.HashMap[String, Constraint] {
  this ++= Strings.parseMap(value).asScala.mapValues(new Constraint(_))
}

object Status {
  class BadRequest(reason: String) extends StatusType {
    override def getStatusCode: Int = Response.Status.BAD_REQUEST.getStatusCode
    override def getReasonPhrase: String = reason
    override def getFamily: Response.Status.Family = Response.Status.BAD_REQUEST.getFamily
  }
  object BadRequest {
    def apply(reason: String) = Response.status(new BadRequest(reason)).build()
  }
}