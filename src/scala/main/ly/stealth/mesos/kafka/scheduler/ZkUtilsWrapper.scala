package ly.stealth.mesos.kafka.scheduler

import ly.stealth.mesos.kafka.Config
import ly.stealth.mesos.kafka.interface.{AdminUtilsProxy, ZkUtilsProxy}
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer

object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

abstract class LazyWrapper[I](fact: (String) => I) {
  private def getInstance() = {
    fact(Config.zk)
  }

  private var instance: Option[I] = None

  def apply(): I = instance match {
    case Some(i) => i
    case None =>
      instance = Some(getInstance())
      instance.get
  }
  def reset() = instance = None
}


object ZkUtilsWrapper extends LazyWrapper(ZkUtilsProxy.apply) {}

object AdminUtilsWrapper extends LazyWrapper(AdminUtilsProxy.apply) { }