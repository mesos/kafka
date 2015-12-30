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

import org.junit.Test
import org.junit.Assert._
import ly.stealth.mesos.kafka.Util.{BindAddress, Period, Range, Version}
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.util

class UtilTest {
  @Test
  def parseMap {
    var map = Util.parseMap("a=1,b=2")
    assertEquals(2, map.size())
    assertEquals("1", map.get("a"))
    assertEquals("2", map.get("b"))

    // missing pair
    try { map = Util.parseMap("a=1,,b=2"); fail() }
    catch { case e: IllegalArgumentException => }

    // null value
    map = Util.parseMap("a=1,b,c=3")
    assertEquals(3, map.size())
    assertNull(map.get("b"))

    try { Util.parseMap("a=1,b,c=3", nullValues = false) }
    catch { case e: IllegalArgumentException => }

    // escaping
    map = Util.parseMap("a=\\,,b=\\=,c=\\\\")
    assertEquals(3, map.size())
    assertEquals(",", map.get("a"))
    assertEquals("=", map.get("b"))
    assertEquals("\\", map.get("c"))

    // open escaping
    try { Util.parseMap("a=\\"); fail() }
    catch { case e: IllegalArgumentException => }

    // null
    assertTrue(Util.parseMap(null).isEmpty)
  }

  @Test
  def formatMap {
    val map = new util.LinkedHashMap[String, String]()
    map.put("a", "1")
    map.put("b", "2")
    assertEquals("a=1,b=2", Util.formatMap(map))

    // null value
    map.put("b", null)
    assertEquals("a=1,b", Util.formatMap(map))

    // escaping
    map.put("a", ",")
    map.put("b", "=")
    map.put("c", "\\")
    assertEquals("a=\\,,b=\\=,c=\\\\", Util.formatMap(map))
  }

  @Test
  def parseJson {
    val node: Map[String, Object] = Util.parseJson("{\"a\":\"1\", \"b\":\"2\"}")
    assertEquals(2, node.size)
    assertEquals("1", node("a").asInstanceOf[String])
    assertEquals("2", node("b").asInstanceOf[String])
  }

  @Test
  def copyAndClose {
    val data = new Array[Byte](16 * 1024)
    for (i <- 0 until data.length) data(i) = i.toByte

    var inClosed = false
    var outClosed = false

    val in = new ByteArrayInputStream(data) {
      override def close(): Unit = super.close(); inClosed = true
    }
    val out = new ByteArrayOutputStream() {
      override def close(): Unit = super.close(); outClosed = true
    }

    Util.copyAndClose(in, out)
    assertTrue(util.Arrays.equals(data, out.toByteArray))
    assertTrue(inClosed)
    assertTrue(outClosed)
  }

  // Period
  @Test
  def Period_init() {
    new Period("1m")

    // empty
    try {
      new Period("")
      fail()
    } catch { case e: IllegalArgumentException => }

    // zero without units
    new Period("0")

    // no units
    try {
      new Period("1")
      fail()
    } catch { case e: IllegalArgumentException => }

    // no value
    try {
      new Period("ms")
      fail()
    } catch { case e: IllegalArgumentException => }

    // wrong unit
    try {
      new Period("1k")
      fail()
    } catch { case e: IllegalArgumentException => }

    // non-integer value
    try {
      new Period("0.5m")
      fail()
    } catch { case e: IllegalArgumentException => }

    // invalid value
    try {
      new Period("Xh")
      fail()
    } catch { case e: IllegalArgumentException => }
  }

  @Test
  def Period_ms {
    assertEquals(0, new Period("0").ms)
    assertEquals(1, new Period("1ms").ms)
    assertEquals(10, new Period("10ms").ms)

    val s: Int = 1000
    assertEquals(s, new Period("1s").ms)
    assertEquals(10 * s, new Period("10s").ms)

    val m: Int = 60 * s
    assertEquals(m, new Period("1m").ms)
    assertEquals(10 * m, new Period("10m").ms)

    val h: Int = 60 * m
    assertEquals(h, new Period("1h").ms)
    assertEquals(10 * h, new Period("10h").ms)

    val d: Int = 24 * h
    assertEquals(d, new Period("1d").ms)
    assertEquals(10 * d, new Period("10d").ms)
  }

  @Test
  def Period_value {
    assertEquals(0, new Period("0").value)
    assertEquals(10, new Period("10ms").value)
    assertEquals(50, new Period("50h").value)
    assertEquals(20, new Period("20d").value)
  }

  @Test
  def Period_unit {
    assertEquals("ms", new Period("0").unit)
    assertEquals("ms", new Period("10ms").unit)
    assertEquals("h", new Period("50h").unit)
    assertEquals("d", new Period("20d").unit)
  }

  @Test
  def Period_toString {
    assertEquals("10ms", "" + new Period("10ms"))
    assertEquals("5h", "" + new Period("5h"))
  }

  // Range
  @Test
  def Range_init {
    new Range("30")
    new Range("30..31")
    new Range(30)
    new Range(30, 31)

    // empty
    try { new Range(""); fail() }
    catch { case e: IllegalArgumentException => }

    // non int
    try { new Range("abc"); fail() }
    catch { case e: IllegalArgumentException => }

    // non int first
    try { new Range("abc..30"); fail() }
    catch { case e: IllegalArgumentException => }

    // non int second
    try { new Range("30..abc"); fail() }
    catch { case e: IllegalArgumentException => }

    // inverted range
    try { new Range("10..0"); fail() }
    catch { case e: IllegalArgumentException => }
  }

  @Test
  def Range_start_end {
    assertEquals(0, new Range("0").start)
    assertEquals(0, new Range("0..10").start)
    assertEquals(10, new Range("0..10").end)
  }

  @Test
  def Range_overlap {
    // no overlap
    assertNull(new Range(0, 10).overlap(new Range(20, 30)))
    assertNull(new Range(20, 30).overlap(new Range(0, 10)))
    assertNull(new Range(0).overlap(new Range(1)))

    // partial
    assertEquals(new Range(5, 10), new Range(0, 10).overlap(new Range(5, 15)))
    assertEquals(new Range(5, 10), new Range(5, 15).overlap(new Range(0, 10)))

    // includes
    assertEquals(new Range(2, 3), new Range(0, 10).overlap(new Range(2, 3)))
    assertEquals(new Range(2, 3), new Range(2, 3).overlap(new Range(0, 10)))
    assertEquals(new Range(5), new Range(0, 10).overlap(new Range(5)))

    // last point
    assertEquals(new Range(0), new Range(0, 10).overlap(new Range(0)))
    assertEquals(new Range(10), new Range(0, 10).overlap(new Range(10)))
    assertEquals(new Range(0), new Range(0).overlap(new Range(0)))
  }

  @Test
  def Range_toString {
    assertEquals("0", "" + new Range("0"))
    assertEquals("0..10", "" + new Range("0..10"))
    assertEquals("0", "" + new Range("0..0"))
  }

  @Test
  def Version_init {
    assertEquals(List(), new Version().asList)
    assertEquals(List(1,0), new Version(1,0).asList)
    assertEquals(List(1,2,3,4), new Version("1.2.3.4").asList)

    try { new Version(" "); fail() }
    catch { case e: IllegalArgumentException => }

    try { new Version("."); fail() }
    catch { case e: IllegalArgumentException => }

    try { new Version("a"); fail() }
    catch { case e: IllegalArgumentException => }
  }

  @Test
  def Version_compareTo {
    assertEquals(0, new Version().compareTo(new Version()))
    assertEquals(0, new Version(0).compareTo(new Version(0)))

    assertTrue(new Version(0).compareTo(new Version(1)) < 0)
    assertTrue(new Version(0).compareTo(new Version(0, 0)) < 0)

    assertTrue(new Version(0, 9, 0, 0).compareTo(new Version(0, 8, 2, 0)) > 0)
  }

  // BindAddress
  @Test
  def BindAddress_init {
    new BindAddress("broker0")
    new BindAddress("192.168.*")
    new BindAddress("if:eth1")

    // unknown source
    try { new BindAddress("unknown:value"); fail() }
    catch { case e: IllegalArgumentException => }
  }

  @Test
  def BindAddress_resolve {
    // address without mask
    assertEquals("host", new BindAddress("host").resolve())

    // address with mask
    assertEquals("127.0.0.1", new BindAddress("127.0.0.*").resolve())

    // unresolvable
    try { new BindAddress("255.255.*").resolve(); fail() }
    catch { case e: IllegalStateException => }
  }
}
