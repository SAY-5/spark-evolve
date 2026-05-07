package com.say5.spark_evolve.schema

import com.say5.spark_evolve.schema.Compatibility.CompatibilityLevel
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaRegistrySpec extends AnyFunSuite with Matchers {

  private def parse(json: String): Schema = new Schema.Parser().parse(json)

  test("empty registry has no subjects") {
    val r = SchemaRegistry.empty()
    r.subjects shouldBe empty
    r.latest("orders") shouldBe None
  }

  test("register persists v1 to disk and bumps version") {
    val r = SchemaRegistry.empty()
    val s = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":"long"}]}""")
    r.register("orders", s, CompatibilityLevel.Backward) shouldBe Right(1)
    r.latest("orders") shouldBe Some(s)
  }

  test("register rejects an incompatible second version") {
    val r  = SchemaRegistry.empty()
    val s1 = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":"long"}]}""")
    val s2 = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":"string"}]}""")
    r.register("orders", s1, CompatibilityLevel.Backward) shouldBe Right(1)
    val res = r.register("orders", s2, CompatibilityLevel.Backward)
    res.isLeft shouldBe true
    r.versions("orders") should have size 1
  }

  test("fromDirectory loads bundled schemas") {
    val r = SchemaRegistry.fromDirectory(java.nio.file.Paths.get("schemas")).get
    r.subjects should contain("orders")
    r.versions("orders") should have size 3
  }

  test("registry round-trips through disk") {
    // empty() roots at a fresh temp dir; we read it back via fromDirectory.
    val r1 = SchemaRegistry.empty()
    val s  = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":"long"}]}""")
    r1.register("orders", s, CompatibilityLevel.Backward) shouldBe Right(1)
    // Walk the temp dir from the file we just wrote.
    val rootField = classOf[SchemaRegistry].getDeclaredField("rootDir")
    rootField.setAccessible(true)
    val root = rootField.get(r1).asInstanceOf[java.nio.file.Path]
    val r2   = SchemaRegistry.fromDirectory(root).get
    r2.latest("orders").map(_.getFullName) shouldBe Some("x.R")
  }
}
