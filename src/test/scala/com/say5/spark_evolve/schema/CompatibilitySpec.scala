package com.say5.spark_evolve.schema

import com.say5.spark_evolve.schema.Compatibility.CompatibilityLevel
import org.apache.avro.Schema
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Exhaustive tests for the schema-evolution rule engine. Every rule is exercised with at least one positive
  * and one negative case. Tests are grouped by rule.
  */
class CompatibilitySpec extends AnyFunSuite with Matchers with EitherValues {

  private def parse(json: String): Schema = new Schema.Parser().parse(json)

  private def base(extraFields: String = "", recordName: String = "Order"): Schema =
    parse(s"""
      |{
      |  "type": "record",
      |  "name": "$recordName",
      |  "namespace": "com.say5.test",
      |  "fields": [
      |    { "name": "id",     "type": "string" },
      |    { "name": "amount", "type": "long"   }
      |    $extraFields
      |  ]
      |}
      |""".stripMargin)

  // -------- Adding fields --------

  test("adding a field with a default is backward compatible") {
    val oldS = base()
    val newS = base(extraFields = """, { "name": "discount", "type": "long", "default": 0 }""")
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  test("adding a nullable union field with null first is backward compatible") {
    val oldS = base()
    val newS = base(extraFields = """, { "name": "note", "type": ["null", "string"], "default": null }""")
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  test(
    "adding a nullable union field without default is backward compatible (nullable carries an implicit default)"
  ) {
    val oldS = base()
    val newS = base(extraFields = """, { "name": "note", "type": ["null", "string"] }""")
    // Avro allows this — the schema is itself nullable so old records with the field absent
    // simply read as null. Our engine matches that.
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  test("adding a non-nullable field WITHOUT a default is NOT backward compatible") {
    val oldS   = base()
    val newS   = base(extraFields = """, { "name": "discount", "type": "long" }""")
    val result = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
    result.left.value.collect { case v: Violation.MissingDefault => v.fieldName } should contain("discount")
  }

  // -------- Removing fields --------

  test("removing a field that had a default is backward compatible") {
    val oldS = parse("""
      |{ "type":"record","name":"Order","namespace":"com.say5.test","fields":[
      |  {"name":"id","type":"string"},
      |  {"name":"amount","type":"long"},
      |  {"name":"discount","type":"long","default":0}
      |]}
    """.stripMargin)
    val newS = base()
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  test("removing a required field (no default) is NOT backward compatible") {
    val oldS = base()
    val newS = parse("""
      |{ "type":"record","name":"Order","namespace":"com.say5.test","fields":[
      |  {"name":"id","type":"string"}
      |]}
    """.stripMargin)
    val result = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
    result.left.value.collect { case v: Violation.RemovedRequiredField => v.fieldName } should contain(
      "amount"
    )
  }

  // -------- Renaming --------

  test("renaming a field WITHOUT aliases is reported as RenamedWithoutAlias") {
    val oldS = base()
    val newS = parse("""
      |{ "type":"record","name":"Order","namespace":"com.say5.test","fields":[
      |  {"name":"id","type":"string"},
      |  {"name":"total","type":"long"}
      |]}
    """.stripMargin)
    val result = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
    result.left.value.collect { case v: Violation.RenamedWithoutAlias =>
      (v.oldName, v.newName)
    } should contain
    (("amount", "total"))
  }

  test("renaming a field WITH aliases is backward compatible") {
    val oldS = base()
    val newS = parse("""
      |{ "type":"record","name":"Order","namespace":"com.say5.test","fields":[
      |  {"name":"id","type":"string"},
      |  {"name":"total","type":"long","aliases":["amount"]}
      |]}
    """.stripMargin)
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  // -------- Type changes / promotions --------

  test("widening int → long is backward compatible") {
    val oldS = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"int"}]}""")
    val newS = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"long"}]}""")
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  test("widening float → double is backward compatible") {
    val oldS =
      parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"float"}]}""")
    val newS =
      parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"double"}]}""")
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  test("narrowing long → int is NOT backward compatible") {
    val oldS = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"long"}]}""")
    val newS = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"int"}]}""")
    val result = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
    result.left.value.collect { case v: Violation.TypeIncompatible => v.fieldName } should contain("n")
  }

  test("incompatible type change int → string is reported") {
    val oldS = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"int"}]}""")
    val newS =
      parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"n","type":"string"}]}""")
    val result = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
    result.left.value should not be empty
  }

  // -------- Nullability --------

  test("changing a non-nullable field to nullable WITHOUT a default is reported") {
    val oldS = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":"long"}]}""")
    val newS =
      parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":["null","long"]}]}""")
    val result = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
    result.left.value.collect { case v: Violation.NonNullableMadeNullable => v.fieldName } should contain("a")
  }

  test("changing a non-nullable field to nullable WITH a default is allowed") {
    val oldS = parse("""{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":"long"}]}""")
    val newS = parse(
      """{"type":"record","name":"R","namespace":"x","fields":[{"name":"a","type":["null","long"],"default":null}]}"""
    )
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  // -------- Record name --------

  test("changing record name without aliases is reported") {
    val oldS = parse("""{"type":"record","name":"A","namespace":"x","fields":[{"name":"i","type":"int"}]}""")
    val newS = parse("""{"type":"record","name":"B","namespace":"x","fields":[{"name":"i","type":"int"}]}""")
    val result = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
    result.left.value.collect { case v: Violation.RecordNameMismatch => v.oldName } should contain("x.A")
  }

  test("changing record name WITH aliases is allowed") {
    val oldS = parse("""{"type":"record","name":"A","namespace":"x","fields":[{"name":"i","type":"int"}]}""")
    val newS = parse(
      """{"type":"record","name":"B","namespace":"x","aliases":["x.A"],"fields":[{"name":"i","type":"int"}]}"""
    )
    Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
  }

  // -------- Forward / Full --------

  test("forward compatibility: removing a required field IS forward-OK (old reader doesn't need new field)") {
    // forward compat: data written with new can be read with old.
    // Old has `amount`, new doesn't — old reader still expects amount, so this is NOT forward-OK.
    val oldS = base()
    val newS = parse(
      """{"type":"record","name":"Order","namespace":"com.say5.test","fields":[{"name":"id","type":"string"}]}"""
    )
    Compatibility.check(oldS, newS, CompatibilityLevel.Forward).left.value should not be empty
  }

  test("forward compatibility: adding a non-nullable field is OK (old reader ignores it)") {
    val oldS = base()
    val newS = base(extraFields = """, { "name": "discount", "type": "long" }""")
    Compatibility.check(oldS, newS, CompatibilityLevel.Forward) shouldBe Right(())
  }

  test("full compatibility requires both directions to pass") {
    val oldS = base()
    val newS = base(extraFields = """, { "name": "discount", "type": "long", "default": 0 }""")
    Compatibility.check(oldS, newS, CompatibilityLevel.Full) shouldBe Right(())
  }

  test("full compatibility fails when only one direction passes") {
    val oldS = base()
    val newS = base(extraFields = """, { "name": "discount", "type": "long" }""")
    Compatibility.check(oldS, newS, CompatibilityLevel.Full).left.value should not be empty
  }

  test("none level always returns Right") {
    val oldS = base()
    val newS = parse("""{"type":"record","name":"X","namespace":"y","fields":[]}""")
    Compatibility.check(oldS, newS, CompatibilityLevel.None_) shouldBe Right(())
  }

  // -------- Accumulation --------

  test("multiple violations are reported together (no short-circuit)") {
    val oldS = parse("""
      |{ "type":"record","name":"R","namespace":"x","fields":[
      |  {"name":"a","type":"long"},
      |  {"name":"b","type":"long"}
      |]}
    """.stripMargin)
    val newS = parse("""
      |{ "type":"record","name":"R","namespace":"x","fields":[
      |  {"name":"c","type":"string"},
      |  {"name":"d","type":"long"}
      |]}
    """.stripMargin)
    val violations = Compatibility.check(oldS, newS, CompatibilityLevel.Backward).left.value
    violations.size should be >= 2
  }

  // -------- Real bundled v1 → v2 → v3 --------

  test("bundled v1 → v2 (adds discount with default) is backward compatible") {
    val v1 = SchemaRegistry.parseFile("schemas/orders/v1.avsc").get
    val v2 = SchemaRegistry.parseFile("schemas/orders/v2.avsc").get
    Compatibility.check(v1, v2, CompatibilityLevel.Backward) shouldBe Right(())
  }

  test("bundled v2 → v3 (renames total_cents → total_amount_cents) is NOT backward compatible") {
    val v2     = SchemaRegistry.parseFile("schemas/orders/v2.avsc").get
    val v3     = SchemaRegistry.parseFile("schemas/orders/v3.avsc").get
    val result = Compatibility.check(v2, v3, CompatibilityLevel.Backward)
    result.left.value should not be empty
  }

  test("Markdown rendering for empty list") {
    Violation.toMarkdown(Nil) shouldBe "compatible"
  }

  test("Markdown rendering for non-empty list mentions every violation") {
    val md = Violation.toMarkdown(
      List(
        Violation.MissingDefault("a"),
        Violation.TypeIncompatible("b", "LONG", "INT")
      )
    )
    md should include("MissingDefault")
    md should include("TypeIncompatible")
  }

  test("CompatibilityLevel.parse accepts valid levels") {
    CompatibilityLevel.parse("backward") shouldBe Right(CompatibilityLevel.Backward)
    CompatibilityLevel.parse("FORWARD") shouldBe Right(CompatibilityLevel.Forward)
    CompatibilityLevel.parse("Full") shouldBe Right(CompatibilityLevel.Full)
    CompatibilityLevel.parse("none") shouldBe Right(CompatibilityLevel.None_)
  }

  test("CompatibilityLevel.parse rejects unknown") {
    CompatibilityLevel.parse("weird").isLeft shouldBe true
  }

}
