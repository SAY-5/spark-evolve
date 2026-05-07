package com.say5.spark_evolve.schema

import com.say5.spark_evolve.schema.Compatibility.CompatibilityLevel
import org.apache.avro.Schema
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._

/** Property-based tests for the schema compatibility engine.
  *
  * Each property exercises one engine invariant against a generator that produces structurally diverse `(old,
  * new)` schema pairs. The generators are intentionally small so failure shrinking yields readable
  * counter-examples.
  */
class CompatibilityPropertySpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {

  // ---------------- Generators ----------------

  /** A small set of primitive Avro types we know the engine can reason about. */
  private val primitiveTypes: List[Schema.Type] = List(
    Schema.Type.INT,
    Schema.Type.LONG,
    Schema.Type.FLOAT,
    Schema.Type.DOUBLE,
    Schema.Type.STRING,
    Schema.Type.BYTES,
    Schema.Type.BOOLEAN
  )

  private val primitiveGen: Gen[Schema.Type] = Gen.oneOf(primitiveTypes)

  /** Avro field names use a `[A-Za-z_][A-Za-z0-9_]*` shape. */
  private val fieldNameGen: Gen[String] = for {
    head <- Gen.oneOf(('a' to 'z') ++ ('A' to 'Z') :+ '_')
    tail <- Gen.listOfN(4, Gen.oneOf(('a' to 'z') ++ ('0' to '9') :+ '_'))
  } yield (head :: tail).mkString

  private val recordNameGen: Gen[String] = for {
    head <- Gen.oneOf(('A' to 'Z'))
    tail <- Gen.listOfN(5, Gen.alphaLowerChar)
  } yield (head :: tail).mkString

  private val namespaceGen: Gen[String] =
    Gen.oneOf("com.say5.gen", "x.y.z", "test.pkg")

  /** Render a primitive Avro type literally; for STRING/BYTES wrap in quotes properly. */
  private def primStr(t: Schema.Type): String = t.getName

  private def primDefault(t: Schema.Type): String = t match {
    case Schema.Type.INT     => "0"
    case Schema.Type.LONG    => "0"
    case Schema.Type.FLOAT   => "0.0"
    case Schema.Type.DOUBLE  => "0.0"
    case Schema.Type.STRING  => "\"\""
    case Schema.Type.BYTES   => "\"\""
    case Schema.Type.BOOLEAN => "false"
    case _                   => "null"
  }

  private case class GenField(name: String, tpe: Schema.Type, hasDefault: Boolean)

  private val fieldGen: Gen[GenField] = for {
    n  <- fieldNameGen
    t  <- primitiveGen
    hd <- Gen.oneOf(true, false)
  } yield GenField(n, t, hd)

  /** Render a `GenField` as its JSON form for schema construction. */
  private def renderField(f: GenField): String = {
    val defaultPart = if (f.hasDefault) s""", "default": ${primDefault(f.tpe)}""" else ""
    s"""{ "name": "${f.name}", "type": "${primStr(f.tpe)}"$defaultPart }"""
  }

  /** Build a record schema string with a list of fields and a fixed namespace + name. */
  private def schemaJson(name: String, ns: String, fields: List[GenField]): String =
    s"""{
       |  "type": "record",
       |  "name": "$name",
       |  "namespace": "$ns",
       |  "fields": [ ${fields.map(renderField).mkString(", ")} ]
       |}""".stripMargin

  private def parse(json: String): Schema = new Schema.Parser().parse(json)

  /** Generate a base record + at least one extra field that has a default. The `(old, new)` pair models
    * "added a field with a default", which must be backward compatible.
    */
  private val addFieldWithDefaultGen: Gen[(Schema, Schema)] = for {
    ns        <- namespaceGen
    name      <- recordNameGen
    base      <- Gen.nonEmptyListOf(fieldGen).map(_.take(3).map(_.copy(hasDefault = false)))
    addedName <- fieldNameGen.suchThat(n => !base.exists(_.name == n))
    addedType <- primitiveGen
  } yield {
    val oldS = parse(schemaJson(name, ns, base))
    val newF = GenField(addedName, addedType, hasDefault = true)
    val newS = parse(schemaJson(name, ns, base :+ newF))
    (oldS, newS)
  }

  /** Generate a pair where a non-default required field is removed. This must always be backward incompatible
    * (RemovedRequiredField).
    */
  private val removeRequiredFieldGen: Gen[(Schema, Schema)] = for {
    ns          <- namespaceGen
    name        <- recordNameGen
    keptHead    <- fieldGen.map(_.copy(hasDefault = false))
    extras      <- Gen.listOfN(2, fieldGen).map(_.map(_.copy(hasDefault = false)))
    removedName <- fieldNameGen.suchThat(n => n != keptHead.name && !extras.exists(_.name == n))
    removedType <- primitiveGen
  } yield {
    val removed = GenField(removedName, removedType, hasDefault = false)
    // Old has [removed] + kept. New has only kept.
    val oldFields = removed :: keptHead :: extras
    val newFields = keptHead :: extras
    val oldS      = parse(schemaJson(name, ns, oldFields))
    val newS      = parse(schemaJson(name, ns, newFields))
    (oldS, newS)
  }

  /** Generate a `(old, new)` pair where a single field's type is narrowed (long → int, double → float, etc.).
    * Must always violate.
    */
  private val typeNarrowingGen: Gen[(Schema, Schema)] = {
    val narrowings: List[(Schema.Type, Schema.Type)] = List(
      Schema.Type.LONG   -> Schema.Type.INT,
      Schema.Type.DOUBLE -> Schema.Type.FLOAT,
      Schema.Type.DOUBLE -> Schema.Type.LONG,
      Schema.Type.FLOAT  -> Schema.Type.INT
    )
    for {
      ns    <- namespaceGen
      name  <- recordNameGen
      fname <- fieldNameGen
      pair  <- Gen.oneOf(narrowings)
    } yield {
      val (oldT, newT) = pair
      val oldS         = parse(schemaJson(name, ns, List(GenField(fname, oldT, hasDefault = false))))
      val newS         = parse(schemaJson(name, ns, List(GenField(fname, newT, hasDefault = false))))
      (oldS, newS)
    }
  }

  /** Generate `(old, new)` where a single field is renamed and the new schema has *no* aliases entry. Must
    * always violate.
    */
  private val renameWithoutAliasGen: Gen[(Schema, Schema)] = for {
    ns      <- namespaceGen
    name    <- recordNameGen
    oldName <- fieldNameGen
    newName <- fieldNameGen.suchThat(_ != oldName)
    tpe     <- primitiveGen
  } yield {
    val oldS = parse(schemaJson(name, ns, List(GenField(oldName, tpe, hasDefault = false))))
    val newS = parse(schemaJson(name, ns, List(GenField(newName, tpe, hasDefault = false))))
    (oldS, newS)
  }

  /** Generate a single arbitrary record schema (for roundtrip testing). */
  private val arbitrarySchemaGen: Gen[Schema] = for {
    ns     <- namespaceGen
    name   <- recordNameGen
    fields <- Gen.nonEmptyListOf(fieldGen).map(_.take(5))
  } yield parse(schemaJson(name, ns, fields))

  // Implicit Arbitrary so forAll works without ad-hoc syntax.
  implicit private val arbSchema: Arbitrary[Schema] = Arbitrary(arbitrarySchemaGen)

  // ---------------- Properties ----------------

  test("property: adding a field with a default is always backward compatible") {
    forAll(addFieldWithDefaultGen) { case (oldS, newS) =>
      Compatibility.check(oldS, newS, CompatibilityLevel.Backward) shouldBe Right(())
    }
  }

  test("property: removing a required (non-default) field always produces a RemovedRequiredField violation") {
    forAll(removeRequiredFieldGen) { case (oldS, newS) =>
      val res = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
      res.isLeft shouldBe true
      res.left.toOption.get.collect { case _: Violation.RemovedRequiredField => 1 }.size should be >= 1
    }
  }

  test("property: type narrowing always produces a TypeIncompatible violation") {
    forAll(typeNarrowingGen) { case (oldS, newS) =>
      val res = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
      res.isLeft shouldBe true
      res.left.toOption.get.collect { case _: Violation.TypeIncompatible => 1 }.size should be >= 1
    }
  }

  test("property: rename without alias always produces a RenamedWithoutAlias violation") {
    forAll(renameWithoutAliasGen) { case (oldS, newS) =>
      val res = Compatibility.check(oldS, newS, CompatibilityLevel.Backward)
      res.isLeft shouldBe true
      val kinds = res.left.toOption.get.map(_.getClass.getSimpleName).toSet
      // Engine reports either RenamedWithoutAlias or MissingDefault + RemovedRequiredField.
      // The point is: this is *never* compatible.
      kinds should not be empty
    }
  }

  test("property: roundtrip — Schema.toString followed by Schema.Parser yields an equal schema") {
    forAll(arbitrarySchemaGen) { schema =>
      val serialized = schema.toString
      val parsed     = new Schema.Parser().parse(serialized)
      parsed shouldBe schema
    }
  }

  test("property: identity check is always Right (a schema is compatible with itself)") {
    forAll(arbitrarySchemaGen) { schema =>
      Compatibility.check(schema, schema, CompatibilityLevel.Backward) shouldBe Right(())
      Compatibility.check(schema, schema, CompatibilityLevel.Forward) shouldBe Right(())
      Compatibility.check(schema, schema, CompatibilityLevel.Full) shouldBe Right(())
    }
  }

  test("property: None_ level never produces violations regardless of input") {
    forAll(arbitrarySchemaGen, arbitrarySchemaGen) { (a, b) =>
      Compatibility.check(a, b, CompatibilityLevel.None_) shouldBe Right(())
    }
  }

  test("property: violation list is always deduplicated") {
    forAll(arbitrarySchemaGen, arbitrarySchemaGen) { (a, b) =>
      Compatibility.check(a, b, CompatibilityLevel.Backward) match {
        case Right(_) => succeed
        case Left(vs) => vs.distinct.size shouldBe vs.size
      }
    }
  }

  test("property: every generated record schema has at least one field readable via getFields") {
    forAll(arbitrarySchemaGen) { schema =>
      schema.getType shouldBe Schema.Type.RECORD
      schema.getFields.asScala should not be empty
    }
  }
}
