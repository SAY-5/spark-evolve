package com.say5.spark_evolve.transform

import com.say5.spark_evolve.schema.SchemaRegistry
import org.apache.avro.Schema
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** Fuzz tests for the Avro deserializer in `Validator.decode`.
  *
  * Property: feeding random byte sequences to the decoder must never throw out of
  * `Validator.decodeWithFallback`. Bad payloads must surface as `None`, not as exceptions, so the bad-records
  * sink can capture them.
  */
class ValidatorFuzzSpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {

  private lazy val readerSchema: Schema = SchemaRegistry.parseFile("schemas/orders/v2.avsc").get

  // Smaller payload sizes are fine — Avro decoders fail fast on bad headers.
  private val smallPayload: Gen[Array[Byte]] = for {
    n     <- Gen.chooseNum(0, 64)
    bytes <- Gen.listOfN(n, Gen.chooseNum(Byte.MinValue, Byte.MaxValue))
  } yield bytes.map(_.toByte).toArray

  private val mediumPayload: Gen[Array[Byte]] = for {
    n     <- Gen.chooseNum(0, 512)
    bytes <- Gen.listOfN(n, Gen.chooseNum(Byte.MinValue, Byte.MaxValue))
  } yield bytes.map(_.toByte).toArray

  test("fuzz: random small byte sequences never crash Validator.decodeWithFallback") {
    forAll(smallPayload) { bytes =>
      noException should be thrownBy {
        val result = Validator.decodeWithFallback(readerSchema, Seq.empty, bytes)
        // Any returned value is acceptable — None is the typical outcome for random bytes.
        // What we're asserting is non-throwing behavior.
        val _ = result
      }
    }
  }

  test("fuzz: random medium byte sequences never crash Validator.decodeWithFallback") {
    forAll(mediumPayload) { bytes =>
      noException should be thrownBy {
        val _ = Validator.decodeWithFallback(readerSchema, Seq.empty, bytes)
      }
    }
  }

  test("fuzz: random bytes against multi-writer fallback never crash") {
    val v1 = SchemaRegistry.parseFile("schemas/orders/v1.avsc").get
    val v2 = SchemaRegistry.parseFile("schemas/orders/v2.avsc").get
    forAll(mediumPayload) { bytes =>
      noException should be thrownBy {
        val _ = Validator.decodeWithFallback(v2, Seq(v1), bytes)
      }
    }
  }

  test("fuzz: empty payload yields a Left from the underlying decode helper") {
    val singleReader = new org.apache.avro.generic.GenericDatumReader[org.apache.avro.generic.GenericRecord](
      readerSchema
    )
    val result = Validator.decode(singleReader, readerSchema, Array.emptyByteArray)
    result.isLeft shouldBe true
  }

  test("fuzz: null payload yields a Left from the underlying decode helper") {
    val singleReader = new org.apache.avro.generic.GenericDatumReader[org.apache.avro.generic.GenericRecord](
      readerSchema
    )
    val result = Validator.decode(singleReader, readerSchema, null)
    result.isLeft shouldBe true
  }

  test("fuzz: payloads of fixed garbage patterns return None or are caught") {
    val patterns = Seq(
      Array.fill[Byte](16)(0),
      Array.fill[Byte](16)(0xff.toByte),
      Array[Byte](0x01, 0x02, 0x03, 0x04, 0x05, 0x06),
      "not_avro_at_all".getBytes("UTF-8"),
      Array[Byte](-1, -1, -1, -1, -1, -1, -1, -1, -1, -1)
    )
    patterns.foreach { p =>
      noException should be thrownBy {
        val _ = Validator.decodeWithFallback(readerSchema, Seq.empty, p)
      }
    }
  }
}
