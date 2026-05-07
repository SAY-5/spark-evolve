package com.say5.spark_evolve.transform

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream

class ValidatorSpec extends AnyFunSuite with Matchers {

  private def schemaV1: Schema =
    new Schema.Parser().parse("""
      |{"type":"record","name":"Order","namespace":"com.say5.test","fields":[
      |  {"name":"order_id","type":"string"},
      |  {"name":"customer_id","type":"string"},
      |  {"name":"customer_segment","type":"string"},
      |  {"name":"total_cents","type":"long"},
      |  {"name":"event_time","type":"long"}
      |]}
    """.stripMargin)

  private def schemaV2: Schema =
    new Schema.Parser().parse("""
      |{"type":"record","name":"Order","namespace":"com.say5.test","fields":[
      |  {"name":"order_id","type":"string"},
      |  {"name":"customer_id","type":"string"},
      |  {"name":"customer_segment","type":"string"},
      |  {"name":"total_cents","type":"long"},
      |  {"name":"event_time","type":"long"},
      |  {"name":"discount_cents","type":"long","default":0}
      |]}
    """.stripMargin)

  private def encode(rec: GenericRecord, schema: Schema): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val enc = EncoderFactory.get().binaryEncoder(out, null)
    val w   = new GenericDatumWriter[GenericRecord](schema)
    w.write(rec, enc)
    enc.flush()
    out.toByteArray
  }

  private def aRecord(schema: Schema): GenericRecord = {
    val r = new GenericData.Record(schema)
    r.put("order_id", "o1")
    r.put("customer_id", "c1")
    r.put("customer_segment", "S")
    r.put("total_cents", 1000L)
    r.put("event_time", 0L)
    if (schema.getField("discount_cents") != null) r.put("discount_cents", 50L)
    r
  }

  test("decode round-trips a v1-encoded record under a v1 reader schema") {
    val payload = encode(aRecord(schemaV1), schemaV1)
    val reader  = new GenericDatumReader[GenericRecord](schemaV1)
    val r       = Validator.decode(reader, schemaV1, payload)
    r.isRight shouldBe true
    r.toOption.get.get("order_id").toString shouldBe "o1"
  }

  test("decode promotes a v1-encoded record under a v2 reader schema (default fills in)") {
    val payload = encode(aRecord(schemaV1), schemaV1)
    val reader  = new GenericDatumReader[GenericRecord](schemaV1, schemaV2)
    val r       = Validator.decode(reader, schemaV2, payload)
    r.isRight shouldBe true
    r.toOption.get.get("discount_cents").asInstanceOf[Long] shouldBe 0L
  }

  test("decode rejects malformed bytes with a structured error") {
    val reader = new GenericDatumReader[GenericRecord](schemaV1)
    val r      = Validator.decode(reader, schemaV1, Array[Byte](1, 2, 3, 4))
    r.isLeft shouldBe true
  }

  test("decode rejects empty payload") {
    val reader = new GenericDatumReader[GenericRecord](schemaV1)
    val r      = Validator.decode(reader, schemaV1, Array.empty[Byte])
    r.isLeft shouldBe true
  }

  test("OrderRowSchema has the expected fields in order") {
    Validator.OrderRowSchema.fieldNames.toList shouldBe List(
      "order_id",
      "customer_id",
      "customer_segment",
      "total_cents",
      "event_time",
      "discount_cents"
    )
  }

  test("BadRowSchema has reason as a non-nullable column") {
    val reason = Validator.BadRowSchema("reason")
    reason.nullable shouldBe false
  }
}
