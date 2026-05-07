package com.say5.spark_evolve.schema

import org.apache.avro.Schema

import scala.jdk.CollectionConverters._

/** Pure-Scala Avro schema compatibility engine.
  *
  * Decides whether a new schema may replace an old schema at a given compatibility level. Accumulates every
  * violation it finds rather than short-circuiting on the first one.
  */
object Compatibility {

  sealed trait CompatibilityLevel
  object CompatibilityLevel {
    case object Backward extends CompatibilityLevel
    case object Forward  extends CompatibilityLevel
    case object Full     extends CompatibilityLevel
    case object None_    extends CompatibilityLevel

    def parse(s: String): Either[String, CompatibilityLevel] = s.toLowerCase match {
      case "backward" => Right(Backward)
      case "forward"  => Right(Forward)
      case "full"     => Right(Full)
      case "none"     => Right(None_)
      case other      => Left(s"unknown compatibility level: $other")
    }
  }

  /** Top-level entry: for the requested level, return Right(()) on success, Left(list of violations)
    * otherwise. The list is never empty on Left.
    */
  def check(
    oldSchema: Schema,
    newSchema: Schema,
    level: CompatibilityLevel
  ): Either[List[Violation], Unit] = {
    val violations = level match {
      case CompatibilityLevel.Backward =>
        checkOneWay(writer = oldSchema, reader = newSchema) ++
          checkRemovedRequired(oldSchema, newSchema)
      case CompatibilityLevel.Forward =>
        // Forward = old reader, new writer. The "removed required field" warning is
        // direction-symmetric only when applied at the user's evolution direction (old→new),
        // so we don't run it here — `checkOneWay` carries the per-field type/default checks.
        checkOneWay(writer = newSchema, reader = oldSchema)
      case CompatibilityLevel.Full =>
        checkOneWay(writer = oldSchema, reader = newSchema) ++
          checkOneWay(writer = newSchema, reader = oldSchema) ++
          checkRemovedRequired(oldSchema, newSchema)
      case CompatibilityLevel.None_ => Nil
    }
    if (violations.isEmpty) Right(()) else Left(violations.distinct)
  }

  /** Detects fields that were required (no default) in `oldS` and disappeared from `newS`. This isn't a
    * strict Avro decode error, but it breaks downstream queries that depend on the column. Listed in the
    * project's compatibility table as a backward-incompatible change.
    */
  private def checkRemovedRequired(oldS: Schema, newS: Schema): List[Violation] = {
    if (oldS.getType != Schema.Type.RECORD || newS.getType != Schema.Type.RECORD) return Nil
    val oldFields      = oldS.getFields.asScala.toList
    val newFields      = newS.getFields.asScala.toList
    val newByName      = newFields.map(_.name).toSet
    val readerHasAlias = newFields.flatMap(_.aliases().asScala).toSet

    val writerOnly = oldFields.filter { of =>
      !newByName.contains(of.name) && !readerHasAlias.contains(of.name)
    }
    val readerOnly = newFields.filter(nf => !oldFields.exists(_.name == nf.name))
    writerOnly
      .filterNot(_.hasDefaultValue)
      .filterNot(of => readerOnly.exists(nf => sameType(of.schema(), nf.schema())))
      .map(of => Violation.RemovedRequiredField(of.name))
  }

  /** Walk writer→reader. The reader walks its own fields and looks them up in the writer's payload by name
    * (with aliases). Fields the reader expects but the writer didn't write must have a default in the reader
    * schema. Type changes between matched fields must be safe promotions.
    */
  private def checkOneWay(writer: Schema, reader: Schema): List[Violation] = {
    val builder = List.newBuilder[Violation]
    builder ++= checkRecordNames(writer, reader)

    if (writer.getType != Schema.Type.RECORD || reader.getType != Schema.Type.RECORD) {
      return builder.result()
    }

    val writerFields = writer.getFields.asScala.toList
    val readerFields = reader.getFields.asScala.toList
    val writerByName = writerFields.map(f => f.name -> f).toMap

    // For each field the reader expects, find what the writer wrote.
    readerFields.foreach { rf =>
      val aliases = rf.aliases().asScala.toSet
      val matched = writerByName.get(rf.name).orElse {
        writerFields.find(wf => aliases.contains(wf.name))
      }
      matched match {
        case None =>
          if (!hasDefaultOrNullable(rf)) {
            builder += Violation.MissingDefault(rf.name)
          }
        case Some(wf) =>
          builder ++= checkTypeChange(rf.name, wf.schema(), rf.schema())
          builder ++= checkNullability(rf.name, wf.schema(), rf.schema(), rf)
      }
    }

    // Likely rename detection: writer field with no reader counterpart whose type matches a
    // reader-only field, where the reader field doesn't list the writer's name in aliases.
    val readerOnly = readerFields.filterNot { rf =>
      writerByName.contains(rf.name) ||
      writerFields.exists(wf => rf.aliases().asScala.contains(wf.name))
    }
    val writerOnly = writerFields.filterNot { wf =>
      readerFields.exists(_.name == wf.name) ||
      readerFields.exists(_.aliases().asScala.contains(wf.name))
    }
    writerOnly.foreach { wf =>
      readerOnly
        .find(rf => sameType(wf.schema(), rf.schema()) && !rf.aliases().asScala.contains(wf.name))
        .foreach(rf => builder += Violation.RenamedWithoutAlias(wf.name, rf.name))
    }

    builder.result()
  }

  private def checkRecordNames(oldS: Schema, newS: Schema): List[Violation] = {
    if (oldS.getType == Schema.Type.RECORD && newS.getType == Schema.Type.RECORD) {
      if (oldS.getFullName != newS.getFullName) {
        // Aliases on the new schema can absorb the old name.
        val aliases = Option(newS.getAliases).map(_.asScala.toSet).getOrElse(Set.empty[String])
        if (!aliases.contains(oldS.getFullName)) {
          return List(Violation.RecordNameMismatch(oldS.getFullName, newS.getFullName))
        }
      }
    }
    Nil
  }

  private def checkTypeChange(
    fieldName: String,
    oldT: Schema,
    newT: Schema
  ): List[Violation] = {
    val oldType = unwrapNullable(oldT)
    val newType = unwrapNullable(newT)
    if (oldType.getType == newType.getType) {
      // Same primitive / record type, no violation.
      Nil
    } else if (isSafePromotion(oldType.getType, newType.getType)) {
      Nil
    } else {
      List(
        Violation.TypeIncompatible(fieldName, oldType.getType.toString, newType.getType.toString)
      )
    }
  }

  private def checkNullability(
    fieldName: String,
    oldT: Schema,
    newT: Schema,
    newField: Schema.Field
  ): List[Violation] = {
    val oldNullable = isNullable(oldT)
    val newNullable = isNullable(newT)
    if (!oldNullable && newNullable && !newField.hasDefaultValue) {
      List(Violation.NonNullableMadeNullable(fieldName))
    } else Nil
  }

  // ---- Helpers ----

  private def hasDefaultOrNullable(f: Schema.Field): Boolean =
    f.hasDefaultValue || isNullable(f.schema())

  private def isNullable(s: Schema): Boolean =
    s.getType == Schema.Type.UNION &&
      s.getTypes.asScala.exists(_.getType == Schema.Type.NULL)

  private def unwrapNullable(s: Schema): Schema =
    if (s.getType == Schema.Type.UNION) {
      val nonNull = s.getTypes.asScala.filterNot(_.getType == Schema.Type.NULL).toList
      nonNull match {
        case single :: Nil => single
        case _             => s
      }
    } else s

  private def sameType(a: Schema, b: Schema): Boolean =
    unwrapNullable(a).getType == unwrapNullable(b).getType

  /** Per the Avro spec, these promotions are reader-safe.
    */
  private def isSafePromotion(oldT: Schema.Type, newT: Schema.Type): Boolean =
    (oldT, newT) match {
      case (Schema.Type.INT, Schema.Type.LONG)     => true
      case (Schema.Type.INT, Schema.Type.FLOAT)    => true
      case (Schema.Type.INT, Schema.Type.DOUBLE)   => true
      case (Schema.Type.LONG, Schema.Type.FLOAT)   => true
      case (Schema.Type.LONG, Schema.Type.DOUBLE)  => true
      case (Schema.Type.FLOAT, Schema.Type.DOUBLE) => true
      case (Schema.Type.STRING, Schema.Type.BYTES) => true
      case (Schema.Type.BYTES, Schema.Type.STRING) => true
      case _                                       => false
    }
}
