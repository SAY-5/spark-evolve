package com.say5.spark_evolve.schema

/** Typed record of a single backward/forward compatibility violation.
  *
  * The engine accumulates a `List[Violation]` rather than throwing on the first problem so operators see
  * every issue with a single check.
  */
sealed trait Violation {
  def fieldName: String
  def message: String
  override def toString: String = s"${getClass.getSimpleName}: $message"
}

object Violation {

  /** Field added to the new schema with no default value. New readers cannot read old data. */
  final case class MissingDefault(fieldName: String) extends Violation {
    val message: String =
      s"field '$fieldName' added without a default; old records would have nothing to fill it with"
  }

  /** Field renamed without recording the old name in `aliases`. */
  final case class RenamedWithoutAlias(oldName: String, newName: String) extends Violation {
    val fieldName: String = oldName
    val message: String =
      s"field '$oldName' appears to have been renamed to '$newName' but new schema has no aliases entry"
  }

  /** Field type changed in a way Avro does not promote. Per Avro spec, only int→long, int→float, int→double,
    * long→float, long→double, float→double, and string→bytes promotions are safe.
    */
  final case class TypeIncompatible(fieldName: String, oldType: String, newType: String) extends Violation {
    val message: String =
      s"field '$fieldName' type changed from '$oldType' to '$newType' which is not a safe promotion"
  }

  /** Required field (no default) was removed from the new schema. */
  final case class RemovedRequiredField(fieldName: String) extends Violation {
    val message: String =
      s"field '$fieldName' was required in old schema and was removed without a default"
  }

  /** Non-nullable field made nullable without a default. Old records lack the union tag; new readers can't
    * pick a default branch.
    */
  final case class NonNullableMadeNullable(fieldName: String) extends Violation {
    val message: String =
      s"field '$fieldName' was non-nullable; making it nullable requires a default"
  }

  /** Top-level record name or namespace differs. */
  final case class RecordNameMismatch(oldName: String, newName: String) extends Violation {
    val fieldName: String = "<root>"
    val message: String =
      s"record full name changed from '$oldName' to '$newName'; this is a different schema, not an evolution"
  }

  /** Render a list of violations as a Markdown bullet list for CLI output. */
  def toMarkdown(vs: List[Violation]): String =
    if (vs.isEmpty) "compatible"
    else vs.map(v => s"- **${v.getClass.getSimpleName}** (${v.fieldName}): ${v.message}").mkString("\n")
}
