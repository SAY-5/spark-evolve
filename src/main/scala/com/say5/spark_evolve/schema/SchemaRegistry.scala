package com.say5.spark_evolve.schema

import org.apache.avro.Schema

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** File-backed Avro schema registry.
  *
  * Layout: `<root>/<subject>/v<version>.avsc`. Subjects are folder names, versions are integers parsed from
  * the file basename. The registry holds an in-memory cache once loaded; `register` persists to disk and
  * bumps the version.
  *
  * Not a substitute for Confluent Schema Registry — this is the local file scheme used by tests and the
  * bundled CLI.
  */
final class SchemaRegistry private (
  rootDir: Path,
  cache: mutable.Map[String, Vector[Schema]]
) {

  /** All subjects currently known. */
  def subjects: Set[String] = cache.keySet.toSet

  /** All versions of a subject, oldest first. */
  def versions(subject: String): Vector[Schema] =
    cache.getOrElse(subject, Vector.empty)

  /** The latest version of a subject if any exists. */
  def latest(subject: String): Option[Schema] =
    cache.get(subject).flatMap(_.lastOption)

  /** Register a new schema for a subject. Validates compatibility against the current latest version at the
    * requested level. On success, the new schema is appended and persisted to `<root>/<subject>/v<n>.avsc`.
    */
  def register(
    subject: String,
    schema: Schema,
    level: Compatibility.CompatibilityLevel
  ): Either[List[Violation], Int] = {
    val current = cache.getOrElse(subject, Vector.empty)
    val checkResult: Either[List[Violation], Unit] = current.lastOption match {
      case None      => Right(())
      case Some(old) => Compatibility.check(old, schema, level)
    }
    checkResult.map { _ =>
      val nextVersion = current.size + 1
      val newVec      = current :+ schema
      cache.update(subject, newVec)
      val subjDir = rootDir.resolve(subject)
      Files.createDirectories(subjDir)
      val target = subjDir.resolve(s"v$nextVersion.avsc")
      Files.writeString(target, schema.toString(true))
      nextVersion
    }
  }
}

object SchemaRegistry {

  /** Build an empty in-memory registry rooted at a temp dir. */
  def empty(): SchemaRegistry = {
    val tmp = Files.createTempDirectory("spark-evolve-registry-")
    new SchemaRegistry(tmp, mutable.Map.empty)
  }

  /** Load all subjects under `root`. Each subdirectory of `root` is a subject; each `v<n>.avsc` inside is a
    * version. Versions are sorted ascending by `n`.
    */
  def fromDirectory(root: Path): Try[SchemaRegistry] = Try {
    if (!Files.isDirectory(root)) {
      throw new IllegalArgumentException(s"not a directory: $root")
    }
    val cache = mutable.Map.empty[String, Vector[Schema]]
    Files.list(root).iterator().asScala.foreach { subjPath =>
      if (Files.isDirectory(subjPath)) {
        val subject = subjPath.getFileName.toString
        val versionedFiles = Files
          .list(subjPath)
          .iterator()
          .asScala
          .toList
          .filter(p => p.getFileName.toString.endsWith(".avsc"))
          .flatMap { p =>
            val name = p.getFileName.toString.stripSuffix(".avsc")
            if (name.startsWith("v")) {
              Try(name.drop(1).toInt).toOption.map(_ -> p)
            } else None
          }
          .sortBy(_._1)
        // Use a fresh parser per file — Avro's parser refuses to redefine a record name.
        val schemas = versionedFiles.map { case (_, p) =>
          new Schema.Parser().parse(p.toFile)
        }.toVector
        if (schemas.nonEmpty) cache.update(subject, schemas)
      }
    }
    new SchemaRegistry(root, cache)
  }

  /** Load a single schema file; convenience for the CLI. */
  def parseFile(path: String): Try[Schema] =
    Try(new Schema.Parser().parse(new File(path))) match {
      case s @ Success(_) => s
      case Failure(t)     => Failure(new RuntimeException(s"failed to parse $path: ${t.getMessage}", t))
    }

  /** Convenience: locate the canonical schemas dir bundled with the project. */
  def defaultDir(): Path = Paths.get("schemas")
}
