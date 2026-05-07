import sbt.Keys._

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.say5"
ThisBuild / version      := "0.1.0"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Wunused:imports"
)

// Spark + Java 17 module access flags. Required for Arrow / Janino on JDK 17.
val javaModuleOpts = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

ThisBuild / javaOptions ++= javaModuleOpts

val sparkVersion               = "3.5.1"
val avroVersion                = "1.11.3"
val kafkaVersion               = "3.7.0"
val testcontainersScalaVersion = "0.41.3"

lazy val IntegrationTest = config("it").extend(Test)

// Marker for spark deps so we can exclude them from the assembly jar while
// keeping them on the compile/run/test classpath.
val icebergVersion = "1.5.2"

val sparkDeps: Seq[ModuleID] = Seq(
  "org.apache.spark"  %% "spark-core"                  % sparkVersion,
  "org.apache.spark"  %% "spark-sql"                   % sparkVersion,
  "org.apache.spark"  %% "spark-sql-kafka-0-10"        % sparkVersion,
  "org.apache.spark"  %% "spark-avro"                  % sparkVersion,
  "org.apache.hadoop" %  "hadoop-aws"                  % "3.3.4",
  "com.amazonaws"     %  "aws-java-sdk-bundle"         % "1.12.262",
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5"  % icebergVersion
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    name := "spark-evolve",
    inConfig(IntegrationTest)(Defaults.testSettings),
    Test / fork := true,
    IntegrationTest / fork := true,
    Test / javaOptions ++= javaModuleOpts,
    IntegrationTest / javaOptions ++= javaModuleOpts,
    run / fork := true,
    run / javaOptions ++= javaModuleOpts,
    Test / parallelExecution := false,
    IntegrationTest / parallelExecution := false,
    libraryDependencies ++= sparkDeps ++ Seq(
      "org.apache.avro"  %  "avro"           % avroVersion,
      "org.apache.kafka" %  "kafka-clients"  % kafkaVersion,
      "com.typesafe"     %  "config"         % "1.4.3",
      "org.slf4j"        %  "slf4j-api"      % "2.0.13",
      "ch.qos.logback"   %  "logback-classic" % "1.4.14"     % Runtime,
      "org.scalatest"    %% "scalatest"      % "3.2.18"     % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0"  % Test,
      "org.scalacheck"   %% "scalacheck"     % "1.17.0"     % Test,
      "com.dimafeng"     %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % Test,
      "com.dimafeng"     %% "testcontainers-scala-kafka"     % testcontainersScalaVersion % Test,
      "com.dimafeng"     %% "testcontainers-scala-minio"     % testcontainersScalaVersion % Test
    ),
    // sbt-assembly settings.
    assembly / assemblyJarName := "spark-evolve.jar",
    assembly / mainClass := Some("com.say5.spark_evolve.Main"),
    // Exclude Spark / Hadoop / AWS jars from the fat jar — the cluster supplies them.
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { f =>
        val name = f.data.getName
        name.startsWith("spark-") ||
        name.startsWith("hadoop-") ||
        name.startsWith("aws-java-sdk-") ||
        name.startsWith("scala-library") ||
        name.startsWith("scala-reflect") ||
        name.startsWith("scala-compiler") ||
        name.startsWith("iceberg-spark-runtime-")
      }
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case "reference.conf"                         => MergeStrategy.concat
      case "application.conf"                       => MergeStrategy.concat
      case x if x.endsWith(".proto")                => MergeStrategy.rename
      case x if x.endsWith("module-info.class")     => MergeStrategy.discard
      case _                                        => MergeStrategy.first
    },
    coverageMinimumStmtTotal := 65,
    coverageFailOnMinimum := false
  )

addCommandAlias("fmtCheck", "scalafmtCheckAll")
addCommandAlias("fmt",      "scalafmtAll")
