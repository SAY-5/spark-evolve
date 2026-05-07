# Sample inputs

This folder is intentionally light. The integration test (`src/it/scala/com/say5/spark_evolve/EndToEndIT.scala`)
generates Avro events at runtime; committing binary fixtures here would just
duplicate that.

If you want a hand to play with manually:

    sbt "runMain com.say5.spark_evolve.BenchHarness 100 /tmp/sample-output"

That writes 100 synthetic events through the same pipeline and dumps the
Parquet output you'd get end-to-end.
