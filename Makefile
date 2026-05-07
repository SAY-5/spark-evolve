# spark-evolve — Makefile.
# Most targets shell out to sbt. JDK 17 must be on PATH.

SBT ?= sbt -batch

.PHONY: help compile test it-test lint fmt assembly up down clean bench bench-smoke schema-check-ok schema-check-bad e2e

help:
	@echo "Targets:"
	@echo "  compile         compile main + test sources"
	@echo "  test            unit tests (no Docker required)"
	@echo "  it-test         integration tests (testcontainers brings up Kafka + MinIO)"
	@echo "  lint            scalafmt check"
	@echo "  fmt             scalafmt rewrite"
	@echo "  assembly        build the fat jar at target/scala-2.13/spark-evolve.jar"
	@echo "  up              docker-compose up -d (kafka + zookeeper + minio)"
	@echo "  down            docker-compose down -v"
	@echo "  bench           run the bench harness (100k events; takes a few minutes)"
	@echo "  bench-smoke     run a 5k-event smoke pass"
	@echo "  e2e             produce a tiny batch through Kafka + MinIO and check output"
	@echo "  schema-check-ok bundled v1 → v2 (compatible) demo"
	@echo "  schema-check-bad bundled v2 → v3 (incompatible) demo"
	@echo "  clean           sbt clean"

compile:
	$(SBT) compile

test:
	$(SBT) test

it-test:
	RUN_INTEGRATION=1 $(SBT) it:test

lint:
	$(SBT) scalafmtCheckAll

fmt:
	$(SBT) scalafmtAll

assembly:
	$(SBT) assembly

up:
	docker compose up -d
	@echo "Kafka on localhost:9092, MinIO on http://localhost:9000 (console http://localhost:9001)"

down:
	docker compose down -v

clean:
	$(SBT) clean

bench:
	./bench/run.sh 100000

bench-smoke:
	./bench/run.sh 5000

e2e:
	./bench/run.sh 1000 --skip-bench-output

schema-check-ok:
	$(SBT) "run schema check --old schemas/orders/v1.avsc --new schemas/orders/v2.avsc --level backward"

schema-check-bad:
	-$(SBT) "run schema check --old schemas/orders/v2.avsc --new schemas/orders/v3.avsc --level backward"
