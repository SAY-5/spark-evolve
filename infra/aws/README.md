# AWS Deploy Path (Stub)

This directory is a documented sketch of how to deploy `spark-evolve` to AWS.
It is **not provisioned** — there's no `terraform apply` here, no live
infrastructure, no real bucket names. The point is to show the production
target the local pipeline mirrors.

## Components

| Component        | AWS service              | Notes                                                     |
|------------------|--------------------------|-----------------------------------------------------------|
| Kafka cluster    | MSK (Apache Kafka)       | TLS + IAM auth; topic `orders` with at least 6 partitions |
| Schema store     | S3 (or Glue Schema Registry) | Local layout `schemas/<subject>/v<n>.avsc` ports directly |
| Object store     | S3                       | bucket `<env>-orders-warehouse`, partition `date_hour=`   |
| Job runner       | EMR-Serverless OR EKS    | submits the assembly jar, spark-submit-equivalent         |
| Bad-record sink  | S3 + SNS topic           | bad records to `bad_records/` and side-channel alert      |

## Wiring (high level)

```
producers  ──►  MSK (orders)  ──►  spark-evolve job  ──►  S3 (Parquet, partitioned)
                                          │
                                          ├──►  S3 (bad_records/)
                                          └──►  SNS / Slack alert
```

## What's intentionally not here

- **Terraform manifests.** This is a study project; provisioning real AWS
  costs money and isn't the engineering point being demonstrated.
- **MSK Connect / Kafka Connect.** The local docker-compose uses plain Kafka
  bootstrap; in AWS you'd add IAM SASL auth at the Spark side.
- **Glue / Athena hookup.** The Parquet layout (partitioned by `date_hour`,
  `customer_segment`) is Glue-friendly; defining the table is a one-liner DDL
  not worth committing.

## What you'd port if you were doing this for real

1. The `s3a://` paths just work against S3 — change `fs.s3a.endpoint` to the
   regional endpoint and switch to the IAM credential provider.
2. The Kafka bootstrap is read from env (`KAFKA_BOOTSTRAP`).
3. The compatibility CLI runs as a pre-deploy gate in CI: a PR that bumps a
   schema must produce a clean `schema check` against the previous version.
4. The bad-records sink fans out to SNS via a `foreachBatch` listener (one
   Spark line; not in scope here).
