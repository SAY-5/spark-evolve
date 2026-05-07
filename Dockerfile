# Multi-stage build: stage 1 produces the assembly jar, stage 2 is a minimal
# runtime image you can drop into a Spark cluster (the fat jar excludes the
# spark/hadoop deps; the cluster supplies them).
FROM eclipse-temurin:17-jdk AS build

# Install sbt.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg ca-certificates \
 && echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
 && curl -sSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | gpg --dearmor -o /usr/share/keyrings/sbt.gpg \
 && apt-get update && apt-get install -y --no-install-recommends sbt \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY build.sbt /src/
COPY project /src/project
RUN sbt update
COPY . /src
RUN sbt -batch assembly

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app
COPY --from=build /src/target/scala-2.13/spark-evolve.jar /app/spark-evolve.jar
COPY --from=build /src/schemas /app/schemas
ENTRYPOINT ["java", \
  "--add-opens=java.base/java.lang=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
  "--add-opens=java.base/java.io=ALL-UNNAMED", \
  "--add-opens=java.base/java.util=ALL-UNNAMED", \
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
  "-jar", "/app/spark-evolve.jar"]
