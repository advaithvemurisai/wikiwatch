# Spark 4.1 + Iceberg 1.12 image for the WikiWatch streaming app (ADR 0003).
# The official image ships Python 3.10; the project standard is Python 3.13, so it is
# added from the deadsnakes PPA and PySpark is pointed at it.
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends software-properties-common gpg-agent \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3.13 \
    && apt-get purge -y software-properties-common gpg-agent \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Jars are pinned by version and SHA-256 (verified against Maven Central's SHA-1 on 2026-10-03).
ARG ICEBERG_VERSION=1.12.0
ARG MAVEN=https://repo1.maven.org/maven2/org/apache/iceberg
ADD --chown=spark:spark \
    --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 \
    ${MAVEN}/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/
ADD --chown=spark:spark \
    --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    ${MAVEN}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/

ENV PYSPARK_PYTHON=python3.13 \
    PYSPARK_DRIVER_PYTHON=python3.13 \
    TZ=UTC

USER spark
WORKDIR /opt/wikiwatch
