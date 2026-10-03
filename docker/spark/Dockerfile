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

# Kafka source for Structured Streaming (versions from the spark-sql-kafka 4.1.3 POM).
ARG MAVEN_ROOT=https://repo1.maven.org/maven2
ADD --chown=spark:spark \
    --checksum=sha256:cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6 \
    ${MAVEN_ROOT}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar \
    /opt/spark/jars/
ADD --chown=spark:spark \
    --checksum=sha256:0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c \
    ${MAVEN_ROOT}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
    /opt/spark/jars/
ADD --chown=spark:spark \
    --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    ${MAVEN_ROOT}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    /opt/spark/jars/
ADD --chown=spark:spark \
    --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef \
    ${MAVEN_ROOT}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
    /opt/spark/jars/

# s3a filesystem for streaming checkpoints and ops files (Spark checkpoints need a Hadoop
# FileSystem; Iceberg's S3FileIO only serves tables). hadoop-aws matches Spark's Hadoop
# 3.4.2. Instead of the 600 MB AWS SDK bundle it reuses the SDK classes already in
# iceberg-aws-bundle (SDK 2.54.17) and adds only the two modules s3a needs that the bundle
# lacks, at that version: the transfer manager and the Apache HTTP client (whose
# httpclient/httpcore dependencies already ship with Spark).
ADD --chown=spark:spark \
    --checksum=sha256:d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9 \
    ${MAVEN_ROOT}/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    /opt/spark/jars/
ADD --chown=spark:spark \
    --checksum=sha256:7bef0ea4efd5e0860bab63e08317040951edb51b72a5be734284c28b8324ab87 \
    ${MAVEN_ROOT}/software/amazon/awssdk/s3-transfer-manager/2.54.17/s3-transfer-manager-2.54.17.jar \
    /opt/spark/jars/
ADD --chown=spark:spark \
    --checksum=sha256:0b00ae3d55c3d1213ca47b5651dc4c2a0080fc9b6192dde3d53c3da6c1811bcb \
    ${MAVEN_ROOT}/software/amazon/awssdk/apache-client/2.54.17/apache-client-2.54.17.jar \
    /opt/spark/jars/

ENV PYSPARK_PYTHON=python3.13 \
    PYSPARK_DRIVER_PYTHON=python3.13 \
    TZ=UTC

USER spark
WORKDIR /opt/wikiwatch
