# Local Iceberg REST catalog backed by Postgres instead of SQLite (ADR 0007).
# The reference server ships only a SQLite driver; SQLite allows one writer at a time and
# failed commits once several streaming queries committed concurrently (SQLITE_BUSY).
FROM apache/iceberg-rest-fixture:1.10.1

# PostgreSQL JDBC driver, pinned by SHA-256 (verified against Maven Central's SHA-1).
ADD --chown=iceberg:iceberg \
    --checksum=sha256:6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
    /usr/lib/iceberg-rest/postgresql.jar

# `java -jar` ignores the classpath, so start the same main class with both jars.
CMD ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
