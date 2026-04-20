FROM flink:1.20-java11

# =============================================================================
# Kafka SQL connector — uber-jar, no transitive deps needed
# =============================================================================
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG KAFKA_CONNECTOR_JAR=flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/${KAFKA_CONNECTOR_JAR}" \
    -o "/opt/flink/lib/${KAFKA_CONNECTOR_JAR}"

# =============================================================================
# Iceberg Flink runtime — uber-jar, includes NessieCatalog + nessie-client
# NOTE: 1.6.x does not exist for flink-1.20; minimum is 1.7.0
# =============================================================================
ARG ICEBERG_VERSION=1.7.2
ARG ICEBERG_JAR=iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/${ICEBERG_JAR}" \
    -o "/opt/flink/lib/${ICEBERG_JAR}"

# =============================================================================
# Iceberg AWS bundle — provides S3FileIO + AWS SDK v2
# iceberg-flink-runtime does NOT bundle the AWS module; this JAR is required
# separately for S3FileIO (software.amazon.awssdk classes).
# =============================================================================
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    -o "/opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# =============================================================================
# Hadoop client runtime
# NessieCatalog (and HadoopFileIO) require org.apache.hadoop.conf.Configuration.
# The Flink 1.20 base image does NOT bundle Hadoop. hadoop-client-runtime is an
# uber-jar that provides the full Hadoop client API without external dependencies.
# NOTE: keep this in lib/ (not plugins/) so it is visible to the application classloader.
# =============================================================================
ARG HADOOP_VERSION=3.3.6
# hadoop-client-api  — provides org.apache.hadoop.conf.Configuration and all API classes
# hadoop-client-runtime — provides the shaded runtime dependencies (protobuf, commons, etc.)
# Both are required: api has the interfaces, runtime has the implementations
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" \
    -o "/opt/flink/lib/hadoop-client-api-${HADOOP_VERSION}.jar" \
 && curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" \
    -o "/opt/flink/lib/hadoop-client-runtime-${HADOOP_VERSION}.jar"

# =============================================================================
# Flink JDBC connector — enables 'connector' = 'jdbc' in Flink SQL
# =============================================================================
ARG JDBC_CONNECTOR_VERSION=3.2.0-1.19
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar" \
    -o "/opt/flink/lib/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar"

# =============================================================================
# ClickHouse JDBC driver
# The -all classifier bundles clickhouse-client + clickhouse-http-client into one JAR.
# Driver class: com.clickhouse.jdbc.ClickHouseDriver
# JDBC URL format: jdbc:clickhouse://host:8123/database
# =============================================================================
ARG CLICKHOUSE_JDBC_VERSION=0.6.4
RUN curl -fSL \
    "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
    -o "/opt/flink/lib/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"

# =============================================================================
# S3 / MinIO filesystem plugin
# The base image ships flink-s3-fs-hadoop-*.jar in /opt/flink/opt/.
# It must live in plugins/s3-fs-hadoop/, NOT lib/ (classloader conflicts).
# Use a glob so this works regardless of the exact Flink patch version.
# =============================================================================
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && ln -s "$(ls /opt/flink/opt/flink-s3-fs-hadoop-*.jar | head -1)" \
          /opt/flink/plugins/s3-fs-hadoop/

# =============================================================================
# Checkpoint directory
# Create with correct ownership for the flink user (uid 9999 in flink:1.20-java11)
# The flink-checkpoints named volume is mounted here; pre-creating it ensures
# the flink user can write subdirectories without permission errors.
# =============================================================================
RUN mkdir -p /tmp/flink-checkpoints && chown 9999:9999 /tmp/flink-checkpoints

# =============================================================================
# Cluster configuration and SQL job
# =============================================================================
COPY conf/config.yaml /opt/flink/conf/config.yaml
COPY sql/ /opt/flink/sql/
