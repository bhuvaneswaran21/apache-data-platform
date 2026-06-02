#!/usr/bin/env bash

set -euo pipefail

USRLIB="$(cd "$(dirname "$0")/.." && pwd)/flink/usrlib"
mkdir -p "$USRLIB"

MAVEN="https://repo1.maven.org/maven2"

declare -A JARS=(
  ["flink-sql-connector-kafka-3.3.0-1.19.jar"]="$MAVEN/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar"
  ["iceberg-flink-runtime-1.19-1.10.1.jar"]="$MAVEN/org/apache/iceberg/iceberg-flink-runtime-1.19/1.10.1/iceberg-flink-runtime-1.19-1.10.1.jar"
  ["flink-s3-fs-hadoop-1.19.1.jar"]="$MAVEN/org/apache/flink/flink-s3-fs-hadoop/1.19.1/flink-s3-fs-hadoop-1.19.1.jar"
  ["hadoop-client-runtime-3.3.4.jar"]="$MAVEN/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar"
)

for name in "${!JARS[@]}"; do
  dest="$USRLIB/$name"
  if [[ -f "$dest" ]]; then
    echo "  [skip]  $name"
  else
    echo "  [fetch] $name ..."
    curl -fsSL --progress-bar -o "$dest" "${JARS[$name]}"
    echo "          → $dest"
  fi
done


UBER="$USRLIB/flink-shaded-hadoop-3-uber-3.1.1.7.2.9.0-173-9.0.jar"
if [[ ! -f "$UBER" ]]; then
  echo "  [WARN] $UBER not found."
  echo "         This JAR is bundled with the repo. Re-clone or restore it."
else
  echo "  [skip]  flink-shaded-hadoop-3-uber (present but NOT mounted — kept for reference)"
fi

echo ""
echo "Flink lib/ JARs (3, mounted in docker-compose):"
echo "  hadoop-client-runtime-3.3.4.jar    ← Hadoop client uber-JAR (Configuration,"
echo "                                        HdfsConfiguration, UGI + all transitive deps)"
echo "  flink-sql-connector-kafka-3.3.0-1.19.jar"
echo "  iceberg-flink-runtime-1.19-1.10.1.jar"
echo "Flink plugins/s3/ (1 JAR, isolated classloader):"
echo "  flink-s3-fs-hadoop-1.19.1.jar"
echo ""
ls -lh "$USRLIB"/*.jar 2>/dev/null | awk '{print "  "$NF, $5}'
