#!/bin/bash
collections=("Sqlite" "SqlServer" "Postgres" "MySql" "MariaDB" "Oracle" "Firebird" "CockroachDb" "DuckDb" "YugabyteDb" "TiDb")

for col in "${collections[@]}"; do
  echo "================================================================================"
  echo "RUNNING: $col"
  echo "================================================================================"
  # Run only one test from the collection to minimize time, but enough to trigger disposal grid
  # ConnectionFacts.GetUtcDateTime_ReturnsCurrentUtc is a good candidate - fast and exists in all
  dotnet test pengdows.hangfire.integration.tests/pengdows.hangfire.integration.tests.csproj \
    -c Release --filter "FullyQualifiedName~${col}ConnectionFacts" -v m --logger "console;verbosity=normal" | \
    sed -n '/┌───/,/└───/p'
  echo ""
done
