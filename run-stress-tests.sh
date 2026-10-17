#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
provider="${1:-all}"
results="${root}/TestResults/stress/${provider}"
mkdir -p "${results}"

case "${provider}" in
  postgres)
    filter='FullyQualifiedName~Postgres|FullyQualifiedName~HangfireStartupStormFacts'
    ;;
  sqlserver)
    filter='FullyQualifiedName~SqlServer|FullyQualifiedName~LockStressTests|FullyQualifiedName~LockCrashPathTests|FullyQualifiedName~LockExceptionTranslationFacts|FullyQualifiedName~LockFailureTests|FullyQualifiedName~LockOutOfProcessTests|FullyQualifiedName~LockBaselineComparisonTests|FullyQualifiedName~BoundaryPressureTests|FullyQualifiedName~JobQueueStressFacts'
    ;;
  mysql)
    filter='FullyQualifiedName~MySqlLockStressFacts|FullyQualifiedName~MySqlQueueClaimStressFacts'
    ;;
  mariadb)
    filter='FullyQualifiedName~MariaDbLockStressFacts|FullyQualifiedName~MariaDbQueueClaimStressFacts'
    ;;
  sqlite)
    filter='FullyQualifiedName~SqliteLockStressFacts'
    ;;
  duckdb)
    filter='FullyQualifiedName~DuckDbLockStressFacts'
    ;;
  shared)
    filter='FullyQualifiedName~InMemorySingleConnectionFacts'
    ;;
  all)
    filter='Category!=LongRunning'
    ;;
  *)
    echo "Unknown stress provider: ${provider}" >&2
    exit 2
    ;;
esac

dotnet test "${root}/pengdows.hangfire.stress.tests/pengdows.hangfire.stress.tests.csproj" \
  -c Release \
  --filter "${filter}" \
  --results-directory "${results}" \
  --logger "trx;LogFileName=StressTests-${provider}.trx" \
  --logger "console;verbosity=normal"
