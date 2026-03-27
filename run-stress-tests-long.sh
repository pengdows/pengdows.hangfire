#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
results="${root}/TestResults/stress-long"
mkdir -p "${results}"

dotnet test "${root}/pengdows.hangfire.stress.tests/pengdows.hangfire.stress.tests.csproj" \
  -c Release \
  --filter "Category=LongRunning" \
  --results-directory "${results}" \
  --logger "trx;LogFileName=StressTests.LongRunning.trx" \
  --logger "console;verbosity=normal"
