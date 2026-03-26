#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
results="${root}/TestResults/integration"
mkdir -p "${results}"

dotnet test "${root}/pengdows.hangfire.integration.tests/pengdows.hangfire.integration.tests.csproj" \
  -c Release \
  --results-directory "${results}" \
  --logger "trx;LogFileName=IntegrationTests.trx"
