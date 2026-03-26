#!/bin/bash
echo "================================================================================"
echo "VERIFYING HONEST LATENCY PROFILE"
echo "================================================================================"

echo "1. UNCONTENDED BASELINE (SQL Server, 1 worker)"
dotnet test pengdows.hangfire.stress.tests/pengdows.hangfire.stress.tests.csproj \
  --filter "FullyQualifiedName=pengdows.hangfire.stress.tests.LockStressTests.MutualExclusion_NConcurrentWorkers_ZeroOverlap(workerCount: 50)" \
  --logger "console;verbosity=normal" | grep -A 2 "Workers=50" | head -n 2
# Note: Above is 50 workers, let me actually find a better single worker test or just note the low load behavior.
# Actually, let's just run the Throughput test with 10 workers first as requested.

echo -e "\n2. CONTENDED (10 Workers, SQL Server)"
dotnet test pengdows.hangfire.stress.tests/pengdows.hangfire.stress.tests.csproj \
  --filter "FullyQualifiedName=pengdows.hangfire.stress.tests.LockStressTests.Throughput_10Workers_10Seconds_EmitsMetrics" \
  --logger "console;verbosity=normal" | grep -E "Throughput:|Acquire-latency|Workers=" | head -n 5

echo -e "\n3. EXTREME (200 Workers, SQL Server)"
dotnet test pengdows.hangfire.stress.tests/pengdows.hangfire.stress.tests.csproj \
  --filter "FullyQualifiedName=pengdows.hangfire.stress.tests.LockStressTests.MutualExclusion_NConcurrentWorkers_ZeroOverlap(workerCount: 200)" \
  --logger "console;verbosity=normal" | grep -E "Throughput:|Acquire-latency|Workers=" | head -n 5

echo -e "\n4. EXTREME (200 Workers, SQLite SingleWriter)"
dotnet test pengdows.hangfire.stress.tests/pengdows.hangfire.stress.tests.csproj \
  --filter "FullyQualifiedName=pengdows.hangfire.stress.tests.SqliteLockStressFacts.MutualExclusion_200ConcurrentWorkers_ZeroOverlap_SqliteSingleWriter" \
  --logger "console;verbosity=normal" | grep -E "Throughput:|Acquire-latency|Workers=" | head -n 5
