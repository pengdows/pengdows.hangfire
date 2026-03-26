-- SqliteDialect.SupportsNamespaces = false: pengdows.crud generates bare table names at
-- runtime (no "HangFire." prefix).  DDL must match — all tables live in the main database.

CREATE TABLE IF NOT EXISTS "Schema" (
    "Version" INTEGER NOT NULL,
    PRIMARY KEY ("Version")
);

CREATE TABLE IF NOT EXISTS "Job" (
    "Id" INTEGER PRIMARY KEY,
    "StateId" INTEGER NULL,
    "StateName" TEXT NULL,
    "InvocationData" TEXT NOT NULL,
    "Arguments" TEXT NOT NULL,
    "CreatedAt" TEXT NOT NULL,
    "ExpireAt" TEXT NULL
);

CREATE TABLE IF NOT EXISTS "State" (
    "Id" INTEGER PRIMARY KEY,
    "JobId" INTEGER NOT NULL,
    "Name" TEXT NOT NULL,
    "Reason" TEXT NULL,
    "CreatedAt" TEXT NOT NULL,
    "Data" TEXT NULL
);

CREATE INDEX IF NOT EXISTS "IX_HangFire_State_JobId" ON "State" ("JobId");

CREATE TABLE IF NOT EXISTS "JobParameter" (
    "JobId" INTEGER NOT NULL,
    "Name" TEXT NOT NULL,
    "Value" TEXT NULL,
    PRIMARY KEY ("JobId", "Name")
);

CREATE TABLE IF NOT EXISTS "JobQueue" (
    "Id" INTEGER PRIMARY KEY,
    "Queue" TEXT NOT NULL,
    "JobId" INTEGER NOT NULL,
    "FetchedAt" TEXT NULL
);

CREATE INDEX IF NOT EXISTS "IX_HangFire_JobQueue_Queue" ON "JobQueue" ("Queue", "FetchedAt");

CREATE TABLE IF NOT EXISTS "Server" (
    "Id" TEXT NOT NULL,
    "Data" TEXT NULL,
    "LastHeartbeat" TEXT NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS "Hash" (
    "Key" TEXT NOT NULL,
    "Field" TEXT NOT NULL,
    "Value" TEXT NULL,
    "ExpireAt" TEXT NULL,
    PRIMARY KEY ("Key", "Field")
);

CREATE TABLE IF NOT EXISTS "List" (
    "Key" TEXT NOT NULL,
    "Id" INTEGER NOT NULL DEFAULT (abs(random())),
    "Value" TEXT NULL,
    "ExpireAt" TEXT NULL,
    PRIMARY KEY ("Key", "Id")
);

CREATE TABLE IF NOT EXISTS "Set" (
    "Key" TEXT NOT NULL,
    "Value" TEXT NOT NULL,
    "Score" REAL NOT NULL DEFAULT 0.0,
    "ExpireAt" TEXT NULL,
    PRIMARY KEY ("Key", "Value")
);

CREATE TABLE IF NOT EXISTS "Counter" (
    "Key" TEXT NOT NULL,
    "Id" INTEGER NOT NULL DEFAULT (abs(random())),
    "Value" INTEGER NOT NULL,
    "ExpireAt" TEXT NULL,
    PRIMARY KEY ("Key", "Id")
);

CREATE TABLE IF NOT EXISTS "AggregatedCounter" (
    "Key" TEXT NOT NULL,
    "Value" INTEGER NOT NULL DEFAULT 0,
    "ExpireAt" TEXT NULL,
    PRIMARY KEY ("Key")
);

CREATE TABLE IF NOT EXISTS "hf_lock" (
    "resource"   TEXT    NOT NULL,
    "owner_id"   TEXT    NOT NULL,
    "expires_at" TEXT    NOT NULL,
    "version"    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY ("resource")
);

INSERT OR IGNORE INTO "Schema" ("Version") VALUES (9);
