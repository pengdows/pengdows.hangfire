-- PostgreSQL schema install (also used by CockroachDB, YugabyteDB)
CREATE SCHEMA IF NOT EXISTS "HangFire";
CREATE TABLE IF NOT EXISTS "HangFire"."Schema" (
    "Version" INT NOT NULL,
    CONSTRAINT "PK_HangFire_Schema" PRIMARY KEY ("Version"));
CREATE TABLE IF NOT EXISTS "HangFire"."Job" (
    "Id"             BIGINT GENERATED ALWAYS AS IDENTITY,
    "StateId"        BIGINT,
    "StateName"      VARCHAR(20),
    "InvocationData" TEXT NOT NULL,
    "Arguments"      TEXT NOT NULL,
    "CreatedAt"      TIMESTAMPTZ NOT NULL,
    "ExpireAt"       TIMESTAMPTZ,
    CONSTRAINT "PK_HangFire_Job" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "HangFire"."State" (
    "Id"        BIGINT GENERATED ALWAYS AS IDENTITY,
    "JobId"     BIGINT NOT NULL,
    "Name"      VARCHAR(20) NOT NULL,
    "Reason"    VARCHAR(100),
    "CreatedAt" TIMESTAMPTZ NOT NULL,
    "Data"      TEXT,
    CONSTRAINT "PK_HangFire_State" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "HangFire"."JobParameter" (
    "JobId" BIGINT NOT NULL,
    "Name"  VARCHAR(40) NOT NULL,
    "Value" TEXT,
    CONSTRAINT "PK_HangFire_JobParameter" PRIMARY KEY ("JobId", "Name"));
CREATE TABLE IF NOT EXISTS "HangFire"."JobQueue" (
    "Id"        BIGINT GENERATED ALWAYS AS IDENTITY,
    "JobId"     BIGINT NOT NULL,
    "Queue"     VARCHAR(50) NOT NULL,
    "FetchedAt" TIMESTAMPTZ,
    CONSTRAINT "PK_HangFire_JobQueue" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "HangFire"."Server" (
    "Id"            VARCHAR(200) NOT NULL,
    "Data"          TEXT,
    "LastHeartbeat" TIMESTAMPTZ NOT NULL,
    CONSTRAINT "PK_HangFire_Server" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "HangFire"."Hash" (
    "Key"      VARCHAR(100) NOT NULL,
    "Field"    VARCHAR(100) NOT NULL,
    "Value"    TEXT,
    "ExpireAt" TIMESTAMPTZ,
    CONSTRAINT "PK_HangFire_Hash" PRIMARY KEY ("Key", "Field"));
CREATE TABLE IF NOT EXISTS "HangFire"."List" (
    "Id"       BIGINT GENERATED ALWAYS AS IDENTITY,
    "Key"      VARCHAR(100) NOT NULL,
    "Value"    TEXT,
    "ExpireAt" TIMESTAMPTZ,
    CONSTRAINT "PK_HangFire_List" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "HangFire"."Set" (
    "Key"      VARCHAR(100) NOT NULL,
    "Value"    VARCHAR(256) NOT NULL,
    "Score"    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    "ExpireAt" TIMESTAMPTZ,
    CONSTRAINT "PK_HangFire_Set" PRIMARY KEY ("Key", "Value"));
CREATE TABLE IF NOT EXISTS "HangFire"."Counter" (
    "Id"       BIGINT GENERATED ALWAYS AS IDENTITY,
    "Key"      VARCHAR(100) NOT NULL,
    "Value"    INT NOT NULL,
    "ExpireAt" TIMESTAMPTZ,
    CONSTRAINT "PK_HangFire_Counter" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "HangFire"."AggregatedCounter" (
    "Key"      VARCHAR(100) NOT NULL,
    "Value"    BIGINT NOT NULL DEFAULT 0,
    "ExpireAt" TIMESTAMPTZ,
    CONSTRAINT "PK_HangFire_AggregatedCounter" PRIMARY KEY ("Key"));
CREATE TABLE IF NOT EXISTS "HangFire"."hf_lock" (
    "resource"   VARCHAR(100) NOT NULL,
    "owner_id"   VARCHAR(40) NOT NULL,
    "expires_at" TIMESTAMPTZ NOT NULL,
    "version"    INT NOT NULL DEFAULT 1,
    CONSTRAINT "PK_HangFire_hf_lock" PRIMARY KEY ("resource"));
INSERT INTO "HangFire"."Schema" ("Version") VALUES (10) ON CONFLICT DO NOTHING;
