-- DuckDB schema install
-- DuckDB 1.x does not support GENERATED ALWAYS AS IDENTITY; uses sequences instead.
CREATE SCHEMA IF NOT EXISTS "HangFire";
CREATE SEQUENCE IF NOT EXISTS "HangFire".job_seq START 1;
CREATE SEQUENCE IF NOT EXISTS "HangFire".state_seq START 1;
CREATE SEQUENCE IF NOT EXISTS "HangFire".jobqueue_seq START 1;
CREATE SEQUENCE IF NOT EXISTS "HangFire".list_seq START 1;
CREATE SEQUENCE IF NOT EXISTS "HangFire".counter_seq START 1;
CREATE TABLE IF NOT EXISTS "HangFire"."Schema" (
    "Version" INT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS "HangFire"."Job" (
    "Id"             BIGINT DEFAULT nextval('"HangFire".job_seq') PRIMARY KEY,
    "StateId"        BIGINT,
    "StateName"      VARCHAR(20),
    "InvocationData" TEXT NOT NULL,
    "Arguments"      TEXT NOT NULL,
    "CreatedAt"      TIMESTAMPTZ NOT NULL,
    "ExpireAt"       TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS "HangFire"."State" (
    "Id"        BIGINT DEFAULT nextval('"HangFire".state_seq') PRIMARY KEY,
    "JobId"     BIGINT NOT NULL,
    "Name"      VARCHAR(20) NOT NULL,
    "Reason"    VARCHAR(100),
    "CreatedAt" TIMESTAMPTZ NOT NULL,
    "Data"      TEXT);
CREATE TABLE IF NOT EXISTS "HangFire"."JobParameter" (
    "JobId" BIGINT NOT NULL,
    "Name"  VARCHAR(40) NOT NULL,
    "Value" TEXT,
    PRIMARY KEY ("JobId", "Name"));
CREATE TABLE IF NOT EXISTS "HangFire"."JobQueue" (
    "Id"        BIGINT DEFAULT nextval('"HangFire".jobqueue_seq') PRIMARY KEY,
    "JobId"     BIGINT NOT NULL,
    "Queue"     VARCHAR(50) NOT NULL,
    "FetchedAt" TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS "HangFire"."Server" (
    "Id"            VARCHAR(200) PRIMARY KEY,
    "Data"          TEXT,
    "LastHeartbeat" TIMESTAMPTZ NOT NULL);
CREATE TABLE IF NOT EXISTS "HangFire"."Hash" (
    "Key"      VARCHAR(100) NOT NULL,
    "Field"    VARCHAR(100) NOT NULL,
    "Value"    TEXT,
    "ExpireAt" TIMESTAMPTZ,
    PRIMARY KEY ("Key", "Field"));
CREATE TABLE IF NOT EXISTS "HangFire"."List" (
    "Id"       BIGINT DEFAULT nextval('"HangFire".list_seq') PRIMARY KEY,
    "Key"      VARCHAR(100) NOT NULL,
    "Value"    TEXT,
    "ExpireAt" TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS "HangFire"."Set" (
    "Key"      VARCHAR(100) NOT NULL,
    "Value"    VARCHAR(256) NOT NULL,
    "Score"    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    "ExpireAt" TIMESTAMPTZ,
    PRIMARY KEY ("Key", "Value"));
CREATE TABLE IF NOT EXISTS "HangFire"."Counter" (
    "Id"       BIGINT DEFAULT nextval('"HangFire".counter_seq') PRIMARY KEY,
    "Key"      VARCHAR(100) NOT NULL,
    "Value"    INT NOT NULL,
    "ExpireAt" TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS "HangFire"."AggregatedCounter" (
    "Key"      VARCHAR(100) PRIMARY KEY,
    "Value"    BIGINT NOT NULL DEFAULT 0,
    "ExpireAt" TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS "HangFire"."hf_lock" (
    "resource"   VARCHAR(100) PRIMARY KEY,
    "owner_id"   VARCHAR(40) NOT NULL,
    "expires_at" TIMESTAMPTZ NOT NULL,
    "version"    INT NOT NULL DEFAULT 1);
INSERT INTO "HangFire"."Schema" ("Version") VALUES (10) ON CONFLICT DO NOTHING;
