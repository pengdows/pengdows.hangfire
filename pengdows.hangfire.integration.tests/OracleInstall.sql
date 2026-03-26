-- Oracle schema install (tables owned by the connecting user; no schema prefix)
-- Requires Oracle 23c (CREATE TABLE IF NOT EXISTS, GENERATED ALWAYS AS IDENTITY)
CREATE TABLE IF NOT EXISTS "Schema" (
    "Version" INTEGER NOT NULL,
    CONSTRAINT "PK_HangFire_Schema" PRIMARY KEY ("Version"));
CREATE TABLE IF NOT EXISTS "Job" (
    "Id"             NUMBER(19) GENERATED ALWAYS AS IDENTITY,
    "StateId"        NUMBER(19),
    "StateName"      VARCHAR2(20),
    "InvocationData" CLOB NOT NULL,
    "Arguments"      CLOB NOT NULL,
    "CreatedAt"      TIMESTAMP WITH TIME ZONE NOT NULL,
    "ExpireAt"       TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "PK_HangFire_Job" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "State" (
    "Id"        NUMBER(19) GENERATED ALWAYS AS IDENTITY,
    "JobId"     NUMBER(19) NOT NULL,
    "Name"      VARCHAR2(20) NOT NULL,
    "Reason"    VARCHAR2(100),
    "CreatedAt" TIMESTAMP WITH TIME ZONE NOT NULL,
    "Data"      CLOB,
    CONSTRAINT "PK_HangFire_State" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "JobParameter" (
    "JobId" NUMBER(19) NOT NULL,
    "Name"  VARCHAR2(40) NOT NULL,
    "Value" CLOB,
    CONSTRAINT "PK_HangFire_JobParameter" PRIMARY KEY ("JobId", "Name"));
CREATE TABLE IF NOT EXISTS "JobQueue" (
    "Id"        NUMBER(19) GENERATED ALWAYS AS IDENTITY,
    "JobId"     NUMBER(19) NOT NULL,
    "Queue"     VARCHAR2(50) NOT NULL,
    "FetchedAt" TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "PK_HangFire_JobQueue" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "Server" (
    "Id"            VARCHAR2(200) NOT NULL,
    "Data"          CLOB,
    "LastHeartbeat" TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT "PK_HangFire_Server" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "Hash" (
    "Key"      VARCHAR2(100) NOT NULL,
    "Field"    VARCHAR2(100) NOT NULL,
    "Value"    CLOB,
    "ExpireAt" TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "PK_HangFire_Hash" PRIMARY KEY ("Key", "Field"));
CREATE TABLE IF NOT EXISTS "List" (
    "Id"       NUMBER(19) GENERATED ALWAYS AS IDENTITY,
    "Key"      VARCHAR2(100) NOT NULL,
    "Value"    CLOB,
    "ExpireAt" TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "PK_HangFire_List" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "Set" (
    "Key"      VARCHAR2(100) NOT NULL,
    "Value"    VARCHAR2(256) NOT NULL,
    "Score"    BINARY_DOUBLE DEFAULT 0 NOT NULL,
    "ExpireAt" TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "PK_HangFire_Set" PRIMARY KEY ("Key", "Value"));
CREATE TABLE IF NOT EXISTS "Counter" (
    "Id"       NUMBER(19) GENERATED ALWAYS AS IDENTITY,
    "Key"      VARCHAR2(100) NOT NULL,
    "Value"    INTEGER NOT NULL,
    "ExpireAt" TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "PK_HangFire_Counter" PRIMARY KEY ("Id"));
CREATE TABLE IF NOT EXISTS "AggregatedCounter" (
    "Key"      VARCHAR2(100) NOT NULL,
    "Value"    NUMBER(19) DEFAULT 0 NOT NULL,
    "ExpireAt" TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "PK_HangFire_AggregatedCounter" PRIMARY KEY ("Key"));
CREATE TABLE IF NOT EXISTS "hf_lock" (
    "resource"   VARCHAR2(100) NOT NULL,
    "owner_id"   VARCHAR2(40) NOT NULL,
    "expires_at" TIMESTAMP WITH TIME ZONE NOT NULL,
    "version"    INTEGER DEFAULT 1 NOT NULL,
    CONSTRAINT "PK_HangFire_hf_lock" PRIMARY KEY ("resource"));
MERGE INTO "Schema" t USING (SELECT 10 AS v FROM DUAL) s ON (t."Version" = s.v) WHEN NOT MATCHED THEN INSERT ("Version") VALUES (s.v);
