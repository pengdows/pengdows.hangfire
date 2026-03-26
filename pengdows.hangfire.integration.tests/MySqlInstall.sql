-- MySQL schema install (also used by MariaDB, TiDB)
CREATE TABLE IF NOT EXISTS `Schema` (
    `Version` INT NOT NULL,
    PRIMARY KEY (`Version`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `Job` (
    `Id`             BIGINT NOT NULL AUTO_INCREMENT,
    `StateId`        BIGINT,
    `StateName`      VARCHAR(20),
    `InvocationData` LONGTEXT NOT NULL,
    `Arguments`      LONGTEXT NOT NULL,
    `CreatedAt`      DATETIME(6) NOT NULL,
    `ExpireAt`       DATETIME(6),
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `State` (
    `Id`        BIGINT NOT NULL AUTO_INCREMENT,
    `JobId`     BIGINT NOT NULL,
    `Name`      VARCHAR(20) NOT NULL,
    `Reason`    VARCHAR(100),
    `CreatedAt` DATETIME(6) NOT NULL,
    `Data`      LONGTEXT,
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `JobParameter` (
    `JobId` BIGINT NOT NULL,
    `Name`  VARCHAR(40) NOT NULL,
    `Value` LONGTEXT,
    PRIMARY KEY (`JobId`, `Name`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `JobQueue` (
    `Id`        BIGINT NOT NULL AUTO_INCREMENT,
    `JobId`     BIGINT NOT NULL,
    `Queue`     VARCHAR(50) NOT NULL,
    `FetchedAt` DATETIME(6),
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `Server` (
    `Id`            VARCHAR(200) NOT NULL,
    `Data`          LONGTEXT,
    `LastHeartbeat` DATETIME(6) NOT NULL,
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `Hash` (
    `Key`      VARCHAR(100) NOT NULL,
    `Field`    VARCHAR(100) NOT NULL,
    `Value`    LONGTEXT,
    `ExpireAt` DATETIME(6),
    PRIMARY KEY (`Key`, `Field`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `List` (
    `Id`       BIGINT NOT NULL AUTO_INCREMENT,
    `Key`      VARCHAR(100) NOT NULL,
    `Value`    LONGTEXT,
    `ExpireAt` DATETIME(6),
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `Set` (
    `Key`      VARCHAR(100) NOT NULL,
    `Value`    VARCHAR(256) NOT NULL,
    `Score`    DOUBLE NOT NULL DEFAULT 0.0,
    `ExpireAt` DATETIME(6),
    PRIMARY KEY (`Key`, `Value`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `Counter` (
    `Id`       BIGINT NOT NULL AUTO_INCREMENT,
    `Key`      VARCHAR(100) NOT NULL,
    `Value`    INT NOT NULL,
    `ExpireAt` DATETIME(6),
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `AggregatedCounter` (
    `Key`      VARCHAR(100) NOT NULL,
    `Value`    BIGINT NOT NULL DEFAULT 0,
    `ExpireAt` DATETIME(6),
    PRIMARY KEY (`Key`)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS `hf_lock` (
    `resource`   VARCHAR(100) NOT NULL,
    `owner_id`   VARCHAR(40) NOT NULL,
    `expires_at` DATETIME(6) NOT NULL,
    `version`    INT NOT NULL DEFAULT 1,
    PRIMARY KEY (`resource`)
) ENGINE=InnoDB;
INSERT IGNORE INTO `Schema` (`Version`) VALUES (10);
