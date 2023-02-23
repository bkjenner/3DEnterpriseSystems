/*This file is part of the 3D Enterprise System Platform. The 3D Enterprise System Platform is free
software: you can redistribute it and/or modify it under the terms of the GNU General Public
License as published by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
--
The 3D Enterprise System Platform is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
--
You should have received a copy of the GNU General Public License
along with the 3D Enterprise System Platform. If not, see <https://www.gnu.org/licenses/>.

Copyright (C) 2023  Blair Kjenner
OVERVIEW
This script sets up the Master Data Server for the stage 1 environment
*/
CREATE SCHEMA IF NOT EXISTS s0000v0000;

ALTER DATABASE sn000000 SET search_path TO s0000v0000;

SET search_path TO s0000v0000;

DROP TABLE IF EXISTS expackage CASCADE;
DROP TABLE IF EXISTS exSubnetServer CASCADE;
DROP TABLE IF EXISTS exsystem CASCADE;

CREATE TABLE IF NOT EXISTS exSubnetServer
(
    id                 BIGSERIAL
        CONSTRAINT exSubnetServer_pkey
            PRIMARY KEY,
    name               VARCHAR,
    systemidSubnetServer INTEGER,
    rowstatus          CHAR,
    syschangehistoryid BIGINT
);

CREATE TABLE exsystem
(
    id                 BIGSERIAL NOT NULL
        CONSTRAINT exsystem_pkey
            PRIMARY KEY,
    exSubnetServerid     BIGINT,
    name               VARCHAR,
    schemaname         VARCHAR,
    productionversion  INTEGER,
    testversion        INTEGER,
    subscriptionkey    VARCHAR,
    rowstatus          CHAR DEFAULT 'a'::bpchar,
    syschangehistoryid BIGINT
);

CREATE TABLE IF NOT EXISTS expackage
(
    id                 BIGSERIAL
        CONSTRAINT expackage_pkey
            PRIMARY KEY,
    exSubnetServerid     BIGINT,
    package            TEXT,
    readdate             timestamp,
    createdate timestamp,
    syschangehistoryid BIGINT
);

INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
VALUES (1, 'sn000001', 2, 'a');
INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
VALUES (2, 'sn000002', 3, 'a');
INSERT INTO s0000v0000.exSubnetServer(id, name, systemidSubnetServer, rowstatus)
VALUES (3, 'sn000003', 20, 'a');

INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
VALUES 
  (1, 1, 'Organization 1 on sn1', 's0001v0000', 0, 0, 'a')
, (2, 1, 'subnet server 1', 's0002v0000', 0, 0, 'a')
, (3, 2, 'subnet server 2', 's0003v0000', 0, 0, 'a')
, (4, 2, 'Organization 4 on sn2', 's0004v0000', 0, 0, 'a')
, (5, 1, 'Organization 5 on sn1', 's0005v0000', 0, 0, 'a')
, (6, 1, 'Organization 6 on sn1', 's0006v0000', 0, 0, 'a')
, (7, 1, 'Organization 7 on sn1', 's0007v0000', 0, 0, 'a')
, (20, 3, 'subnet server 3', 's0020v0000', 0, 0, 'a')
, (21, 3, 'Organization 21 on sn3', 's0021v0000', 0, 0, 'a');

SELECT SETVAL(PG_GET_SERIAL_SEQUENCE('exSubnetServer', 'id'), 3);
-- Even though I have systems 20 and 21, right now if I add one
-- on sn000001 I would prefer it was 8..19
SELECT SETVAL(PG_GET_SERIAL_SEQUENCE('exsystem', 'id'), 7);

CREATE TABLE IF NOT EXISTS sysdictionarytable
(
    id                    BIGINT  NOT NULL PRIMARY KEY,
    name                  VARCHAR NOT NULL,
    systemmodule          VARCHAR,
    syschangehistoryid    BIGINT
);

CREATE TABLE IF NOT EXISTS exrecordgroup
(
    id                    BIGINT  NOT NULL PRIMARY KEY,
    exrecordgroupidparent BIGINT
        CONSTRAINT fk_exrecordgroup_exrecordgroupidparent
            REFERENCES exrecordgroup,
    name                  VARCHAR NOT NULL,
    sysdictionarytableid  BIGINT
        CONSTRAINT fk_sysdictionarytable_sysdictionarytableid
            REFERENCES sysdictionarytable,
    syschangehistoryid    BIGINT
);

CREATE TABLE IF NOT EXISTS exgovernance
(
    id                 BIGINT                NOT NULL PRIMARY KEY,
    exsystemid         BIGINT                NOT NULL
        CONSTRAINT fk_exgovernance_exsystemid
            REFERENCES exsystem,
    exrecordgroupid    BIGINT                NOT NULL
        CONSTRAINT fk_exgovernance_exrecordgroupid
            REFERENCES exrecordgroup,
    rowidsubscribedto  BIGINT,
    transferdate       timestamp,
    syschangehistoryid BIGINT,
    CONSTRAINT exgovernance_exsystemid_exrecordgroupid_rowidsubscribedto_i_key
        UNIQUE (exsystemid, exrecordgroupid, rowidsubscribedto, transferdate)
);

CREATE TABLE IF NOT EXISTS exgovernancedetail
(
    id                               BIGINT NOT NULL PRIMARY KEY,
    exgovernanceid                   BIGINT NOT NULL
        CONSTRAINT fk_exgovernancedetail_exgovernanceid
            REFERENCES exgovernance,
    exsystemid                       BIGINT NOT NULL
        CONSTRAINT fk_exgovernancedetail_exsystemid
            REFERENCES exsystem,
    exrecordgroupid                  BIGINT NOT NULL
        CONSTRAINT fk_exgovernancedetail_exrecordgroupid
            REFERENCES exrecordgroup,
    sysdictionarytableidsubscribedto BIGINT NOT NULL
        CONSTRAINT fk_sysdictionarytable_sysdictionarytableid
            REFERENCES sysdictionarytable,
    rowidsubscribedto                BIGINT,
    syschangehistoryid               BIGINT,
    rowidmaster                      BIGINT
);

/*
select * from exsystem;
delete from exsystem where id>2;
SELECT SETVAL(PG_GET_SERIAL_SEQUENCE('exsystem', 'id'), MAX(id))
FROM exsystem;

create table exsystembak
as
select * from exsystem

create table exSubnetServerbak
as
select * from exSubnetServer


*/