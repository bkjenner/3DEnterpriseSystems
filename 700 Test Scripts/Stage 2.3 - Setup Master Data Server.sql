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
This script sets up the master data server for the proof of concept environment.
---------------------------------------
Instructions

  CREATE SCHEMA IF NOT EXISTS s0000v0000;

*/

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
VALUES (10, 'sn000010', 90, 'a')
, (11, 'sn000011', 91, 'a')
, (12, 'sn000012', 92, 'a')
, (13, 'sn000013', 93, 'a')
, (14, 'sn000014', 94, 'a');
-- INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
-- VALUES (11, 'sn000011', 109, 'a');

INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
VALUES 
  (90, 10, 'Subnet server 10', 's0090v0000', 0, 0, 'a')
, (100, 10, 'Executive Office', 's0100v0000', 0, 0, 'a')
, (101, 10, 'Citizen Registry', 's0101v0000', 0, 0, 'a')
, (102, 10, 'Corporate Registry', 's0102v0000', 0, 0, 'a')
, (103, 10, 'Land Titles', 's0103v0000', 0, 0, 'a')
, (104, 10, 'Energy', 's0104v0000', 0, 0, 'a')
, (105, 10, 'Municipal Affairs', 's0105v0000', 0, 0, 'a')
, (106, 10, 'Municipal District #1', 's0106v0000', 0, 0, 'a')
, (107, 10, 'Municipal District #2', 's0107v0000', 0, 0, 'a')
, (108, 10, 'Vehicle Registry', 's0108v0000', 0, 0, 'a')
, (109, 10, 'Public Works', 's0109v0000', 0, 0, 'a')
, (110, 10, 'ABC Consulting Company', 's0110v0000', 0, 0, 'a')
, (120, 10, 'Bank of Fairfax', 's0120v0000', 0, 0, 'a');

DROP TABLE IF EXISTS exgovernancedetail CASCADE;
DROP TABLE IF EXISTS exgovernance CASCADE;
DROP TABLE IF EXISTS exrecordgroup CASCADE;
DROP TABLE sysdictionarytable CASCADE;

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
    transferdate       TIMESTAMP,
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
