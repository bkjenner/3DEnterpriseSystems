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
This script sets up subnet sn00002 for the stage 1 test environment.
---------------------------------------
Instructions

1. Connect to Postgres

2. If there are active sessions, you can terminate them with the following statement.  Also make
sure sn00001 and sn000002 are not highlighted in datagrip or pgadmin.

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000001bak;
CREATE DATABASE sn000001bak WITH TEMPLATE sn000001 OWNER postgres;

DROP DATABASE if exists sn000002;
CREATE DATABASE sn000002 WITH TEMPLATE sn000001 OWNER postgres;

3. Connect to 'sn000002'

4. Run the following script

*/

SET SEARCH_PATH TO PUBLIC;
CREATE EXTENSION IF NOT EXISTS dblink;
-- This needs to be s0001v0000 because we are copying from it.
set search_path to s0001v0000, s0000v0000, public;

CALL spsysschemacreate(
        p_firstname := 'Pete',
        p_lastname := 'Peters',
        p_username := 'ppeters',
        p_email := '',
        p_organizationname := 'Organization 3 on sn2',
        p_systemname := 'System 3',
        p_IsSubnetServer := TRUE,
        p_exSystemID := 3
);

CALL spsysschemacreate(
        p_firstname := 'Wendy',
        p_lastname := 'Givens',
        p_username := 'wgivens',
        p_email := '',
        p_organizationname := 'Organization 4 on sn2',
        p_systemname := 'System 4',
        p_exSystemID := 4);

-- Delete glrate because we use it for subscription testing
DELETE from s0003v0000.glrate;
DELETE from s0004v0000.glrate;

SET SEARCH_PATH TO s0003v0000, s0000v0000, public;

DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
CREATE TRIGGER exHistoryCallImport
    AFTER INSERT
    ON exHistory
    FOR EACH STATEMENT
EXECUTE PROCEDURE trexHistoryImportCall();

SET SEARCH_PATH TO s0004v0000, s0000v0000, public;

DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
CREATE TRIGGER exHistoryCallImport
    AFTER INSERT
    ON exHistory
    FOR EACH STATEMENT
EXECUTE PROCEDURE trexHistoryImportCall();

ALTER DATABASE sn000002 SET search_path TO S0004V0000, S0000V0000, public;

DROP SCHEMA IF EXISTS _staging CASCADE;
DROP SCHEMA IF EXISTS s0001v0000 CASCADE;
DROP SCHEMA IF EXISTS s0002v0000 CASCADE;
DROP SCHEMA IF EXISTS s0005V0000 CASCADE;
DROP SCHEMA IF EXISTS s0006V0000 CASCADE;
DROP SCHEMA IF EXISTS s0007V0000 CASCADE;
DROP SCHEMA IF EXISTS s0008V0000 CASCADE;
/*
5. Connect to Postgres

6. Take backup of sn000002

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000002bak;
CREATE DATABASE sn000002bak WITH TEMPLATE sn000002 OWNER postgres;

*/
