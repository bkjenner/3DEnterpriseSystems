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
This script sets up subnet sn00003 for the stage 1 test environment.
---------------------------------------
Instructions

1. Connect to Postgres

2. If there are active sessions, you can terminate them with the following statement
*/
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000003;
CREATE DATABASE sn000003 WITH TEMPLATE sn000001 OWNER postgres;
ALTER DATABASE sn000003 SET search_path TO S0021V0000, S0000V0000, public;

/*
3. Close all sessions. This is necessary because the default search path will not
kick in until this is done.  If you don't do this you are going to find that the
import does not automatically run.

Switch to sn000003

4. Run the following script
*/
SET SEARCH_PATH TO PUBLIC;
CREATE EXTENSION IF NOT EXISTS dblink;
SET search_path TO s0001v0000, s0000v0000, public;

CALL spsysschemacreate(
        p_firstname := '',
        p_lastname := '',
        p_username := '',
        p_email := '',
        p_organizationname := 'Subnet Server3',
        p_systemname := 'System 20',
        p_IsSubnetServer := TRUE,
        p_exSystemID:=20);

CALL spsysschemacreate(
        p_firstname := '',
        p_lastname := '',
        p_username := '',
        p_email := '',
        p_organizationname := 'Organization 21',
        p_systemname := 'System 21',
        p_exSystemID:=21);

-- Delete glrate because we use it for subscription testing
DELETE from s0020v0000.glratetype;
DELETE from s0021v0000.glratetype;
DELETE from s0020v0000.glrate;
DELETE from s0021v0000.glrate;

SET SEARCH_PATH TO s0020v0000, s0000v0000, public;

insert into exSubnetServer (id, name, systemidSubnetServer, rowstatus, syschangehistoryid)
select id, name, systemidSubnetServer, rowstatus, syschangehistoryid from vwexSubnetServeraws a
where not exists (select from exSubnetServer aa where aa.id=a.id);

insert into exsystem (id, exSubnetServerid, name, schemaname, rowstatus, syschangehistoryid)
select id, exSubnetServerid, name, schemaname, rowstatus, syschangehistoryid from vwexsystemaws a
where not exists (select from exsystem aa where aa.id=a.id);

DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
CREATE TRIGGER exHistoryCallImport
    AFTER INSERT
    ON exHistory
    FOR EACH STATEMENT
EXECUTE PROCEDURE trexHistoryImportCall();

SET SEARCH_PATH TO s0021v0000, s0000v0000, public;

insert into exSubnetServer (id, name, systemidSubnetServer, rowstatus, syschangehistoryid)
select id, name, systemidSubnetServer, rowstatus, syschangehistoryid from vwexSubnetServeraws a
where not exists (select from exSubnetServer aa where aa.id=a.id);

insert into exsystem (id, exSubnetServerid, name, schemaname, rowstatus, syschangehistoryid)
select id, exSubnetServerid, name, schemaname, rowstatus, syschangehistoryid from vwexsystemaws a
where not exists (select from exsystem aa where aa.id=a.id);

DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;

CREATE TRIGGER exHistoryCallImport
    AFTER INSERT
    ON exHistory
    FOR EACH STATEMENT
EXECUTE PROCEDURE trexHistoryImportCall();

ALTER DATABASE sn000003 SET search_path TO S0021V0000, S0000V0000, public;

DROP SCHEMA IF EXISTS s0001v0000 CASCADE;
DROP SCHEMA IF EXISTS s0002v0000 CASCADE;
DROP SCHEMA IF EXISTS s0005v0000 CASCADE;
DROP SCHEMA IF EXISTS s0006v0000 CASCADE;
DROP SCHEMA IF EXISTS s0007v0000 CASCADE;
DROP SCHEMA IF EXISTS _Staging CASCADE;

/*
5. Switch to postgres

6. Run the following script
*/
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000003bak;
CREATE DATABASE sn000003bak WITH TEMPLATE sn000003 OWNER postgres;

*/