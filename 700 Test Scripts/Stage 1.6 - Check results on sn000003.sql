/*
To setup for testing without rebuilding.

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000003;
CREATE DATABASE sn000003 WITH TEMPLATE sn000003bak OWNER postgres;

ALTER DATABASE sn000003 SET search_path TO S0021V0000, S0000V0000, public;

*/

--Test 2

SET SEARCH_PATH TO s0021v0000, s0000v0000, public;

select 's0021v0000' SystemName, UID, Name, crmgenderid, sysCHangeHistoryID from s0021v0000.vwcrmcontact where id=fnsysIDGet(1, 128);
select 's0021v0000' SystemName, UID, Address1, sysCHangeHistoryID from vwcrmaddress where crmcontactid=fnsysIDGet(1, 128);
--Test 3
select * from s0021v0000.vwglratetype;
--Test 4
select * from s0021v0000.vwglrate order by id, temporalstartdate;

--Test 7
select 's0021v0000' SystemName, UID, Name, crmgenderid, sysCHangeHistoryID from s0021v0000.vwcrmcontact where id=fnsysIDGet(1, 128);
select 's0021v0000' SystemName, UID, Address1, sysCHangeHistoryID from s0021v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128);

--Test 8
select * from s0021v0000.vwglrate where id=3 order by temporalstartdate;

--Test 9
select * from s0021v0000.vwglrate where id=1;

--Test 10
select * from s0021v0000.vwglrate where id=1;


