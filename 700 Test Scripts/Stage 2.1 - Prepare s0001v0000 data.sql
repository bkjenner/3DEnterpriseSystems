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
This script randomizes and prepares the data from the stage 1 test environment for the
proof of concept environment.
---------------------------------------
Instructions

Prior to running this create the database and run all steps up to DataExchange 2 - Setup sn000001

-- Switch to Postgres

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000010;
CREATE DATABASE sn000010 WITH TEMPLATE sn000001 OWNER postgres;
ALTER DATABASE sn000010 SET search_path TO S0001V0000, S0000V0000, public
ALTER DATABASE sn000010 SET client_min_messages TO ERROR;
COMMENT ON DATABASE sn000010 IS 'Doreney Government';

-- Switch to sn000010
*/

DROP SCHEMA IF EXISTS _staging CASCADE;
DROP SCHEMA IF EXISTS s0002v0000 CASCADE;
DROP SCHEMA IF EXISTS s0005V0000 CASCADE;
DROP SCHEMA IF EXISTS s0006V0000 CASCADE;
DROP SCHEMA IF EXISTS s0007V0000 CASCADE;
DROP SCHEMA IF EXISTS s0008V0000 CASCADE;
DROP SCHEMA IF EXISTS s0009V0000 CASCADE;

SET search_path TO DEFAULT;
UPDATE glentry
SET glaccountid=1300
WHERE glaccountid = 10000000000000;
UPDATE glentry
SET glaccountid=1300
WHERE glaccountid = 10000000000002;
UPDATE glentry
SET glaccountid=1300
WHERE glaccountid = 10000000000001;
UPDATE glentry
SET glaccountid=10000000000004
WHERE glaccountid = 10000000000003;
UPDATE glentry
SET glaccountid=10000000000006
WHERE glaccountid = 10000000000007;
UPDATE glentry
SET glaccountid=10000000000006
WHERE glaccountid = 10000000000008;
UPDATE glentry
SET glaccountid=10000000000024
WHERE glaccountid = 10000000000011;
UPDATE glentry
SET glaccountid=10000000000025
WHERE glaccountid = 10000000000012;
UPDATE glentry
SET glaccountid=10000000000023
WHERE glaccountid = 10000000000016;
UPDATE glentry
SET glaccountid=10000000000024
WHERE glaccountid = 10000000000017;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000031;
UPDATE glentry
SET glaccountid=10000000000032
WHERE glaccountid = 10000000000033;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000034;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000035;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000039;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000040;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000041;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000042;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000043;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000044;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000047;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000048;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000050;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000051;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000052;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000053;
UPDATE glentry
SET glaccountid=10000000000030
WHERE glaccountid = 10000000000054;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 10000000000055;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 10000000000056;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 10000000000057;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 10000000000058;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 2300;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 10000000000059;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 10000000000060;
UPDATE glentry
SET glaccountid=2200
WHERE glaccountid = 10000000000061;
UPDATE glentry
SET glaccountid=10000000000075
WHERE glaccountid = 10000000000080;
UPDATE glentry
SET glaccountid=10000000000083
WHERE glaccountid = 10000000000074;
UPDATE glentry
SET glaccountid=10000000000083
WHERE glaccountid = 10000000000082;
UPDATE glentry
SET glaccountid=10000000000083
WHERE glaccountid = 10000000000084;
UPDATE glentry
SET glaccountid=10000000000075
WHERE glaccountid = 10000000000085;
UPDATE glentry
SET glaccountid=10000000000092
WHERE glaccountid = 10000000000111;

UPDATE fatype
SET glaccountidaccummulateddepreciation=10000000000024
WHERE glaccountidaccummulateddepreciation = 10000000000017;

UPDATE fatype
SET glaccountidasset=10000000000023
WHERE glaccountidasset = 10000000000016;

UPDATE acttype
SET glaccountid=NULL
WHERE glaccountid = 10000000000008;

UPDATE glbatch
SET glaccountid=NULL;
UPDATE glbillingaccount g
SET glaccountid=10000000000032
WHERE glaccountid IN (10000000000033, 10000000000053, 10000000000054);

TRUNCATE TABLE glaccountbalance;

DELETE
FROM glaccount
WHERE id IN
      (10000000000000, 10000000000002, 10000000000001, 10000000000003, 10000000000007, 10000000000008, 10000000000009,
       10000000000010, 10000000000011, 10000000000012, 10000000000016, 10000000000017, 10000000000031, 10000000000033,
       10000000000034, 10000000000035, 10000000000039, 10000000000040, 10000000000041, 10000000000042, 10000000000043,
       10000000000044, 10000000000047, 10000000000048, 10000000000050, 10000000000051, 10000000000052, 10000000000053,
       10000000000054, 10000000000055, 10000000000056, 10000000000057, 10000000000058, 2300, 10000000000059,
       10000000000060, 10000000000061, 10000000000080, 10000000000074, 10000000000082, 10000000000084, 10000000000085,
       10000000000111);
UPDATE glaccount
SET description = 'Investments'
WHERE id = 10000000000004;
UPDATE glaccount
SET description = 'Misc Payable'
WHERE id = 10000000000030;
UPDATE glaccount
SET description = 'Other Expenses'
WHERE id = 10000000000117;

UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000101;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 2600;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000103;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000104;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000105;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000106;
UPDATE glaccount
SET glaccountidparent= 10000000000073
WHERE id = 10000000000107;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000108;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000109;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000110;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000111;
UPDATE glaccount
SET glaccountidparent= 10000000000073
WHERE id = 10000000000112;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000113;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000114;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000115;
UPDATE glaccount
SET glaccountidparent= 10000000000117
WHERE id = 10000000000116;

SELECT fnsysHierarchyUpdate('glaccount', 'referencenumber');

DELETE
FROM crmrelationship
WHERE crmrelationshiptypeid IN (10, 30, 40, 50, 60, 80, 90);

UPDATE crmrelationship c
SET crmrelationshiptypeid=10, rowstatus='a';

UPDATE crmrelationshiptype
SET primaryname = 'Employer', secondaryname = 'Employee'
WHERE id = 10;
UPDATE crmrelationshiptype
SET primaryname = 'Parent', secondaryname = 'Child'
WHERE id = 20;
UPDATE crmrelationshiptype
SET primaryname = 'Board Chairman', secondaryname = 'Chairman of board'
WHERE id = 30;
UPDATE crmrelationshiptype
SET primaryname = 'Board Member', secondaryname = 'Member of board'
WHERE id = 40;
UPDATE crmrelationshiptype
SET primaryname = 'Spousal', secondaryname = 'Spousal'
WHERE id = 50;
UPDATE crmrelationshiptype
SET primaryname = 'Customer', secondaryname = 'Customer'
WHERE id = 60;
UPDATE crmrelationshiptype
SET primaryname = 'Educational Institute', secondaryname = 'Student'
WHERE id = 70;
UPDATE crmrelationshiptype
SET primaryname = 'Corporation CEO', secondaryname = 'CEO of Corporation'
WHERE id = 80;
UPDATE crmrelationshiptype
SET primaryname = 'Corporation Partner', secondaryname = 'Partner of Corporation'
WHERE id = 90;

DROP TABLE IF EXISTS aaIDConversion;
CREATE TEMP TABLE aaIDConversion
AS
SELECT a.*, c.ContactType, RANK() OVER ( ORDER BY (c.contacttype, a.id) ) NewID
FROM crmcontact a
JOIN LATERAL (SELECT EXISTS(SELECT FROM crmcontactsubtypeemployee aa WHERE aa.crmcontactid = a.id) IsEmployee) b
     ON TRUE
JOIN LATERAL (SELECT CASE WHEN b.isEmployee THEN 1
                          WHEN lastname IS NOT NULL THEN 2
                          ELSE 3
                          END contacttype) c
     ON TRUE;

CALL spsysChangeHistoryDisableTriggers();
CALL spsysForeignKeyConstraintGenerate(TRUE);
TRUNCATE TABLE crmcontact;
-- 104183 is the first contact in scrambled contacts that is not a person or oil company.  There are 1295 people in the db.
UPDATE aaIDConversion
SET newid=CASE WHEN lastname IS NOT NULL THEN fnsysIDGet(1, newid)
               ELSE fnsysIDGet(102, newid + 104183 - 1295)
               END;

INSERT INTO crmcontact (id, crmgenderid, crmlanguageid, crmsalutationid, birthdate, comments, contactnumber, contactperson, firstname, lastname, middlename, name, picture, preferredfirstname, previouslastname, website, rowstatus, syschangehistoryid)
SELECT NewID,
       crmgenderid,
       crmlanguageid,
       crmsalutationid,
       birthdate,
       NULL        comments,
       contactnumber,
       contactperson,
       firstname,
       lastname,
       middlename,
       name,
       picture,
       preferredfirstname,
       previouslastname,
       website,
       rowstatus,
       contacttype syschangehistoryid
FROM aaIDConversion;

UPDATE actactivity a
SET rowidperformedby=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidperformedby;
UPDATE actactivity a
SET rowidperformedfor=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidperformedfor;
UPDATE actactivitysubtypebilling a
SET crmcontactidinvoicethrough=b.newid
FROM aaIDConversion b
WHERE a.crmcontactidinvoicethrough = b.id;
UPDATE actproject a
SET crmcontactid=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid = b.id;
UPDATE actprojectresourceallocation a
SET crmcontactid=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid = b.id;
UPDATE actratebilling a
SET rowidchargedby=b.newid
FROM aaIDConversion b
WHERE a.rowidchargedby = b.id;
UPDATE actrateexpense a
SET rowidchargedby=b.newid
FROM aaIDConversion b
WHERE a.rowidchargedby = b.id;
UPDATE comattributedetail a
SET rowidappliesto=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidappliesto
  AND a.sysdictionarytableidappliesto = 100;
UPDATE compersonalcomment a
SET crmcontactiduser=b.newid
FROM aaIDConversion b
WHERE a.crmcontactiduser = b.id;
UPDATE crmaddress a
SET crmcontactid=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid = b.id;
UPDATE crmaddressemail a
SET crmcontactid=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid = b.id;
UPDATE crmaddressphone a
SET crmcontactid=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid = b.id;
UPDATE crmcontactsubtypeemployee
SET id=id + 10000000000000, crmcontactid=crmcontactid + 10000000000000;
UPDATE crmcontactsubtypeemployee a
SET id=b.newid
FROM aaIDConversion b
WHERE b.id = a.id - 10000000000000;
UPDATE crmcontactsubtypeemployee a
SET id=crmcontactid;
UPDATE crmcontactsubtypeuser
SET id=id + 10000000000000, crmcontactid=crmcontactid + 10000000000000;
UPDATE crmcontactsubtypeuser a
SET id=b.newid
FROM aaIDConversion b
WHERE b.id = a.id - 10000000000000;
UPDATE crmcontactsubtypeuser a
SET id=crmcontactid;
UPDATE crmrelationship a
SET crmcontactid1=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid1 = b.id;
UPDATE crmrelationship a
SET crmcontactid2=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid2 = b.id;
UPDATE falocationhistory a
SET crmcontactidassignedto=b.newid
FROM aaIDConversion b
WHERE a.crmcontactidassignedto = b.id;
UPDATE glbillingaccount a
SET crmcontactid=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid = b.id;
UPDATE gldeposit a
SET crmcontactid=b.newid
FROM aaIDConversion b
WHERE a.crmcontactid = b.id;
UPDATE glentry a
SET rowidchargedto=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidchargedto
  AND a.sysdictionarytableidchargedto = 100;
UPDATE glsetup a
SET crmcontactidcompany=b.newid
FROM aaIDConversion b
WHERE a.crmcontactidcompany = b.id;
UPDATE gltransactionsubtypecashreceipt a
SET crmcontactidenteredby=b.newid
FROM aaIDConversion b
WHERE a.crmcontactidenteredby = b.id;
UPDATE gltransactionsubtypecashreceipt a
SET crmcontactidpaidfor=b.newid
FROM aaIDConversion b
WHERE a.crmcontactidpaidfor = b.id;
UPDATE hrposition a
SET crmcontactidemployee=b.newid
FROM aaIDConversion b
WHERE a.crmcontactidemployee = b.id;
UPDATE syschangehistory a
SET crmcontactiduser=b.newid
FROM aaIDConversion b
WHERE a.crmcontactiduser = b.id;
UPDATE syschangehistory a
SET rowidappliesto=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidappliesto
  AND a.sysdictionarytableidappliesto = 100;

UPDATE actactivity
SET acttypeid=10000000000004
WHERE acttypeid = 10000000000022;
UPDATE actactivity
SET acttypeid=10000000000027
WHERE acttypeid = 10000000000028;
UPDATE actactivity
SET acttypeid=10000000000007
WHERE acttypeid = 10000000000036;
UPDATE actactivity
SET acttypeid=10000000000004
WHERE acttypeid = 10000000000002;
UPDATE actactivity
SET acttypeid=10000000000040
WHERE acttypeid IN
      (10000000000035, 10000000000043, 10000000000042, 10000000000014, 10000000000003, 10000000000045, 10000000000046,
       10000000000038, 10000000000012, 10000000000013, 10000000000016, 10000000000011, 10000000000019, 10000000000041,
       10000000000021, 10000000000017);

UPDATE actratebilling
SET acttypeid=10000000000004
WHERE acttypeid = 10000000000022;
UPDATE actratebilling
SET acttypeid=10000000000004
WHERE acttypeid = 10000000000002;
UPDATE actratebilling
SET acttypeid=10000000000040
WHERE acttypeid IN
      (10000000000035, 10000000000043, 10000000000042, 10000000000014, 10000000000003, 10000000000045, 10000000000046,
       10000000000038, 10000000000012, 10000000000013, 10000000000016, 10000000000011, 10000000000019, 10000000000041,
       10000000000021, 10000000000017);

UPDATE actrateexpense
SET acttypeid=10000000000004
WHERE acttypeid = 10000000000022;
UPDATE actrateexpense
SET acttypeid=10000000000004
WHERE acttypeid = 10000000000002;
UPDATE actrateexpense
SET acttypeid=10000000000040
WHERE acttypeid IN
      (10000000000035, 10000000000043, 10000000000042, 10000000000014, 10000000000003, 10000000000045, 10000000000046,
       10000000000038, 10000000000012, 10000000000013, 10000000000016, 10000000000011, 10000000000019, 10000000000041,
       10000000000021, 10000000000017);

CALL spIDConversion('faFixedAsset', 'ID', 1);

UPDATE glentry a
SET rowidchargedto=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidchargedto
  AND a.sysdictionarytableidchargedto = 200;

UPDATE faLocationHistory a
SET rowidLocation=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidLocation
  AND a.sysdictionarytableidlocation = 200;

CALL spIDConversion('faLocationHistory', 'ID', 1);
CALL spIDConversion('faType', 'Id', 1);
CALL spIDConversion('glBatch', 'id', 1);
CALL spIDConversion('glTransaction', 'id', 1);
CALL spIDConversion('glTransactionSubTypeCashReceipt', 'id', 1);
CALL spIDConversion('glTransactionSubTypeCheque', 'id', 1);
CALL spIDConversion('glTransactionSubTypeInvoice', 'id', 1);
CALL spIDConversion('glEntry', 'id', 1);
CALL spIDConversion('actProject', 'Id', 1);

UPDATE glentry a
SET rowidchargedto=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidchargedto
  AND a.sysdictionarytableidchargedto = 401;
CALL spIDConversion('actProjectResourceAllocation', 'id', 1);
CALL spIDConversion('actRateBilling', 'id', 1);
CALL spIDConversion('actRateExpense', 'id', 1);
CALL spIDConversion('actRateFactor', 'id', 1);
CALL spIDConversion('actActivity', 'id', 1);
CALL spIDConversion('actActivitySubTypeBilling', 'id', 1);
CALL spIDConversion('glbillingaccount', 'id', 1);
UPDATE glentry a
SET rowidchargedto=b.newid
FROM aaIDConversion b
WHERE b.id = a.rowidchargedto
  AND a.sysdictionarytableidchargedto = 321;

UPDATE glentry
SET sysdictionarytableidchargedto=NULL
WHERE rowidchargedto IS NULL;
UPDATE glentry
SET rowidchargedto=NULL
WHERE sysdictionarytableidchargedto IS NULL;

update glentry a set description=b.description
FROM gltransaction b
WHERE a.gltransactionid=b.id
AND a.description is null;

-- I need to simplify the test data so the subprojects have the same performed for as the main project
DO
$$
    DECLARE
        l_updatecount INT;
        l_loopcount   INT := 0;
    BEGIN
        LOOP
            UPDATE actproject a
            SET crmcontactid=b.crmcontactid, syschangehistoryid= -1
            FROM actproject b
            WHERE a.actprojectidparent = b.id
              AND COALESCE(a.crmcontactid, -1) != COALESCE(b.crmcontactid, -1);
            GET DIAGNOSTICS l_updatecount = ROW_COUNT;
            IF l_updatecount = 0
            THEN
                RETURN;
            END IF;
            l_loopcount := l_loopcount + 1;
            IF l_loopcount > 100
            THEN
                RAISE EXCEPTION 'Recursive loop on updating crmcontactid on actproject';
            END IF;
        END LOOP;
    END
$$ LANGUAGE plpgsql;

-- Then I need to update who the activity is performed for to align with the project
UPDATE actactivity a
SET rowidperformedfor=b.crmcontactid
FROM actproject b
WHERE b.id = a.actprojectid
  AND b.crmcontactid = a.rowidperformedfor;

-- There are a bunch of random charges to contacts (mostly GST on revenues) that I want to eliminate.
UPDATE glentry a
SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
WHERE glaccountid = 10000000000006
  AND a.sysdictionarytableidchargedto = 100;
