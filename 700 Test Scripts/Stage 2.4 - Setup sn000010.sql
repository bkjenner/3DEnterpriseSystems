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
This script sets up test environments for the proof of concept environment.
---------------------------------------
Instructions

Use the following script to restore the sn000010 from backup
in the event you need to rerun the script.

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000010;
  DROP DATAbase sn000010temp;
CREATE DATABASE sn000010 WITH TEMPLATE sn000010origbak OWNER postgres;
ALTER DATABASE sn000010 SET search_path TO S0001V0000, S0000V0000, public;

truncate table pgagent.pga_jobsteplog cascade;
truncate table pgagent.pga_jobstep cascade;
truncate table pgagent.pga_schedule cascade;
truncate table pgagent.pga_job cascade;

*/

ALTER DATABASE sn000010 SET search_path TO S0001V0000, S0000V0000, public;

SELECT PG_TERMINATE_BACKEND(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname LIKE 'sn%'
  AND datname != 'sn000000'
  AND pid <> PG_BACKEND_PID();

SELECT fnsysMDSExecute('do $temp$ begin set search_path to s0000v0000,public; truncate table expackage; UPDATE exsystem set exsubnetserverid=10; PERFORM SETVAL(PG_GET_SERIAL_SEQUENCE(''exsystem'', ''id''), 111, false); end $temp$ LANGUAGE plpgsql;');

SET client_min_messages TO error;
DO -- Create Data Server
$$
    BEGIN
        --*********************************************  Create Data Server ********************************************
        SET search_path TO s0001v0000, s0000v0000,public;

        IF NOT EXISTS(SELECT FROM vwexsystemaws WHERE id = 101)
        THEN
            RAISE EXCEPTION 'You need to run Stage 2.3 - Setup Master Data Server';
        END IF;

        DROP SCHEMA IF EXISTS s0090v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := '',
                p_lastname := '',
                p_username := '',
                p_email := '',
                p_organizationname := 'Subnet Server 90 for sn000010',
                p_systemname := 'System 90',
                p_IsSubnetServer := TRUE,
                p_exSystemID := 90,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0090v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

    END; -- Finish Data Server
$$ LANGUAGE plpgsql;
DO -- Create Citizen Registry (101)
$$
    DECLARE
        l_id              BIGINT;
        l_systemid        INT := 101;
        l_comlocationlast INT;
    BEGIN
        --**************************************  Create Citizen Registry (101) ***************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0101v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'John',
                p_lastname := 'Kristjanson',
                p_username := 'JKristjanson',
                p_email := 'JKristjanson@hotmail.com',
                p_organizationname := 'Doreney Citizen Registry (101)',
                p_systemname := 'Citizen Registry (101)',
                p_exSystemID := 101,
                p_Turnontriggersandconstraints := FALSE);

        SET SEARCH_PATH TO s0101v0000, s0000v0000, public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        -- Pick every 20th contact to be an employee and setup 20 employees
        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        -- setup hr positions and assign the employees to them
        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 1) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 1)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        INSERT INTO crmcontact (id, crmgenderid, birthdate, firstname, lastname, middlename, name, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               a.crmgenderid,
               a.birthdate,
               a.firstname,
               a.lastname,
               a.middlename,
               a.name,
               a.contactperson,
               'a',
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE ((a.lastname IS NOT NULL
            AND a.sysChangeHistoryID = 1
                   )
            OR fnsysidget(l_systemid, a.id) IN (
                                                   SELECT crmcontactid
                                                   FROM crmcontactsubtypeemployee))
          AND jl0.id NOT IN (
                                SELECT id
                                FROM crmcontact);

        -- Only get addresses for 101 because addresses for 102 contacts will get populated through subscriptions
        INSERT INTO crmaddress (id, crmcontactid, crmaddresstypeid, address1, address2, address3, city, comcityid, provincestate, postalzip, isprimaryaddress, rowstatus)
        WITH cityprov AS (
                             SELECT a.id cityid, a.name city, b.name province
                             FROM comcity a
                             JOIN comprovincestate b
                                  ON b.id = a.comprovincestateid)
        SELECT fnsysidget(l_systemid, a.id),
               fnsysidget(l_systemid, a.crmcontactid),
               b.id addresstype,
               a.address1,
               a.address2,
               a.address3,
               a.city,
               c.cityid,
               a.province,
               a.postalcode,
               FALSE,
               'a'  rowstatus
        FROM demodata.crmaddress a
        JOIN      crmaddresstype b
                  ON b.description = a.addresstype
        LEFT JOIN cityprov c
                  ON c.city = a.city AND c.province = a.province
        WHERE fnsysidget(l_systemid, crmcontactid) IN (
                                                          SELECT id
                                                          FROM crmcontact);

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               1000                           sysmultilinktableruleid, -- Connect to contacts
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Citizen Registration Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id = fnsysidget(100, 0);

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        -- changehistory id is used in a later step to assign locations
        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO crmrelationship (id, crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fnsysidget(l_systemid, crmcontactid1),
               fnsysidget(l_systemid, crmcontactid2),
               crmrelationshiptypeid,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM DemoData.crmrelationship
        WHERE sysChangeHistoryID = 1
          AND crmrelationshiptypeid IN (20, 50);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid, syschangehistoryid=999
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid, sysdictionarytableidchargedto=100, glaccountid=fnsysidget(l_systemid, 1),
            description=c.name || ' (' || fnsysidview(b.crmcontactid) || ') citizen registration fee',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             crmcontact c
        WHERE a.id = b.id
          AND c.id = b.crmcontactid;

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        PERFORM fnsysIDSetValAll();

        -- setup customer and employee relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               fnsysidget(l_systemid, crmcontactid1),
               60,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM DemoData.crmrelationship a
        WHERE crmrelationshiptypeid IN (20, 50)
          AND sysChangeHistoryID = 1
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        CALL spsysTemporalDataNormalize('crmrelationship');

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, 100, NULL, 'Contact', NULL, 1, 1, 'a', NULL)
        RETURNING id INTO l_id;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, 110, 11900, 'Address', NULL, 2, 2, 'a', NULL);

        INSERT INTO exsubscriber (exsystemid, name)
        SELECT id, name
        FROM exsystem
        WHERE id > 99
          AND id != l_systemid
          AND name NOT ILIKE 'dw%';

        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid
    END; -- Finish Citizen Registry (101)
$$ LANGUAGE plpgsql;
-- 100 is done after 101 because 100 adds subscriptions to 101's db
DO -- Setup Corporate (100)
$$
    DECLARE
        l_id              BIGINT;
        l_systemid        INT := 100;
        l_comlocationlast INT;
    BEGIN
        --**************************************  Setup Corporate (100) ***************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0100v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Blaire',
                p_lastname := 'Wendle',
                p_username := 'bwendle',
                p_email := 'bwendle@hotmail.com',
                p_organizationname := 'Doreney Executive Office (100)',
                p_systemname := 'Executive Office (100)',
                p_exSystemID := l_systemid,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0100v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, NAME, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre a;

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, crmgenderid, birthdate, firstname, lastname, middlename, name, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               crmgenderid,
               birthdate,
               firstname,
               lastname,
               middlename,
               name,
               contactperson,
               'a',
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT DISTINCT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE (sysdictionarytableidchargedto = 200
            AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto))
           OR sysdictionarytableidchargedto = 401;

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        -- setup employee relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        INSERT INTO exrecordgroup (sysdictionarytableid, name)
        VALUES (113, 'Contact Phone Type'),
               (114, 'Contact Address Type'),
               (121, 'Contact Relationship Type'),
               (130, 'Contact Gender'),
               (131, 'Contact Language'),
               (132, 'Contact Salutation'),
               (206, 'Fixed Asset Depreciation Method'),
               (207, 'Fixed Asset Disposal Reason'),
               (208, 'Fixed Asset Status'),
               (209, 'Fixed Asset Type'),
               (300, 'GL Account'),
               (301, 'GL Cost Centre'),
               (315, 'GL Transaction Type'),
               (317, 'GL Entry Type'),
               (323, 'GL Billing Account Status'),
               (341, 'GL Deposit Status'),
               (390, 'GL Batch Status'),
               (391, 'GL Batch Type'),
               (392, 'GL Billing Method'),
               (393, 'GLEFT Type'),
               (394, 'GL Payment Method'),
               (395, 'GL Posting Status'),
               (396, 'GL Rate'),
               (397, 'GL Rate Type'),
               (398, 'GL Account Type'),
               (400, 'Activity Type'),
               (403, 'Activity Billing Status'),
               (404, 'Activity Source'),
               (405, 'Activity Status'),
               (406, 'Activity Cost Unit'),
               (407, 'Activity Billing Method'),
               (408, 'Activity Priority'),
               (502, 'Country'),
               (503, 'Location'),
               (510, 'Attribute'),
               (521, 'Cross Reference Type'),
               (601, 'HR Employment Status'),
               (602, 'HR Grade'),
               (603, 'HR Position Classification'),
               (604, 'HR Position Type');

        -- We only want to share the generic activity types not the ones that are specific to a department
        UPDATE exrecordgroup SET whereclause='glcostcentreid is null' WHERE syschangehistoryid = 400;

        INSERT INTO exsubscriber (name)
        VALUES ('Doreney Departments')
        RETURNING id INTO l_id;

        INSERT INTO exsubscriber (exsubscriberidparent, exsystemid, name)
        SELECT l_id, id, name
        FROM exsystem
        WHERE id > 100
          AND name NOT ILIKE 'dw%';

        PERFORM fnsysHierarchyUpdate('exsubscriber', 'id');

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid)
        SELECT l_id, id
        FROM exrecordgroup
        WHERE id > fnsysidget(l_systemid, 1);

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid

    END; -- Finish Corporate (100)
$$ LANGUAGE plpgsql;

DO -- Create Corporate Registry (102)
$$
    DECLARE
        l_id              BIGINT;
        l_systemid        INT := 102;
        l_comlocationlast INT;

    BEGIN
        --**************************************  Create Corporate Registry (102) **************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0102v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Denis',
                p_lastname := 'Halep',
                p_username := 'DHalep',
                p_email := 'dhalep@shaw.ca',
                p_organizationname := 'Doreney Corporate Registry (102)',
                p_systemname := 'Corporate Registry (102)',
                p_exSystemID := l_systemid,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0102v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               1000                             sysmultilinktableruleid, -- Connect to contacts
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Corporate Registration Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id = fnsysidget(100, 0);

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO crmrelationship (id, crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fnsysidget(l_systemid, crmcontactid1),
               fnsysidget(101, crmcontactid2),
               crmrelationshiptypeid,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM DemoData.crmrelationship a
        WHERE sysChangeHistoryID = 1
          AND crmrelationshiptypeid = 80;

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');


        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT crmcontactid1
                            FROM crmrelationship a
                            UNION
                            SELECT crmcontactid2
                            FROM crmrelationship a
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321
                            UNION
                            -- System 100 and 101 may reference 102 contacts
                            -- that need to be added
                            SELECT ID crmContactID
                            FROM s0100v0000.crmcontact
                            WHERE fnsysidview(id, 's') = 102
                            UNION
                            SELECT ID crmContactID
                            FROM s0101v0000.crmcontact
                            WHERE fnsysidview(id, 's') = 102);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid, sysdictionarytableidchargedto=100, glaccountid=fnsysidget(l_systemid, 1),
            description=c.name || ' (' || fnsysidview(b.crmcontactid) || ') corporate registration fee',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             crmcontact c
        WHERE a.id = b.id
          AND c.id = b.crmcontactid;

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        -- Only get addresses for 102 because addresses for 101 contacts will get populated through subscriptions
        INSERT INTO crmaddress (id, crmcontactid, crmaddresstypeid, address1, address2, address3, city, comcityid, provincestate, postalzip, isprimaryaddress, rowstatus)
        WITH cityprov AS (
                             SELECT a.id cityid, a.name city, b.name province
                             FROM comcity a
                             JOIN comprovincestate b
                                  ON b.id = a.comprovincestateid)
        SELECT fnsysidget(l_systemid, a.id),
               fnsysidget(l_systemid, a.crmcontactid),
               b.id addresstype,
               a.address1,
               a.address2,
               a.address3,
               a.city,
               c.cityid,
               a.province,
               a.postalcode,
               FALSE,
               'a'  rowstatus
        FROM demodata.crmaddress a
        JOIN      crmaddresstype b
                  ON b.description = a.addresstype
        LEFT JOIN cityprov c
                  ON c.city = a.city AND c.province = a.province
        WHERE fnsysidget(l_systemid, crmcontactid) IN (
                                                          SELECT id
                                                          FROM crmcontact);

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        -- setup customer and employee relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               fnsysidget(l_systemid, crmcontactid1),
               60,
               '2000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM DemoData.crmrelationship a
        WHERE sysChangeHistoryID = 1
          AND a.crmrelationshiptypeid = 80
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, 100, NULL, 'Contact', NULL, 1, 1, 'a', NULL)
        RETURNING id INTO l_id;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, 110, 11900, 'Address', NULL, 2, 2, 'a', NULL);

        INSERT INTO exsubscriber (exsystemid, name)
        SELECT id, name
        FROM exsystem
        WHERE id > 99
          AND id != l_systemid
          AND name NOT ILIKE 'dw%';

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM exsubscriber
                   WHERE exsystemid = 100
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM s0100v0000.crmcontact
        WHERE fnsysIDView(ID, 's') = 102
        UNION ALL
        SELECT (
                   SELECT id
                   FROM exsubscriber
                   WHERE exsystemid = 101
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM s0101v0000.crmcontact
        WHERE fnsysIDView(ID, 's') = 102;

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid

    END; -- Finish Corporate Registry (102)
$$ LANGUAGE plpgsql;
DO -- Create Land Registry (103)
$$
    DECLARE
        l_id                      BIGINT;
        l_systemid                INT := 103;
        l_comlocationlast         INT;
        l_sysmultilinktableruleid BIGINT;
    BEGIN
        --**************************************  Create Land Registry (103) ***************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0103v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Donald',
                p_lastname := 'Morgan',
                p_username := 'DMorgan',
                p_email := 'dkmorgan@gmail.com',
                p_organizationname := 'Doreney Land Titles (103)',
                p_systemname := 'Land Titles (103)',
                p_exSystemID := l_systemid,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0103v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO sysdictionarytable (ID, Name, Translation, ChangeHistoryLevel, ChangeHistoryScope, Description, IsChangeHistoryUsed, IsTableTemporal, NormalizedName, SingularName, PluralName, SystemModule, TableType, RowStatus, sysChangeHistoryID)
        SELECT ID,
               Name,
               Translation,
               ChangeHistoryLevel,
               ChangeHistoryScope,
               Description,
               IsChangeHistoryUsed,
               IsTableTemporal,
               NormalizedName,
               SingularName,
               PluralName,
               SystemModule,
               TableType,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarytable
        WHERE fnsysIDView(ID, 's') = l_systemid
          AND id NOT IN (
                            SELECT id
                            FROM sysdictionarytable);

        INSERT INTO sysdictionarycolumn (ID, sysDictionaryTableID, Name, Label, ColumnSequence, DefaultValue, isnullable, datatype, datalength, decimals, purpose, sysDictionaryTableIDForeign, IsChangeHistoryUsed, RowStatus, sysChangeHistoryID)
        SELECT ID,
               sysDictionaryTableID,
               Name,
               Label,
               ColumnSequence,
               DefaultValue,
               isnullable,
               datatype,
               datalength,
               decimals,
               purpose,
               sysDictionaryTableIDForeign,
               IsChangeHistoryUsed,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarycolumn
        WHERE fnsysIDView(ID, 's') = l_systemid
          AND LOWER(name) != 'mttaxrollid'
          AND id NOT IN (
                            SELECT id
                            FROM sysdictionarycolumn);

        CALL spsysSchemaUpdate();

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               NULL, --1001                        sysmultilinktableruleid, -- Connect to land
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Land Registration Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id = fnsysidget(100, 0);

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO lrparcel (id, legaldescription, atslocation, acresgross, surveystatus, parceltype, ownership, acresunbroken, waterstatus, purpose, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id) id,
               legaldescription,
               atslocation,
               acresgross,
               surveystatus,
               parceltype,
               ownership,
               acresunbroken,
               waterstatus,
               purpose,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM demodata.lrparcel l
        WHERE syschangehistoryid = 1;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, fnsysidget(103, 2), NULL, 'Land Parcel', NULL, 1, 1, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, drilldowncolumn, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, fnsysidget(103, 4), fnsysidget(103, 38), 'Land Parcel->Title Holder', NULL,'crmcontactid', 1, 1, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, 100, 12260, 'Land Parcel->Land TitleHolder->Contact', NULL, 5, 3, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, 110, 11900, 'Land Parcel->Land TitleHolder->Contact->Address', 'crmaddresstypeid=50', 5, 3, 'a', NULL);

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, fnsysidget(103, 3), NULL, 'Land Interest', NULL, 3, 1, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, drilldowncolumn, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, fnsysidget(103, 1), fnsysidget(103, 4), 'Land Interest->Parcels', null, 'rowidappliesto', 4, 2, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, fnsysidget(103, 2), fnsysidget(103, 10), 'Land Interest->Parcel->ParcelDetail', NULL, 5, 3, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, drilldowncolumn, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, fnsysidget(103, 4), fnsysidget(103, 38), 'Land Interest->Parcel->ParcelDetail->TitleHolders', NULL, 'crmcontactid', 5, 3, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, 100, 12260, 'Land Interest->Parcel->ParcelDetail->TitleHolders->Contact', NULL, 5, 3, 'a', NULL)
        RETURNING id INTO l_id;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, 110, 11900, 'Land Interest->Parcel->ParcelDetail->TitleHolders->Contact->Address', 'crmaddresstypeid=50', 5, 3, 'a', NULL);

        PERFORM fnsysHierarchyUpdate('exrecordgroup', 'name');

        INSERT INTO exsubscriber (exsystemid, name)
        SELECT id, name
        FROM exsystem
        WHERE id > 99
          AND id != l_systemid
          AND name NOT ILIKE 'dw%';

        INSERT INTO lrinterest (id, crmcontactid, interestnumber, origin, interesttype, holderstatus, purpose, intereststatus, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id) id,
               CASE WHEN crmcontactid < 100000 THEN fnsysidget(101, crmcontactid)
                    ELSE fnsysidget(102, crmcontactid)
                    END                   crmcontactid,
               interestnumber,
               origin,
               interesttype,
               holderstatus,
               purpose,
               intereststatus,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM demodata.lrinterest l
        WHERE syschangehistoryid = 1;

        INSERT INTO lrparcelinterest (id, sysdictionarytableidappliesto, rowidappliesto, lrinterestid, acres, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id)             id,
               sysdictionarytableidappliesto,
               fnsysidget(l_systemid, rowidappliesto) lrparcelid,
               fnsysidget(l_systemid, lrinterestid)   lrinterestid,
               acres,
               temporalstartdate,
               COALESCE(temporalenddate, '9999-12-31'),
               'a',
               syschangehistoryid
        FROM demodata.lrparcelinterest l
        WHERE syschangehistoryid = 1;

        INSERT INTO lrparceltitleholder (lrparcelid, crmcontactid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, l.id) lrparcelid,
               CASE WHEN m.crmcontactid < 100000 THEN fnsysidget(101, m.crmcontactid)
                    ELSE fnsysidget(102, m.crmcontactid)
                    END                     crmcontactid,
               MIN(l.temporalstartdate),
               MIN(l.temporalstartdate),
               'a',
               1
        FROM demodata.lrparcel l
        JOIN demodata.mttaxroll m
             ON m.id = l.mttaxrollid AND m.temporalenddate = '9999-12-31'
        WHERE l.syschangehistoryid = 1
          AND l.temporalstartdate != '1000-01-01'
        GROUP BY crmcontactid, l.id
        UNION
        SELECT fnsysidget(l_systemid, l.id) lrparcelid,
               CASE WHEN r.crmcontactid2 < 100000 THEN fnsysidget(101, r.crmcontactid2)
                    END                     crmcontactid,
               MIN(l.temporalstartdate),
               MIN(l.temporalenddate),
               'a',
               2
        FROM demodata.lrparcel l
        JOIN demodata.mttaxroll m
             ON m.id = l.mttaxrollid AND m.temporalenddate = '9999-12-31'
        JOIN demodata.crmrelationship r
             ON r.crmcontactid1 = m.crmcontactid AND r.crmrelationshiptypeid = 50 AND
                r.temporalstartdate <= m.temporalstartdate AND r.temporalenddate >= m.temporalenddate
        WHERE l.syschangehistoryid = 1
          AND l.temporalstartdate != '1000-01-01'
        GROUP BY crmcontactid2, l.id;

        INSERT INTO lrplanparcel (id, lrparcelid, lrplanid, disposition, rowstatus)
        SELECT fnsysidget(l_systemid, id),
               fnsysidget(l_systemid, lrparcelid),
               fnsysidget(l_systemid, lrplanid),
               disposition,
               'a'
        FROM demodata.lrplanparcel
        WHERE fnsysidget(l_systemid, lrparcelid) IN (
                                                        SELECT id
                                                        FROM lrparcel);

        INSERT INTO lrplan (id, crmcontactid, plannumber, planstatus, plantype, planmethod, registrationdate, rowstatus)
        SELECT fnsysidget(l_systemid, id),
               CASE WHEN crmcontactid < 100000 THEN fnsysidget(101, crmcontactid)
                    ELSE fnsysidget(102, crmcontactid)
                    END crmcontactid,
               plannumber,
               planstatus,
               plantype,
               planmethod,
               registrationdate,
               'a'
        FROM demodata.lrplan
        WHERE fnsysidget(l_systemid, id) IN (
                                                SELECT lrplanid
                                                FROM lrplanparcel);

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);


        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT crmcontactid
                            FROM lrparceltitleholder
                            UNION
                            SELECT crmcontactid
                            FROM lrplan
                            UNION
                            SELECT crmcontactid
                            FROM lrinterest
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321);

        -- As a result of the title holder relationship begin created outside of the data preparation process there
        -- may be records that need to get added into crmcontact
        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id lrparcelid
                        FROM lrParcel aa
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.lrparcelid, sysdictionarytableidchargedto=fnsysidget(l_systemid, 2),
            glaccountid=fnsysidget(l_systemid, 1),
            description=c.legaldescription || ' (' || fnsysidview(c.id) || ') land title registration fee',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.lrparcelid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             lrparcel c
        WHERE a.id = b.id
          AND c.id = b.lrparcelid
          AND c.temporalenddate = '9999-12-31';

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid, syschangehistoryid=99
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        -- insert into customer and relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               ID,
               60,
               '1000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM crmcontact a
        WHERE fnsysIDView(ID, 's') != 103
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        INSERT INTO s0102v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0102v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 102;

        INSERT INTO s0102v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0102v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0102v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 102
          AND lastname IS NULL;

        CALL spsysTemporalDataNormalize(p_table := 'lrparcel');
        CALL spsysTemporalDataNormalize(p_table := 'lrinterest');
        CALL spsysTemporalDataNormalize(p_table := 'lrparcelinterest');
        CALL spsysTemporalDataNormalize(p_table := 'lrparceltitleholder');
        CALL spsysTemporalDataNormalize('hrposition');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid
    END; -- Finish Land Registry (103)
$$ LANGUAGE plpgsql;
DO --  Create Energy Dept (104)
$$
    DECLARE
        l_systemid        INT := 104;
        l_comlocationlast INT;
    BEGIN
        --**************************************  Create Energy Dept (104) *********************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0104v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Dick',
                p_lastname := 'Smith',
                p_username := 'DSmith',
                p_email := 'd.smith@hotmail.com',
                p_organizationname := 'Doreney Energy (104)',
                p_systemname := 'Energy (104)',
                p_exSystemID := 104,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0104v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO sysdictionarytable (ID, Name, Translation, ChangeHistoryLevel, ChangeHistoryScope, Description, IsChangeHistoryUsed, IsTableTemporal, NormalizedName, SingularName, PluralName, SystemModule, TableType, RowStatus, sysChangeHistoryID)
        SELECT ID,
               Name,
               Translation,
               ChangeHistoryLevel,
               ChangeHistoryScope,
               Description,
               IsChangeHistoryUsed,
               IsTableTemporal,
               NormalizedName,
               SingularName,
               PluralName,
               SystemModule,
               TableType,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarytable
        WHERE fnsysIDView(ID, 's') = l_systemid
            OR ID in (fnsysidget(103, 2), fnsysidget(103, 4));

        INSERT INTO sysdictionarycolumn (ID, sysDictionaryTableID, Name, Label, ColumnSequence, DefaultValue, isnullable, datatype, datalength, decimals, purpose, sysDictionaryTableIDForeign, IsChangeHistoryUsed, RowStatus, sysChangeHistoryID)
        SELECT ID,
               sysDictionaryTableID,
               Name,
               Label,
               ColumnSequence,
               DefaultValue,
               isnullable,
               datatype,
               datalength,
               decimals,
               purpose,
               sysDictionaryTableIDForeign,
               IsChangeHistoryUsed,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarycolumn
        WHERE (fnsysIDView(ID, 's') = l_systemid
            OR sysdictionarytableid in (fnsysidget(103, 2), fnsysidget(103, 4)))
          AND LOWER(name) != 'mttaxrollid';

        CALL spsysSchemaUpdate();

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               NULL, --1002                        sysmultilinktableruleid, -- Connect to contacts
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Well Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id = fnsysidget(100, 0);

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO lrparcel (id, legaldescription, atslocation, acresgross, surveystatus, parceltype, ownership, acresunbroken, waterstatus, purpose, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(103, id) id,
               legaldescription,
               atslocation,
               acresgross,
               surveystatus,
               parceltype,
               ownership,
               acresunbroken,
               waterstatus,
               purpose,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM demodata.lrparcel l
        WHERE id IN (
                        SELECT lrparcelid
                        FROM demodata.ogwell
                        WHERE syschangehistoryid = 1);

        INSERT INTO s0103v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT DISTINCT (
                            SELECT id
                            FROM s0103v0000.exsubscriber
                            WHERE exsystemid = l_systemid
                            ORDER BY id
                            LIMIT 1) exsubscriberid,
                        (
                            SELECT id
                            FROM s0103v0000.exrecordgroup
                            WHERE name ILIKE 'land parcel'
                            ORDER BY id
                            LIMIT 1) exrecordgroupid,
                        id
        FROM lrparcel;

        INSERT INTO ogwell (id, uwi, lrparcelid, wellstatus, statusdate, welltype, crmcontactidlicensee, crmcontactidoperator, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id)  id,
               uwi,
               fnsysidget(103, lrparcelid) lrparcelid,
               wellstatus,
               statusdate,
               welltype,
               CASE WHEN crmcontactidlicensee < 100000 THEN fnsysidget(101, crmcontactidlicensee)
                    ELSE fnsysidget(102, crmcontactidlicensee)
                    END                    crmcontactidlicensee,
               CASE WHEN crmcontactidoperator < 100000 THEN fnsysidget(101, crmcontactidoperator)
                    ELSE fnsysidget(102, crmcontactidoperator)
                    END                    crmcontactidoperator,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM demodata.ogwell
        WHERE syschangehistoryid = 1;

        INSERT INTO ogwellprod (id, ogwellid, productionyear, productdescription, productionvolume, inletgatheredvol, portionofproduced)
        SELECT fnsysidget(l_systemid, id)       id,
               fnsysidget(l_systemid, ogwellid) ogwellid,
               productionyear,
               productdescription,
               productionvolume,
               inletgatheredvol,
               portionofproduced
        FROM demodata.ogwellprod
        WHERE ogwellid IN (
                              SELECT id
                              FROM demodata.ogwell
                              WHERE syschangehistoryid = 1);

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT crmcontactidlicensee
                            FROM ogwell
                            UNION
                            SELECT crmcontactidoperator
                            FROM ogwell
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id ogwellid
                        FROM ogwell aa
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.ogwellid, sysdictionarytableidchargedto=fnsysidget(104, 1),
            glaccountid=fnsysidget(l_systemid, 1),
            description=c.uwi || ' (' || fnsysidview(b.ogwellid) || ') Oil Well Revenue',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.ogwellid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             ogwell c
        WHERE a.id = b.id
          AND c.id = b.ogwellid
          AND c.temporalenddate = '9999-12-31';

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        -- insert into customer and relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               ID,
               60,
               '1000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM crmcontact a
        WHERE fnsysIDView(ID, 's') != l_systemid
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        INSERT INTO s0102v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0102v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 102;

        INSERT INTO s0102v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0102v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0102v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 102
          AND lastname IS NULL;

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysTemporalDataNormalize(p_table := 'ogwell');
        CALL spsysTemporalDataNormalize(p_table := 'lrparcel');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid
    END; --  Finish Energy Dept (104)
$$ LANGUAGE plpgsql;
DO -- Create Municipal Affairs (105)
$$
    DECLARE
        l_systemid        INT := 105;
        l_comlocationlast INT;

    BEGIN
        --**************************************  Create Municipal Affairs (105) **************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0105v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Denis',
                p_lastname := 'Williams',
                p_username := 'DWilliams',
                p_email := 'dWilliams@shaw.ca',
                p_organizationname := 'Doreney Municipal Affairs (105)',
                p_systemname := 'Municipal Affairs (105)',
                p_exSystemID := 105,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0105v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO sysdictionarytable (ID, Name, Translation, ChangeHistoryLevel, ChangeHistoryScope, Description, IsChangeHistoryUsed, IsTableTemporal, NormalizedName, SingularName, PluralName, SystemModule, TableType, RowStatus, sysChangeHistoryID)
        SELECT ID,
               Name,
               Translation,
               ChangeHistoryLevel,
               ChangeHistoryScope,
               Description,
               IsChangeHistoryUsed,
               IsTableTemporal,
               NormalizedName,
               SingularName,
               PluralName,
               SystemModule,
               TableType,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarytable
        WHERE fnsysIDView(ID, 's') = 105
           OR ID = fnsysidget(103, 2);

        INSERT INTO sysdictionarycolumn (ID, sysDictionaryTableID, Name, Label, ColumnSequence, DefaultValue, isnullable, datatype, datalength, decimals, purpose, sysDictionaryTableIDForeign, IsChangeHistoryUsed, RowStatus, sysChangeHistoryID)
        SELECT ID,
               sysDictionaryTableID,
               Name,
               Label,
               ColumnSequence,
               DefaultValue,
               isnullable,
               datatype,
               datalength,
               decimals,
               purpose,
               sysDictionaryTableIDForeign,
               IsChangeHistoryUsed,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarycolumn
        WHERE fnsysIDView(ID, 's') = 105
           OR sysDictionaryTableID = fnsysidget(103, 2);

        CALL spsysSchemaUpdate();

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               NULL, --1003                    sysmultilinktableruleid, -- Connect to taxrolls
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Municipal Tax Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id IN (fnsysidget(100, 0), fnsysidget(100, 6));

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO mttaxroll (id, taxrollnumber, crmcontactid, status, type, disposition, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(106 + (id % 2)::INT, id) id,
               taxrollnumber,
               CASE WHEN crmcontactid < 100000 THEN fnsysidget(101, crmcontactid)
                    ELSE fnsysidget(102, crmcontactid)
                    END                            crmcontactidcrmcontactid,
               status,
               type,
               disposition,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM demodata.mttaxroll
        WHERE syschangehistoryid = 1;

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT crmcontactid
                            FROM mtTaxroll
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY fnsysidview(aa.id, 'r')) rowid, id mttaxrollid
                        FROM mttaxroll aa
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.mttaxrollid, sysdictionarytableidchargedto=fnsysidget(105, 1),
            glaccountid=fnsysidget(l_systemid, 1),
            description=c.taxrollnumber || ' (' || fnsysidview(b.mttaxrollid) || ') Property Tax Revenue',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.mttaxrollid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             mttaxroll c
        WHERE a.id = b.id
          AND c.id = b.mttaxrollid
          AND c.temporalenddate = '9999-12-31';

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE (sysdictionarytableidchargedto = 200
            AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto));

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        -- insert into customer and relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               ID,
               60,
               '1000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM crmcontact a
        WHERE fnsysIDView(ID, 's') != l_systemid
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        INSERT INTO s0102v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0102v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 102;

        INSERT INTO s0102v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0102v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0102v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 102
          AND lastname IS NULL;

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysTemporalDataNormalize(p_table := 'mttaxroll');
        CALL spsysTemporalDataNormalize(p_table := 'mtassessment');
        CALL spsysTemporalDataNormalize(p_table := 'lrparcel');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid
    END; -- Finish Municipal Affairs (105)
$$ LANGUAGE plpgsql;
DO -- Create Municipality #1 (106)
$$
    DECLARE
        l_systemid        INT := 106;
        l_comlocationlast INT;
        l_rgtaxrollid     BIGINT;
        l_rgparcelid      BIGINT;
        l_subscriberid    BIGINT;
    BEGIN
        --**************************************  Create Municipality #1 (106) **************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0106v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Donald',
                p_lastname := 'Edwards',
                p_username := 'DEdwards',
                p_email := 'dedwards@gmail.com',
                p_organizationname := 'Doreney Municipal District #1 (106)',
                p_systemname := 'Municipal District #1 (106)',
                p_exSystemID := 106,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0106v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO sysdictionarytable (ID, Name, Translation, ChangeHistoryLevel, ChangeHistoryScope, Description, IsChangeHistoryUsed, IsTableTemporal, NormalizedName, SingularName, PluralName, SystemModule, TableType, RowStatus, sysChangeHistoryID)
        SELECT ID,
               Name,
               Translation,
               ChangeHistoryLevel,
               ChangeHistoryScope,
               Description,
               IsChangeHistoryUsed,
               IsTableTemporal,
               NormalizedName,
               SingularName,
               PluralName,
               SystemModule,
               TableType,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarytable
        WHERE fnsysIDView(ID, 's') = 105
           OR ID in (fnsysidget(103, 2), fnsysidget(103, 4));

        INSERT INTO sysdictionarycolumn (ID, sysDictionaryTableID, Name, Label, ColumnSequence, DefaultValue, isnullable, datatype, datalength, decimals, purpose, sysDictionaryTableIDForeign, IsChangeHistoryUsed, RowStatus, sysChangeHistoryID)
        SELECT ID,
               sysDictionaryTableID,
               Name,
               Label,
               ColumnSequence,
               DefaultValue,
               isnullable,
               datatype,
               datalength,
               decimals,
               purpose,
               sysDictionaryTableIDForeign,
               IsChangeHistoryUsed,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarycolumn
        WHERE fnsysIDView(ID, 's') = 105
           OR sysDictionaryTableID in (fnsysidget(103, 2), fnsysidget(103, 4));

        CALL spsysSchemaUpdate();

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               NULL, --1003                    sysmultilinktableruleid, -- Connect to taxrolls
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Municipal Tax Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id IN (fnsysidget(100, 0), fnsysidget(100, 6));

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO mttaxroll (id, taxrollnumber, crmcontactid, status, type, disposition, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id) id,
               taxrollnumber,
               CASE WHEN crmcontactid < 100000 THEN fnsysidget(101, crmcontactid)
                    ELSE fnsysidget(102, crmcontactid)
                    END                   crmcontactidcrmcontactid,
               status,
               type,
               disposition,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM demodata.mttaxroll
        WHERE id % 2 = 0
          AND syschangehistoryid = 1;

        INSERT INTO lrparcel (id, mttaxrollid, legaldescription, atslocation, acresgross, surveystatus, parceltype, ownership, acresunbroken, waterstatus, purpose, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(103, id)                 id,
               fnsysidget(l_systemid, mttaxrollid) mttaxrollid,
               legaldescription,
               atslocation,
               acresgross,
               surveystatus,
               parceltype,
               ownership,
               acresunbroken,
               waterstatus,
               purpose,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM demodata.lrparcel l
        WHERE fnsysidget(l_systemid, mttaxrollid) IN (
                                                         SELECT id
                                                         FROM mttaxroll);

        INSERT INTO mtassessment (id, lrparcelid, land, building, assessmentdate, total, landuse, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id) id,
               fnsysidget(103, a.lrparcelid),
               a.land,
               a.building,
               a.assessmentdate,
               a.total,
               a.landuse,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.mtassessment a
        WHERE fnsysidget(103, a.lrparcelid) IN (
                                                   SELECT id
                                                   FROM lrparcel);

        INSERT INTO s0103v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT DISTINCT (
                            SELECT id
                            FROM s0103v0000.exsubscriber
                            WHERE exsystemid = 106
                            ORDER BY id
                            LIMIT 1) exsubscriberid,
                        (
                            SELECT id
                            FROM s0103v0000.exrecordgroup
                            WHERE name ILIKE 'land parcel'
                            ORDER BY id
                            LIMIT 1) exrecordgroupid,
                        id
        FROM lrparcel;

        -- Set up subscriptions to taxrolls, parcels and parcel assessments for Municipal Affairs;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, 1050000000000001, NULL, 'Taxroll', NULL, 1, 1, 'a', NULL)
        RETURNING id INTO l_rgtaxrollid;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_rgtaxrollid, 1030000000000002, 1030000000000013, 'Parcel', NULL, 2, 2, 'a', NULL)
        RETURNING id INTO l_rgparcelid;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_rgparcelid, 1050000000000002, 1050000000000013, 'Assessment', NULL, 3, 3, 'a', NULL);

        INSERT INTO exsubscriber (exsystemid, name)
        SELECT id, name
        FROM exsystem
        WHERE id = 105
        RETURNING id INTO l_subscriberid;

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid)
        SELECT l_subscriberid, l_rgtaxrollid;

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT crmcontactid
                            FROM mtTaxroll
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id mttaxrollid
                        FROM mttaxroll aa
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.mttaxrollid, sysdictionarytableidchargedto=fnsysidget(105, 1),
            glaccountid=fnsysidget(l_systemid, 1),
            description=c.taxrollnumber || ' (' || fnsysidview(b.mttaxrollid) || ') Property Tax Revenue',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.mttaxrollid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             mttaxroll c
        WHERE a.id = b.id
          AND c.id = b.mttaxrollid
          AND c.temporalenddate = '9999-12-31';

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        -- insert into customer and relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(106, 2),
               ID,
               60,
               '1000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM crmcontact a
        WHERE fnsysIDView(ID, 's') != 106
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        INSERT INTO s0102v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0102v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 102;

        INSERT INTO s0102v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0102v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0102v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 102
          AND lastname IS NULL;

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysTemporalDataNormalize(p_table := 'mttaxroll');
        CALL spsysTemporalDataNormalize(p_table := 'mtassessment');
        CALL spsysTemporalDataNormalize(p_table := 'lrparcel');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid
    END; -- Finish Municipality #1 (106)
$$ LANGUAGE plpgsql;
DO -- Create Municipality #2 (107)
$$
    DECLARE
        l_systemid        INT := 107;
        l_comlocationlast INT;
        l_rgtaxrollid     BIGINT;
        l_rgparcelid      BIGINT;
        l_subscriberid    BIGINT;
    BEGIN
        --**************************************  Create Municipality #2 (107) **************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0107v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Betty',
                p_lastname := 'Coutts',
                p_username := 'bcoutts',
                p_email := 'bcoutts@gmail.com',
                p_organizationname := 'Doreney Municipal District #2 (107)',
                p_systemname := 'Municipal District #2 (107)',
                p_exSystemID := 107,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0107v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO sysdictionarytable (ID, Name, Translation, ChangeHistoryLevel, ChangeHistoryScope, Description, IsChangeHistoryUsed, IsTableTemporal, NormalizedName, SingularName, PluralName, SystemModule, TableType, RowStatus, sysChangeHistoryID)
        SELECT ID,
               Name,
               Translation,
               ChangeHistoryLevel,
               ChangeHistoryScope,
               Description,
               IsChangeHistoryUsed,
               IsTableTemporal,
               NormalizedName,
               SingularName,
               PluralName,
               SystemModule,
               TableType,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarytable
        WHERE fnsysIDView(ID, 's') = 105
           OR ID in (fnsysidget(103, 2), fnsysidget(103, 4));

        INSERT INTO sysdictionarycolumn (ID, sysDictionaryTableID, Name, Label, ColumnSequence, DefaultValue, isnullable, datatype, datalength, decimals, purpose, sysDictionaryTableIDForeign, IsChangeHistoryUsed, RowStatus, sysChangeHistoryID)
        SELECT ID,
               sysDictionaryTableID,
               Name,
               Label,
               ColumnSequence,
               DefaultValue,
               isnullable,
               datatype,
               datalength,
               decimals,
               purpose,
               sysDictionaryTableIDForeign,
               IsChangeHistoryUsed,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarycolumn
        WHERE fnsysIDView(ID, 's') = 105
           OR sysDictionaryTableID in (fnsysidget(103, 2), fnsysidget(103, 4));

        CALL spsysSchemaUpdate();

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT *
        FROM s0106v0000.glaccount
        WHERE id = fnsysidget(106, 1);-- Tax Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id IN (fnsysidget(100, 0), fnsysidget(100, 6));

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO mttaxroll (id, taxrollnumber, crmcontactid, status, type, disposition, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id) id,
               taxrollnumber,
               CASE WHEN crmcontactid < 100000 THEN fnsysidget(101, crmcontactid)
                    ELSE fnsysidget(102, crmcontactid)
                    END                   crmcontactidcrmcontactid,
               status,
               type,
               disposition,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM demodata.mttaxroll
        WHERE id % 2 = 1
          AND syschangehistoryid = 1;

        INSERT INTO lrparcel (id, mttaxrollid, legaldescription, atslocation, acresgross, surveystatus, parceltype, ownership, acresunbroken, waterstatus, purpose, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(103, id)                 id,
               fnsysidget(l_systemid, mttaxrollid) mttaxrollid,
               legaldescription,
               atslocation,
               acresgross,
               surveystatus,
               parceltype,
               ownership,
               acresunbroken,
               waterstatus,
               purpose,
               temporalstartdate,
               temporalenddate,
               'a',
               syschangehistoryid
        FROM demodata.lrparcel l
        WHERE fnsysidget(l_systemid, mttaxrollid) IN (
                                                         SELECT id
                                                         FROM mttaxroll);

        INSERT INTO mtassessment (id, lrparcelid, land, building, assessmentdate, total, landuse, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id) id,
               fnsysidget(103, a.lrparcelid),
               a.land,
               a.building,
               a.assessmentdate,
               a.total,
               a.landuse,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.mtassessment a
        WHERE fnsysidget(103, a.lrparcelid) IN (
                                                   SELECT id
                                                   FROM lrparcel);

        -- Set up subscriptions to taxrolls, parcels and parcel assessments for Municipal Affairs;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, 1050000000000001, NULL, 'Taxroll', NULL, 1, 1, 'a', NULL)
        RETURNING id INTO l_rgtaxrollid;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_rgtaxrollid, 1030000000000002, 1030000000000013, 'Parcel', NULL, 2, 2, 'a', NULL)
        RETURNING id INTO l_rgparcelid;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_rgparcelid, 1050000000000002, 1050000000000013, 'Assessment', NULL, 3, 3, 'a', NULL);

        INSERT INTO exsubscriber (exsystemid, name)
        SELECT id, name
        FROM exsystem
        WHERE id = 105
        RETURNING id INTO l_subscriberid;

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid)
        SELECT l_subscriberid, l_rgtaxrollid;

        INSERT INTO s0103v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT DISTINCT (
                            SELECT id
                            FROM s0103v0000.exsubscriber
                            WHERE exsystemid = 107
                            ORDER BY id
                            LIMIT 1) exsubscriberid,
                        (
                            SELECT id
                            FROM s0103v0000.exrecordgroup
                            WHERE name ILIKE 'land parcel'
                            ORDER BY id
                            LIMIT 1) exrecordgroupid,
                        id
        FROM lrparcel;

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT crmcontactid
                            FROM mtTaxroll
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id mttaxrollid
                        FROM mttaxroll aa
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.mttaxrollid, sysdictionarytableidchargedto=fnsysidget(105, 1),
            glaccountid=fnsysidget(106, 1),
            description=c.taxrollnumber || ' (' || fnsysidview(b.mttaxrollid) || ') Property Tax Revenue',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.mttaxrollid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             mttaxroll c
        WHERE a.id = b.id
          AND c.id = b.mttaxrollid
          AND c.temporalenddate = '9999-12-31';

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        -- insert into customer and relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(107, 2),
               ID,
               60,
               '1000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM crmcontact a
        WHERE fnsysIDView(ID, 's') != 107
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        INSERT INTO s0102v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0102v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 102;

        INSERT INTO s0102v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0102v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0102v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 102
          AND lastname IS NULL;

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysTemporalDataNormalize(p_table := 'mttaxroll');
        CALL spsysTemporalDataNormalize(p_table := 'mtassessment');
        CALL spsysTemporalDataNormalize(p_table := 'lrparcel');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid
        PERFORM fnsysIDSetValAll();
    END; -- Finish Municipality #2 (107)
$$ LANGUAGE plpgsql;
DO -- Create Vehicle Registry (108)
$$
    DECLARE
        l_id                      BIGINT;
        l_systemid                INT := 108;
        l_comlocationlast         INT;
        l_sysmultilinktableruleid BIGINT;
    BEGIN
        --**************************************  Create Vehicle Registry (108) ***************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0108v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'Kathy',
                p_lastname := 'Carsarefun',
                p_username := 'kcarsarefun',
                p_email := 'kcarsarefun@gmail.com',
                p_organizationname := 'Doreney Vehicle Registry (108)',
                p_systemname := 'Vehicle Registry (108)',
                p_exSystemID := l_systemid,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0108v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO sysdictionarytable (ID, Name, Translation, ChangeHistoryLevel, ChangeHistoryScope, Description, IsChangeHistoryUsed, IsTableTemporal, NormalizedName, SingularName, PluralName, SystemModule, TableType, RowStatus, sysChangeHistoryID)
        SELECT ID,
               Name,
               Translation,
               ChangeHistoryLevel,
               ChangeHistoryScope,
               Description,
               IsChangeHistoryUsed,
               IsTableTemporal,
               NormalizedName,
               SingularName,
               PluralName,
               SystemModule,
               TableType,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarytable
        WHERE fnsysIDView(ID, 's') = l_systemid
          AND id NOT IN (
                            SELECT id
                            FROM sysdictionarytable);

        INSERT INTO sysdictionarycolumn (ID, sysDictionaryTableID, Name, Label, ColumnSequence, DefaultValue, isnullable, datatype, datalength, decimals, purpose, sysDictionaryTableIDForeign, IsChangeHistoryUsed, RowStatus, sysChangeHistoryID)
        SELECT ID,
               sysDictionaryTableID,
               Name,
               Label,
               ColumnSequence,
               DefaultValue,
               isnullable,
               datatype,
               datalength,
               decimals,
               purpose,
               sysDictionaryTableIDForeign,
               IsChangeHistoryUsed,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarycolumn
        WHERE fnsysIDView(ID, 's') = l_systemid
          AND id NOT IN (
                            SELECT id
                            FROM sysdictionarycolumn);

        CALL spsysSchemaUpdate();

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               NULL, --1001                        sysmultilinktableruleid, -- Connect to Vehicle
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Vehicle Registration Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre
        WHERE referencenumber = l_systemid::VARCHAR
           OR id = fnsysidget(100, 0);

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype
        WHERE id < fnsysidget(100, 100);

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO vrvehicle
        SELECT *
        FROM demodata.vrvehicle;

        CALL spsysTemporalDataNormalize('vrvehicle');

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        -- As a result of the title holder relationship begin created outside of the data preparation process there
        -- may be records that need to get added into crmcontact
        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE a.id % 20 = l_systemid - 99
        ORDER BY a.id
        LIMIT 1000;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);


        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE jl0.id IN (
                            SELECT crmcontactid
                            FROM crmcontactsubtypeemployee
                            UNION
                            SELECT crmcontactid
                            FROM vrvehicle
                            UNION
                            SELECT DISTINCT rowidchargedto
                            FROM glentry
                            WHERE sysdictionarytableidchargedto = 100
                            UNION
                            SELECT DISTINCT bb.crmcontactid
                            FROM glentry aa
                            JOIN demodata.glbillingaccount bb
                                 ON bb.id = aa.rowidchargedto
                            WHERE aa.sysdictionarytableidchargedto = 321);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM crmcontactsubtypeemployee
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 401) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id vrvehicleid
                        FROM vrvehicle aa
                        WHERE temporalenddate = '9999-12-31')
        UPDATE glentry a
        SET rowidchargedto=b.vrvehicleid, sysdictionarytableidchargedto=fnsysidget(l_systemid, 2),
            glaccountid=fnsysidget(l_systemid, 1),
            description=c.make || ' ' || c.model || '(' || c.serialnumber || ') (' || fnsysidview(c.id) ||
                        ') Vehicle registration fee',
            syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.vrvehicleid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 401) b,
             vrvehicle c
        WHERE a.id = b.id
          AND c.id = b.vrvehicleid
          AND c.temporalenddate = '9999-12-31';

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                                            AND id NOT IN (
                                                              SELECT crmcontactid
                                                              FROM crmcontactsubtypeemployee)) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid, syschangehistoryid=99
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE a.id % 20 = l_systemid - 99
        LIMIT 100;

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=NULL;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedby) rowid, rowidperformedby crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedby
                                 FROM actactivity aaa
                                 WHERE rowidperformedby IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE actactivity a
        SET rowidperformedby=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedby = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidperformedfor) rowid, rowidperformedfor crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidperformedfor
                                 FROM actactivity aaa
                                 WHERE rowidperformedfor IS NOT NULL) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE aa.id NOT IN (fnsysidget(l_systemid, 1), fnsysidget(l_systemid, 2))
                          AND aa.id NOT IN (
                                               SELECT aaa.crmcontactid
                                               FROM crmcontactsubtypeemployee aaa))

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM actactivity aa
                 JOIN old bb
                      ON aa.rowidperformedfor = bb.crmcontactid
                 JOIN new cc
                      ON bb.rowid = cc.rowid) b
        WHERE a.id = b.id;

        UPDATE actactivity
        SET rowidperformedfor=fnsysidget(l_systemid, 2), sysdictionarytableidperformedfor=100
        WHERE rowidperformedfor IS NULL;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        PERFORM fnsysIDSetValAll();

        -- insert into customer and relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               ID,
               60,
               '1000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM crmcontact a
        WHERE fnsysIDView(ID, 's') != l_systemid
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        INSERT INTO s0102v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0102v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 102;

        INSERT INTO s0102v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0102v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0102v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 102
          AND lastname IS NULL;

        CALL spsysTemporalDataNormalize('hrposition');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid
    END; -- Finish Vehicle Registry (108)
$$ LANGUAGE plpgsql;
DO -- Public Works (109)
$$
    DECLARE
        l_id              BIGINT;
        l_systemid        INT := 109;
        l_comlocationlast INT;

    BEGIN
        --**************************************  Create Public Works (109) **************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0109v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'William',
                p_lastname := 'Tindall',
                p_username := 'wTindall',
                p_email := 'wtindall@shaw.ca',
                p_organizationname := 'Doreney Public Works (109)',
                p_systemname := 'Public Works (109)',
                p_exSystemID := l_systemid,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0109v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               1000                   sysmultilinktableruleid, -- Connect to contacts
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'Public Works Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        DELETE FROM glcostcentre;
        INSERT INTO glcostcentre
        SELECT *
        FROM demodata.glcostcentre;

        TRUNCATE TABLE fatype;
        INSERT INTO fatype
        SELECT *
        FROM demodata.fatype;

        TRUNCATE TABLE comlocation;
        INSERT INTO comLocation
        SELECT *
        FROM demodata.comlocation;

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        INSERT INTO hrposition (id, crmcontactidemployee, hrpositionidparent, commencementdate, workingtitle, temporalstartdate, temporalenddate, rowstatus)
        SELECT fnsysidget(100, 0) id,
               NULL,
               NULL,
               NULL,
               a.workingtitle,
               '1000-01-01',
               '9999-12-31',
               'a'
        FROM demodata.hrposition A
        WHERE id = 0
        UNION
        SELECT fnsysidget(l_systemid, b.id) id,
               a.crmcontactid,
               CASE WHEN b.hrpositionidparent IS NULL THEN fnsysidget(100, 0)
                    ELSE fnsysidget(l_systemid, b.hrpositionidparent)
                    END,
               a.temporalstartdate,
               b.workingtitle,
               a.temporalstartdate,
               a.temporalenddate,
               'a'
        FROM crmcontactsubtypeemployee a
        LEFT JOIN demodata.hrposition b
                  ON b.id = a.syschangehistoryid;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');
        CALL spsysTemporalDataNormalize('hrposition');

        SELECT MAX(syschangehistoryid) INTO l_comlocationlast FROM demodata.comlocation WHERE syschangehistoryid < 100;

        INSERT INTO fafixedasset (id, fastatusid, fatypeid, description, purchasedate, fixedassetnumber, serialnumber, make, modelnumber, modelyear, comments, depreciationlife, depreciationsalvagevalue, warrantyexpirydate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fastatusid,
               fatypeid,
               description,
               purchasedate,
               fnsysidview(fnsysidget(l_systemid, id)) fixedassetnumber,
               serialnumber,
               make,
               modelnumber,
               modelyear,
               comments,
               depreciationlife,
               depreciationsalvagevalue,
               warrantyexpirydate,
               'a'                                     rowstatus,
               RANK() OVER (ORDER BY id)               syschangehistoryid
        FROM demodata.fafixedasset
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 40;

        TRUNCATE TABLE falocationhistory;
        INSERT INTO falocationhistory (id, crmcontactidassignedto, glCostCentreIDOwnedBy, fafixedassetid, sysdictionarytableidlocation, rowidlocation, details, isactive, isconfirmed, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)           id,
               b.crmcontactid,
               e.id                                   glCostCentreIDOwnedBy,
               fnsysidget(l_systemid, fafixedassetid) fafixedassetid,
               503                                    sysdictionarytableidlocation,
               d.id                                   rowidlocation,
               a.details,
               a.isactive,
               a.isconfirmed,
               a.temporalstartdate,
               a.temporalenddate,
               a.rowstatus,
               a.syschangehistoryid
        FROM demodata.falocationhistory a
        LEFT JOIN crmcontactsubtypeemployee b
                  ON b.id = fnsysidget(l_systemid, a.crmcontactidassignedto) AND b.temporalenddate = '9999-12-31'
        JOIN      fafixedasset c
                  ON c.id = fnsysidget(l_systemid, a.fafixedassetid)
        LEFT JOIN comlocation d
                  ON d.syschangehistoryid = (c.syschangehistoryid % l_comlocationlast) + 1
        JOIN      glcostcentre e
                  ON e.referencenumber = l_systemid::VARCHAR
        ORDER BY a.id;

        CALL spsysTemporalDataNormalize('falocationhistory');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE (a.id % 20 = l_systemid - 99)
           OR a.id IN (
                          SELECT gltransactionid
                          FROM demodata.actactivitysubtypebilling)
        ORDER BY a.id;

        -- if a transaction was reversed we need to pick up that transaction too.
        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)                    id,
               fnsysidget(l_systemid, glbatchid)               glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               fnsysidget(l_systemid, gltransactionidreversed) gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                                            syschangehistoryid
        FROM demodata.gltransaction a
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT aa.gltransactionidreversed
                                                  FROM gltransaction aa
                                                  WHERE aa.gltransactionidreversed IS NOT NULL)
          AND fnsysidget(l_systemid, a.id) NOT IN (
                                                      SELECT aa.id
                                                      FROM gltransaction aa);

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               b.id                                      glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE (jl0.id IN (
                             SELECT crmcontactid
                             FROM crmcontactsubtypeemployee
                             UNION
                             SELECT DISTINCT rowidchargedto
                             FROM glentry
                             WHERE sysdictionarytableidchargedto = 100
                             UNION
                             SELECT DISTINCT bb.crmcontactid
                             FROM glentry aa
                             JOIN demodata.glbillingaccount bb
                                  ON bb.id = aa.rowidchargedto
                             WHERE aa.sysdictionarytableidchargedto = 321)
            OR syschangehistoryid = 1)
          AND jl0.id != fnsysidget(101, 2);

        INSERT INTO crmcontact
        SELECT *
        FROM s0100v0000.crmcontact
        WHERE id = fnsysidget(100, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0101v0000.crmcontact
        WHERE id = fnsysidget(101, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0102v0000.crmcontact
        WHERE id = fnsysidget(102, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0103v0000.crmcontact
        WHERE id = fnsysidget(103, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0104v0000.crmcontact
        WHERE id = fnsysidget(104, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0105v0000.crmcontact
        WHERE id = fnsysidget(105, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0106v0000.crmcontact
        WHERE id = fnsysidget(106, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0107v0000.crmcontact
        WHERE id = fnsysidget(107, 2);
        INSERT INTO crmcontact
        SELECT *
        FROM s0108v0000.crmcontact
        WHERE id = fnsysidget(108, 2);

        -- Identify all departments which will be used later as clients
        UPDATE crmcontact
        SET syschangehistoryid= -1
        WHERE fnsysidview(id, 's') != l_systemid
          AND fnsysidview(id, 'r') = 2;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE syschangehistoryid = -1) aaa) aa)
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET rowidchargedto=fnsysidget(l_systemid, rowidchargedto)
        WHERE sysdictionarytableidchargedto = 401;

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY CASE WHEN glaccountid = 1000000000000032 THEN 1
                                                          ELSE 2
                                                          END, crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.type, aa.crmcontactid) rowid, crmcontactid crmcontactid
                        FROM ( -- First list all employees followed by everyone else
                                 SELECT DISTINCT *
                                 FROM (
                                          SELECT 1 type, crmcontactid
                                          FROM crmcontactsubtypeemployee
                                          UNION
                                          SELECT 2 type, id
                                          FROM crmcontact
                                          WHERE syschangehistoryid = -1) aaa) aa)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON (bb.rowid % (
                                         SELECT MAX(rowid)
                                         FROM new)) + 1 = cc.rowid) b
        WHERE a.id = b.id;

        -- Get rid of duplicate accounts
        WITH new AS (
                        SELECT id oldid, newid
                        FROM glbillingaccount A
                        JOIN (
                                 SELECT crmcontactid, glaccountid, MIN(id) NewID
                                 FROM glbillingaccount
                                 GROUP BY crmcontactid, glaccountid
                                 HAVING COUNT(*) > 1) b
                             ON b.crmcontactid = a.crmcontactid AND b.glaccountid = a.glaccountid
                        WHERE a.id != NewID)
        UPDATE glentry a
        SET rowidchargedto=b.newid
        FROM new b
        WHERE a.sysdictionarytableidchargedto = 321
          AND a.rowidchargedto = b.oldid;

        DELETE
        FROM glbillingaccount
        WHERE id NOT IN (
                            SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);


        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        JOIN glcostcentre b
             ON b.referencenumber = l_systemid::VARCHAR
        WHERE b.id = a.glcostcentreid
           OR a.glcostcentreid IS NULL;

        INSERT INTO actproject
        SELECT *
        FROM demodata.actproject a;

        UPDATE actproject
        SET id=fnsysidget(l_systemid, id), actprojectidparent=fnsysidget(l_systemid, actprojectidparent);

        INSERT INTO actratebilling
        SELECT *
        FROM demodata.actratebilling a;

        UPDATE actratebilling
        SET id=fnsysidget(l_systemid, id), actprojectid=fnsysidget(l_systemid, actprojectid);

        INSERT INTO actrateexpense
        SELECT *
        FROM demodata.actrateexpense a;

        INSERT INTO actactivitysubtypebilling (id, actactivityid, crmcontactidinvoicethrough, gltransactionid, actbillingstatusid, billedamount, overridebilledamount, overridehours, revenueamount, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fnsysidget(l_systemid, actactivityid),
               crmcontactidinvoicethrough,
               fnsysidget(l_systemid, gltransactionid),
               actbillingstatusid,
               billedamount,
               overridebilledamount,
               overridehours,
               revenueamount,
               rowstatus,
               syschangehistoryid
        FROM demodata.actactivitysubtypebilling a;

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE fnsysidget(l_systemid, id) IN (
                                                SELECT actactivityid
                                                FROM actactivitysubtypebilling);

        UPDATE actactivity SET id=fnsysidget(l_systemid, id), actprojectid=fnsysidget(l_systemid, actprojectid);

        DROP TABLE IF EXISTS t_employees;
        CREATE TEMP TABLE t_employees
        (
            id    SERIAL,
            oldid BIGINT,
            newid BIGINT
        );

        INSERT INTO t_employees (oldid)
        SELECT DISTINCT rowidperformedby
        FROM actactivity
        WHERE rowidperformedby IS NOT NULL;

        UPDATE t_employees a
        SET newid=crmcontactid
        FROM crmcontactsubtypeemployee b
        WHERE b.syschangehistoryid = (a.id % 20) + 1;

        DROP TABLE IF EXISTS t_clients;
        CREATE TEMP TABLE t_clients
        (
            id    SERIAL,
            oldid BIGINT,
            newid BIGINT
        );

        INSERT INTO t_clients (oldid)
        SELECT DISTINCT crmcontactid
        FROM (
                 SELECT crmcontactid
                 FROM actproject
                 UNION ALL
                 SELECT rowidperformedfor
                 FROM actactivity) a
        WHERE crmcontactid IS NOT NULL;

        WITH new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE syschangehistoryid = -1)
        UPDATE t_clients a
        SET newid=b.crmcontactid
        FROM new b
        WHERE (a.id % 8) + 1 = b.rowid;

        UPDATE actproject a
        SET crmcontactid=COALESCE(b.newid, fnsysidget(l_systemid, 2))
        FROM t_clients b
        WHERE b.oldid = crmcontactid
           OR b.oldid IS NULL;

        UPDATE actproject a
        SET glcostcentreid=b.id
        FROM glcostcentre b
        WHERE b.referencenumber = fnsysidview(a.crmcontactid, 's')::VARCHAR;

        UPDATE actactivity a
        SET rowidperformedfor=COALESCE(b.crmcontactid, fnsysidget(l_systemid, 2)), sysdictionarytableidperformedfor=100
        FROM actproject b
        WHERE b.id = actprojectid
           OR b.id IS NULL;

        UPDATE actactivity a
        SET rowidperformedby=b.newid
        FROM t_employees b
        WHERE b.oldid = rowidperformedby;

        UPDATE actratebilling a
        SET rowidchargedby = b.newid
        FROM t_employees b
        WHERE b.oldid = rowidchargedby;

        UPDATE actrateexpense a
        SET rowidchargedby=b.newid
        FROM t_employees b
        WHERE b.oldid = rowidchargedby;

        -- Change general operation activities to one of the five specific types
        UPDATE actactivity a
        SET acttypeid=b.acttypeid
        FROM (
                 SELECT RANK() OVER (ORDER BY aa.id) rowid, aa.id actTypeID
                 FROM actType aa
                 WHERE aa.glcostcentreid IS NOT NULL
                   AND aa.topdownlevel != 3) b
        WHERE a.acttypeid = fnsysidget(100, 2)
          AND (a.sysCHangeHistoryID % 5) + 1 = b.rowid;

        SET session_replication_role = REPLICA;
        INSERT INTO s0100v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(100, 2);

        INSERT INTO s0101v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(101, 2);

        INSERT INTO s0102v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(102, 2);

        INSERT INTO s0103v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(103, 2);

        INSERT INTO s0104v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(104, 2);

        INSERT INTO s0105v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(105, 2);

        INSERT INTO s0106v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(106, 2);

        INSERT INTO s0107v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(107, 2);

        INSERT INTO s0108v0000.actproject
        SELECT *
        FROM actproject
        WHERE crmcontactid = fnsysidget(108, 2);

        DROP TABLE IF EXISTS t_projectstobedeleted;
        CREATE TEMP TABLE t_projectstobedeleted
        (
            id BIGINT
        );

        -- The following statements fix the performed for on the activities.  This is necessary
        -- because each system is selecting random records based on dividing the id by 20 and getting
        -- the remainder.  Then they assign performed for's for all records based their contacts.
        -- Public services is taking any activities that are billable and using them as activities
        -- to bill the departments.  We need to change the performed for to the performed for
        -- on the project which is the department.  Note, I did recurse through the projects in
        -- preparing demo data and made sure the performed for on sub-activities was the same
        -- as the performed for on the projects
        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(100, 2)
                        GROUP BY crmcontactid)
        UPDATE s0100v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0100v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(101, 2)
                        GROUP BY crmcontactid)
        UPDATE s0101v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0101v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(102, 2)
                        GROUP BY crmcontactid)
        UPDATE s0102v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0102v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(103, 2)
                        GROUP BY crmcontactid)
        UPDATE s0103v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0103v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(104, 2)
                        GROUP BY crmcontactid)
        UPDATE s0104v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0104v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(105, 2)
                        GROUP BY crmcontactid)
        UPDATE s0105v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0105v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(106, 2)
                        GROUP BY crmcontactid)
        UPDATE s0106v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0106v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(107, 2)
                        GROUP BY crmcontactid)
        UPDATE s0107v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0107v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        WITH sub AS (
                        SELECT MIN(id) actprojectidfirst, crmcontactid
                        FROM actproject
                        WHERE crmcontactid = fnsysidget(108, 2)
                        GROUP BY crmcontactid)
        UPDATE s0108v0000.actactivity a
        SET actprojectid = COALESCE(b.id, C.actprojectidfirst), rowidperformedfor = C.crmcontactid
        FROM actproject b,
             sub C,
             s0109v0000.actactivity D
        WHERE fnsysidview(a.id, 'r') = fnsysidview(D.id, 'r')
          AND D.actprojectid IS NOT NULL
          AND ((b.id = D.actprojectid
            AND b.crmcontactid = C.crmcontactid)
            OR b.id IS NULL);

        INSERT INTO t_projectstobedeleted
        SELECT id
        FROM s0108v0000.actactivity a
        WHERE actprojectid IS NOT NULL;

        DELETE
        FROM actactivity a
        WHERE a.id IN (
                          SELECT fnsysidget(109, fnsysidview(id, 'r'))
                          FROM t_projectstobedeleted);

        UPDATE actactivitysubtypebilling a
        SET id=b.id, actactivityid=b.id
        FROM actactivity b
        WHERE fnsysidview(b.id, 'r') = fnsysidview(a.id, 'r');

        DELETE
        FROM actactivitysubtypebilling
        WHERE actactivityid NOT IN (
                                       SELECT id
                                       FROM actactivity);

        -- Set up subscriptions to projects for all departments
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, 401, NULL, 'Project', NULL, 1, 1, 'a', NULL)
        RETURNING id INTO l_id;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_id, 401, 10500, 'Subproject', NULL, 2, 2, 'a', NULL);

        INSERT INTO exsubscriber (exsystemid, name)
        SELECT id, name
        FROM exsystem
        WHERE id > 99
          AND id != l_systemid
          AND name NOT ILIKE 'dw%';

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT b.id exsubscriberid, l_id exrecordgroupid, a.id rowidsubscribedto
        FROM actproject a
        JOIN exsubscriber b
             ON b.exsystemid = fnsysidview(a.crmcontactid, 's')
        WHERE fnsysidview(a.crmcontactid, 's') != l_systemid;

        PERFORM fnsysIDSetValAll();

        INSERT INTO s0101v0000.crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM crmcontact A
        WHERE id NOT IN (
                            SELECT id
                            FROM s0101v0000.crmcontact)
          AND fnsysidview(a.id, 's') = 101;

        INSERT INTO s0101v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        SELECT (
                   SELECT id
                   FROM s0101v0000.exsubscriber
                   WHERE exsystemid = l_systemid
                   ORDER BY id
                   LIMIT 1) exsubscriberid,
               (
                   SELECT id
                   FROM s0101v0000.exrecordgroup
                   WHERE name ILIKE 'contact'
                   ORDER BY id
                   LIMIT 1) exrecordgroupid,
               id
        FROM crmcontact
        WHERE fnsysIDView(ID, 's') = 101;

        -- setup customer and employee relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               fnsysidget(l_systemid, id),
               60,
               '2000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM DemoData.crmcontact a
        WHERE syschangehistoryid = -1
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        SET session_replication_role = ORIGIN;

        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid

    END; -- Finish Public Works (109)
$$ LANGUAGE plpgsql;
DO -- ABC Consulting Inc (110)
$$
    DECLARE
        l_billingaccountid BIGINT;
        l_recordgroupid    BIGINT;
        l_subscriberid     BIGINT;
        l_subscriptionid   BIGINT;
        l_systemid         INT := 110;
    BEGIN
        --**************************************  Create ABC Consulting Inc (110) **************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0110v0000 CASCADE;
        CALL spsysschemacreate(
                p_firstname := 'William',
                p_lastname := 'Tindall',
                p_username := 'wTindall',
                p_email := 'wtindall@shaw.ca',
                p_organizationname := 'ABC Consulting Inc (110)',
                p_systemname := 'ABC Consulting Inc (110)',
                p_exSystemID := l_systemid,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0110v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO glaccount
        SELECT *
        FROM demodata.glaccount
        WHERE id NOT IN (
                            SELECT id
                            FROM glaccount);

        INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 1),
               glaccountidparent,
               glaccounttypeid,
               1000                         sysmultilinktableruleid, -- Connect to contacts
               bankaccountnumber,
               topdownlevel,
               bottomuplevel,
               comments,
               'ABC Consulting Inc Revenue' description,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               displaysequence,
               rowstatus,
               syschangehistoryid
        FROM glaccount
        WHERE id = fnsysidget(100, 62);-- Consulting Revenue

        TRUNCATE TABLE crmrelationshiptype;
        INSERT INTO crmrelationshiptype
        SELECT *
        FROM demodata.crmrelationshiptype;

        INSERT INTO crmcontactsubtypeemployee (crmcontactid, hremploymentstatusid, commencementdate, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(101, id),
               40,
               dateadd('year', 25, birthdate),
               dateadd('year', 25, birthdate),
               '9999-12-31',
               'a',
               RANK() OVER (ORDER BY id)
        FROM demodata.crmcontact
        WHERE id % 20 = l_systemid - 99
        ORDER BY id
        LIMIT 20;

        CALL spsysTemporalDataNormalize('crmcontactsubtypeemployee');

        INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)      id,
               fnsysidget(l_systemid, glbatchid) glbatchid,
               fnsysidget(l_systemid, glentryidmain),
               glpostingstatusid,
               NULL                              gltransactionidreversed,
               gltransactiontypeid,
               sysdictionarytableidsubtype,
               createdate,
               description,
               duedate,
               referencenumber,
               transactiondate,
               a.id                              syschangehistoryid
        FROM demodata.gltransaction a
        WHERE ((a.id % 20 = l_systemid - 99)
            OR a.id IN (
                           SELECT gltransactionid
                           FROM demodata.actactivitysubtypebilling
                           WHERE id % 20 = l_systemid - 99))
            -- only interested in getting employee payable and consulting revenue transactions
          AND a.id NOT IN (
                              SELECT gltransactionid
                              FROM demodata.glentry
                              WHERE sysdictionarytableidchargedto = 321
                                AND glaccountid NOT IN (1400, 1000000000000032))
        ORDER BY a.id;

        INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
        SELECT fnsysidget(l_systemid, a.id)              id,
               a.glaccountid,
               a.glcostcentreid,
               a.glentrytypeid,
               fnsysidget(l_systemid, a.gltransactionid) gltransactionid,
               a.sysdictionarytableidchargedto,
               a.rowidchargedto,
               a.amount,
               a.bankreconciliationdate,
               a.comments,
               a.description,
               a.reconciliationbalance,
               a.referencenumber,
               a.reportingperioddate,
               a.rollupamount,
               a.syschangehistoryid
        FROM demodata.glentry a
        WHERE a.gltransactionid IN (
                                       SELECT syschangehistoryid
                                       FROM gltransaction);

        INSERT INTO crmcontact (id, name, lastname, middlename, firstname, crmgenderid, birthdate, contactperson, rowstatus, syschangehistoryid)
        SELECT jl0.id,
               name,
               lastname,
               middlename,
               firstname,
               crmgenderid,
               birthdate,
               contactperson,
               rowstatus,
               syschangehistoryid
        FROM DemoData.crmcontact A
        JOIN LATERAL (SELECT CASE WHEN a.id < 100000 THEN fnsysidget(101, a.id)
                                  ELSE fnsysidget(102, a.id)
                                  END id) jl0
             ON TRUE
        WHERE (jl0.id IN (
                             SELECT crmcontactid
                             FROM crmcontactsubtypeemployee
                             UNION
                             SELECT DISTINCT rowidchargedto
                             FROM glentry
                             WHERE sysdictionarytableidchargedto = 100
                             UNION
                             SELECT DISTINCT bb.crmcontactid
                             FROM glentry aa
                             JOIN demodata.glbillingaccount bb
                                  ON bb.id = aa.rowidchargedto
                             WHERE aa.sysdictionarytableidchargedto = 321)
            OR syschangehistoryid = 1)
          AND jl0.id != fnsysidget(101, 2);

        INSERT INTO crmcontact
        SELECT *
        FROM s0109v0000.crmcontact
        WHERE fnsysidview(id, 'r') = 2
          AND id NOT IN (
                            SELECT id
                            FROM crmcontact);

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto crmcontactid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 100) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) aa)
        UPDATE glentry a
        SET rowidchargedto=b.crmcontactid
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glentry aa
                 JOIN OLD bb
                      ON aa.rowidchargedto = bb.crmcontactid
                 JOIN NEW cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 100) b
        WHERE a.id = b.id;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto fafixedassetid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 200) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id fafixedassetid
                        FROM fafixedasset aa)
        UPDATE glentry a
        SET rowidchargedto=b.fafixedassetid
        FROM (
                 SELECT aa.id, cc.fafixedassetid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.fafixedassetid
                 JOIN new cc
                      ON bb.rowid = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 200) b
        WHERE a.id = b.id;

        UPDATE glentry a
        SET rowidchargedto=fnsysidget(109, rowidchargedto)
        WHERE sysdictionarytableidchargedto = 401;

        UPDATE glentry a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.gltransactionid = b.gltransactionid;

        UPDATE gltransaction a
        SET description=b.description
        FROM (
                 SELECT gltransactionid, description FROM glentry aa WHERE syschangehistoryid = 999) b
        WHERE a.id = b.gltransactionid;

        INSERT INTO glbillingaccount
        SELECT *
        FROM demodata.glbillingaccount a
        WHERE id IN (
                        SELECT rowidchargedto
                        FROM glentry
                        WHERE sysdictionarytableidchargedto = 321
                          AND glaccountid = 1000000000000032)
          AND glaccountid = 1000000000000032;

        WITH old AS (
                        -- We want employee accounts to come first then other accounts
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid,
                               crmcontactid
                        FROM glbillingaccount),
             new AS (
                        SELECT RANK() OVER (ORDER BY crmcontactid) rowid, crmcontactid
                        FROM (
                                 SELECT DISTINCT crmcontactid
                                 FROM crmcontactsubtypeemployee) a)
        UPDATE glbillingaccount a
        SET crmcontactid=b.crmcontactid, syschangehistoryid=999
        FROM (
                 SELECT aa.id, cc.crmcontactid
                 FROM glbillingaccount aa
                 JOIN old bb
                      ON aa.crmcontactid = bb.crmcontactid
                 JOIN NEW cc
                      ON (cc.rowid % (
                                         SELECT MAX(rowid)
                                         FROM old)) + 1 = bb.rowid) b
        WHERE a.id = b.id;

        UPDATE glbillingaccount a
        SET crmcontactid=(
                             SELECT MIN(crmcontactid) FROM glbillingaccount WHERE syschangehistoryid = 999)
        WHERE crmcontactid NOT IN (
                                      SELECT DISTINCT crmcontactid
                                      FROM glbillingaccount
                                      WHERE syschangehistoryid = 999);

        -- Get rid of duplicate accounts
        WITH new AS (
                        SELECT id oldid, newid
                        FROM glbillingaccount A
                        JOIN (
                                 SELECT crmcontactid, glaccountid, MIN(id) NewID
                                 FROM glbillingaccount
                                 GROUP BY crmcontactid, glaccountid
                                 HAVING COUNT(*) > 1) b
                             ON b.crmcontactid = a.crmcontactid AND b.glaccountid = a.glaccountid
                        WHERE a.id != NewID)
        UPDATE glentry a
        SET rowidchargedto=b.newid
        FROM new b
        WHERE a.sysdictionarytableidchargedto = 321
          AND a.rowidchargedto = b.oldid;

        DELETE
        FROM glbillingaccount
        WHERE id NOT IN (
                            SELECT rowidchargedto FROM glentry WHERE sysdictionarytableidchargedto = 321);

        UPDATE glbillingaccount a
        SET name=b.name || ' - ' || c.description, id=fnsysidget(l_systemid, a.id)
        FROM crmcontact b,
             glaccount c
        WHERE b.id = a.crmcontactid
          AND c.id = a.glaccountid;

        WITH old AS (
                        SELECT RANK() OVER (ORDER BY aa.rowidchargedto) rowid, rowidchargedto glbillingaccountid
                        FROM (
                                 SELECT DISTINCT rowidchargedto
                                 FROM glentry aaa
                                 WHERE sysdictionarytableidchargedto = 321
                                   AND glaccountid = 1000000000000032) aa),
             new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id glbillingaccountid
                        FROM glbillingaccount aa)
        UPDATE glentry a
        SET rowidchargedto=b.glbillingaccountid
        FROM (
                 SELECT aa.id, cc.glbillingaccountid
                 FROM glentry aa
                 JOIN old bb
                      ON aa.rowidchargedto = bb.glbillingaccountid
                 JOIN new cc
                      ON bb.rowid % (
                                        SELECT MAX(rowid)
                                        FROM new) + 1 = cc.rowid
                 WHERE aa.sysdictionarytableidchargedto = 321
                   AND aa.glaccountid = 1000000000000032) b
        WHERE a.id = b.id;

        PERFORM fnsysIDSetVal('glbillingaccount');

        INSERT INTO glbillingaccount (crmcontactid, glaccountid, glbillingaccountstatusid, isgstexempt, name, rowstatus)
        SELECT id                              crmcontactid,
               1400                            glaccountid,
               20                              glbillingaccountstatusid,
               FALSE,
               name || ' - Account Receivable' name,
               'a'                             rowstatus
        FROM crmcontact
        WHERE id = fnsysidget(109, 2)
        RETURNING id INTO l_billingaccountid;

        UPDATE glentry SET rowidchargedto=l_billingaccountid WHERE glaccountid = 1400;

        UPDATE glentry a
        SET sysdictionarytableidchargedto=NULL, rowidchargedto=NULL
        WHERE sysdictionarytableidchargedto = 200
          AND NOT EXISTS(SELECT FROM fafixedasset aa WHERE aa.id = a.rowidchargedto);

        INSERT INTO glbatch
        SELECT *
        FROM demodata.glbatch A
        WHERE fnsysidget(l_systemid, a.id) IN (
                                                  SELECT glbatchid
                                                  FROM gltransaction);

        UPDATE glbatch SET id=fnsysidget(l_systemid, id);

        INSERT INTO acttype
        SELECT a.*
        FROM demodata.acttype a
        WHERE glcostcentreid IS NULL;

        UPDATE acttype
        SET glcostcentreid=NULL, id=fnsysidget(l_systemid, fnsysidview(id, 'r')),
            acttypeidparent=fnsysidget(l_systemid, fnsysidview(acttypeidparent, 'r'));

        UPDATE acttype SET description = 'Operate ABC Consulting' WHERE id = fnsysidget(l_systemid, 1);
        UPDATE acttype SET description = 'Develop and Support Systems' WHERE id = fnsysidget(l_systemid, 2);

        INSERT INTO actactivity
        SELECT *
        FROM demodata.actactivity a
        WHERE id % 20 = l_systemid - 99;

        UPDATE actactivity
        SET id=fnsysidget(l_systemid, id), actprojectid=fnsysidget(109, actprojectid),
            acttypeid=fnsysidget(l_systemid, fnsysidview(acttypeid, 'r'));

        INSERT INTO actactivitysubtypebilling (id, actactivityid, crmcontactidinvoicethrough, gltransactionid, actbillingstatusid, billedamount, overridebilledamount, overridehours, revenueamount, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, id),
               fnsysidget(l_systemid, actactivityid),
               crmcontactidinvoicethrough,
               fnsysidget(l_systemid, gltransactionid),
               actbillingstatusid,
               billedamount,
               overridebilledamount,
               overridehours,
               revenueamount,
               rowstatus,
               syschangehistoryid
        FROM demodata.actactivitysubtypebilling a
        WHERE fnsysidget(l_systemid, actactivityid) IN (
                                                           SELECT id
                                                           FROM actactivity);

        UPDATE actactivitysubtypebilling a
        SET id=b.id, actactivityid=b.id
        FROM actactivity b
        WHERE fnsysidview(b.id, 'r') = fnsysidview(a.id, 'r');

        INSERT INTO actproject
        SELECT *
        FROM s0109v0000.actproject a
        WHERE id IN (
                        SELECT DISTINCT actprojectid
                        FROM actactivity
                        UNION
                        SELECT DISTINCT rowidchargedto
                        FROM glentry a
                        WHERE a.sysdictionarytableidchargedto = 401);

        INSERT INTO actproject
        SELECT *
        FROM s0109v0000.actproject a
        WHERE id IN (
                        SELECT DISTINCT actprojectidparent
                        FROM actproject)
          AND id NOT IN (
                            SELECT id
                            FROM actproject);

        UPDATE actproject SET glcostcentreid=0;

        INSERT INTO actratebilling
        SELECT *
        FROM demodata.actratebilling a
        WHERE fnsysidget(109, actprojectid) IN (
                                                   SELECT id
                                                   FROM actproject);

        UPDATE actratebilling
        SET id=fnsysidget(l_systemid, id), actprojectid=fnsysidget(109, actprojectid),
            acttypeid=fnsysidget(l_systemid, fnsysidview(acttypeid, 'r'));

        INSERT INTO actrateexpense
        SELECT *
        FROM demodata.actrateexpense a;

        DROP TABLE IF EXISTS t_employees;
        CREATE TEMP TABLE t_employees
        (
            id    SERIAL,
            oldid BIGINT,
            newid BIGINT
        );

        INSERT INTO t_employees (oldid)
        SELECT DISTINCT rowidperformedby
        FROM actactivity
        WHERE rowidperformedby IS NOT NULL;

        UPDATE t_employees a
        SET newid=crmcontactid
        FROM crmcontactsubtypeemployee b
        WHERE b.syschangehistoryid = (a.id % 20) + 1;

        DROP TABLE IF EXISTS t_clients;
        CREATE TEMP TABLE t_clients
        (
            id    SERIAL,
            oldid BIGINT,
            newid BIGINT
        );

        INSERT INTO t_clients (oldid)
        SELECT DISTINCT crmcontactid
        FROM (
                 SELECT crmcontactid
                 FROM actproject
                 UNION ALL
                 SELECT rowidperformedfor
                 FROM actactivity) a
        WHERE crmcontactid IS NOT NULL;

        WITH new AS (
                        SELECT RANK() OVER (ORDER BY aa.id) rowid, id crmcontactid
                        FROM crmcontact aa
                        WHERE syschangehistoryid = -1)
        UPDATE t_clients a
        SET newid=b.crmcontactid
        FROM new b
        WHERE (a.id % 8) + 1 = b.rowid;

        UPDATE actactivity a
        SET rowidperformedby=b.newid
        FROM t_employees b
        WHERE b.oldid = rowidperformedby;

        UPDATE actactivity a
        SET rowidperformedfor=b.newid
        FROM t_clients b
        WHERE b.oldid = rowidperformedfor;

        UPDATE actactivity a
        SET rowidperformedfor=b.crmcontactid
        FROM actproject b
        WHERE b.id = a.actprojectid;

        UPDATE actratebilling a
        SET rowidchargedby = b.newid
        FROM t_employees b
        WHERE b.oldid = rowidchargedby;

        DELETE
        FROM actratebilling
        WHERE rowidchargedby NOT IN (
                                        SELECT id
                                        FROM crmcontact);

        UPDATE actrateexpense a
        SET rowidchargedby=b.newid
        FROM t_employees b
        WHERE b.oldid = rowidchargedby;

        DELETE
        FROM actrateexpense
        WHERE rowidchargedby NOT IN (
                                        SELECT id
                                        FROM crmcontact);

        -- Set up subscriptions so activities are exported to Public works
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (NULL, 420, NULL, 'actActivity', 'actprojectid is not null', 1, 1, 'a', NULL)
        RETURNING id INTO l_recordgroupid;
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (l_recordgroupid, 421, 10165, 'actActivitysubtypebilling', NULL, 2, 2, 'a', NULL);

        INSERT INTO exsubscriber (exsystemid, name)
        SELECT id, name
        FROM exsystem
        WHERE id = 109
        RETURNING id INTO l_subscriberid;

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid)
        SELECT l_subscriberid, l_recordgroupid exrecordgroupid
        RETURNING id INTO l_subscriptionid;

        -- set the activity type id to the activity type id for public services
        INSERT INTO exsubscriptionredaction (exsubscriptionid, sysdictionarycolumnidredacted, redactedsql, redactedvalue, redactedtranslation)
        VALUES (l_SubscriptionID, fnsysIDGet(0, 10060), '(fnsysidget(110,2)', '1100000000000002', 'ABC Consulting');

        -- set the activity type id to the activity type id for public services
        INSERT INTO exsubscriptionredaction (exsubscriptionid, sysdictionarycolumnidredacted, redactedsql, redactedvalue, redactedtranslation)
        VALUES (l_SubscriptionID, fnsysIDGet(0, 10040), '(fnsysidget(109,3)', '1090000000000003', 'Develop applications');

        INSERT INTO s0109v0000.crmcontact
        SELECT *
        FROM crmcontact
        WHERE id = fnsysidget(l_systemid, 2)
          AND NOT EXISTS(SELECT FROM s0109v0000.crmcontact WHERE id = fnsysidget(l_systemid, 2));

        DELETE
        FROM s0109v0000.actactivitysubtypebilling
        WHERE actactivityid IN (
                                   SELECT fnsysidget(109, fnsysidview(id, 'r'))
                                   FROM actactivity);

        DELETE
        FROM s0109v0000.actactivity
        WHERE id IN (
                        SELECT fnsysidget(109, fnsysidview(id, 'r'))
                        FROM actactivity);

        PERFORM fnsysIDSetValAll();

        -- setup customer and employee relationships
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, temporalstartdate, temporalenddate, rowstatus, syschangehistoryid)
        SELECT fnsysidget(l_systemid, 2),
               fnsysidget(l_systemid, id),
               60,
               '2000-01-01',
               '9999-12-31',
               'a',
               syschangehistoryid
        FROM DemoData.crmcontact a
        WHERE syschangehistoryid = -1
        UNION
        SELECT fnsysidget(l_systemid, 2),
               crmcontactid,
               10,
               temporalstartdate,
               temporalenddate,
               rowstatus,
               syschangehistoryid
        FROM crmcontactsubtypeemployee
        WHERE temporalenddate = '9999-12-31';

        CALL spsysTemporalDataNormalize('crmrelationship');
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid

    END; -- Finish ABC Consulting Inc (110)
$$ LANGUAGE plpgsql;

DO -- Setup Bank of Fairfax
$$
    DECLARE
        l_systemid        INT := 120;
        l_id BIGINT;
    BEGIN
        --**************************************  Create Municipal Affairs (105) **************************************
        SET search_path TO s0001v0000, s0000v0000,public;
        DROP SCHEMA IF EXISTS s0120v0000 CASCADE;
        CALL spsysschemacreate(
                p_systemname := 'Bank of Fairfax (120)',
                p_exSystemID := 120,
                p_Turnontriggersandconstraints := FALSE);

        SET search_path TO s0120v0000, s0000v0000,public;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id)
        AND ID > 99 AND name not ilike 'dw%';

        INSERT INTO sysdictionarytable (ID, Name, Translation, ChangeHistoryLevel, ChangeHistoryScope, Description, IsChangeHistoryUsed, IsTableTemporal, NormalizedName, SingularName, PluralName, SystemModule, TableType, RowStatus, sysChangeHistoryID)
        SELECT ID,
               Name,
               Translation,
               ChangeHistoryLevel,
               ChangeHistoryScope,
               Description,
               IsChangeHistoryUsed,
               IsTableTemporal,
               NormalizedName,
               SingularName,
               PluralName,
               SystemModule,
               TableType,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarytable
        WHERE fnsysidview(ID, 's') = 103;

        INSERT INTO sysdictionarycolumn (ID, sysDictionaryTableID, Name, Label, ColumnSequence, DefaultValue, isnullable, datatype, datalength, decimals, purpose, sysDictionaryTableIDForeign, IsChangeHistoryUsed, RowStatus, sysChangeHistoryID)
        SELECT ID,
               sysDictionaryTableID,
               Name,
               Label,
               ColumnSequence,
               DefaultValue,
               isnullable,
               datatype,
               datalength,
               decimals,
               purpose,
               sysDictionaryTableIDForeign,
               IsChangeHistoryUsed,
               RowStatus,
               sysChangeHistoryID
        FROM demodata.sysdictionarycolumn
        WHERE fnsysidview(ID, 's') = 103
        and name not ilike 'mttaxrollid';

        CALL spsysSchemaUpdate();

        INSERT INTO crmcontact
        SELECT *
        FROM s0102v0000.crmcontact
        WHERE id = fnsysidget(102, 114300);

        INSERT INTO s0103v0000.exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
        VALUES ((SELECT id FROM s0103v0000.exsubscriber WHERE exsystemid=120),
                (SELECT id FROM s0103v0000.exrecordgroup WHERE name ILIKE 'Land Interest'),
                fnsysidget(103, 101));

        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyCacheRefresh();
        CALL spsysViewCreate();
        CALL spsysForeignKeyConstraintGenerate();
        --select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid

    END; -- Finish Bank of Fairfax (120)
$$ LANGUAGE plpgsql;

--xx
SET SEARCH_PATH TO s0100v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0101v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0102v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0103v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0104v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0105v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0106v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0107v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0108v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0109v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

SET SEARCH_PATH TO s0110v0000, s0000v0000, public;
CALL spexPackageExport(p_sysChangeHistoryID := (fnsysChangeHistoryCreate('Export Data')));

-- The distribute will trigger imports in all of the systems
SET SEARCH_PATH TO s0090v0000, s0000v0000, public;
CALL spexpackagedistribute();

SET SEARCH_PATH TO s0100v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0101v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0102v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0103v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0104v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0105v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0106v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0107v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0108v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0109v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0110v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

SET SEARCH_PATH TO s0120v0000, s0000v0000, public;
CALL spexPackageImport(p_OverrideWarningConditions := TRUE);

CREATE OR REPLACE PROCEDURE s0000v0000.spSetupFinanceAssetSubscriptions() AS
$$
DECLARE
    l_NewChangeHistoryID BIGINT;
    l_subscriberid       BIGINT;
    l_id                 BIGINT;
BEGIN

    l_NewChangeHistoryID :=
            fnsysChangeHistoryCreate('Setup record groups, subscriptions to aggregate finances and assets in s0100v0000');

    INSERT INTO exsubscriber (exsystemid, name, rowstatus, syschangehistoryid)
    VALUES (100, 'Doreney Executive Office', 'a', l_NewChangeHistoryID)
    RETURNING id INTO l_subscriberid;

    INSERT INTO exrecordgroup (sysdictionarytableid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
    VALUES (310, 'Gl Batch','glbatchstatusid=2', 1, 1, 'a', l_NewChangeHistoryID)
    RETURNING id INTO l_id;

--         INSERT INTO exsubscriptionredaction (exsubscriptionid, sysdictionarycolumnidredacted, redactedsql, redactedvalue, redactedtranslation, syschangehistoryid)
--         VALUES (l_exSubscriptionID, fnsysIDGet(0, 90), '(select crmcontactidcompany from glsetup)', '10000000000095', 'M1 Software Inc.', l_NewChangeHistoryID);
--
--         INSERT INTO exsubscriptionredaction (exsubscriptionid, sysdictionarycolumnidredacted, redactedsql, redactedvalue, redactedtranslation, syschangehistoryid)
--         VALUES (l_exSubscriptionID, fnsysIDGet(0, 10550), 'null', 'null', 'null', l_NewChangeHistoryID);

    INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, syschangehistoryid)
    VALUES (l_id, l_subscriberid, l_NewChangeHistoryID);

    INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
    VALUES (l_id, 311, 15990, 'GlBatch->Transactions', 1, 1, 'a', l_NewChangeHistoryID)
    RETURNING id INTO l_id;

    INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
    VALUES (l_id, 316, 14940, 'GlBatch->Transaction->Entries', 1, 1, 'a', l_NewChangeHistoryID);

    INSERT INTO exrecordgroup (sysdictionarytableid, name, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
    VALUES (200, 'FixedAsset', 1, 1, 'a', l_NewChangeHistoryID)
    RETURNING id INTO l_id;

    INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, syschangehistoryid)
    VALUES (l_id, l_subscriberid, l_NewChangeHistoryID);

    INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
    VALUES (l_id, 201, 13190, 'FixedAsset->Locations', 1, 1, 'a', l_NewChangeHistoryID)
    RETURNING id INTO l_id;

    PERFORM fnsysHierarchyUpdate('exrecordgroup', 'name');

    CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

END
$$ LANGUAGE plpgsql;

SET SEARCH_PATH TO s0101v0000, s0000v0000, public;
call spSetupFinanceAssetSubscriptions();

SET SEARCH_PATH TO s0102v0000, s0000v0000, public;
call spSetupFinanceAssetSubscriptions();

SET SEARCH_PATH TO s0103v0000, s0000v0000, public;
call spSetupFinanceAssetSubscriptions();

SET SEARCH_PATH TO s0104v0000, s0000v0000, public;
call spSetupFinanceAssetSubscriptions();

SET SEARCH_PATH TO s0105v0000, s0000v0000, public;
call spSetupFinanceAssetSubscriptions();

SET SEARCH_PATH TO s0108v0000, s0000v0000, public;
call spSetupFinanceAssetSubscriptions();

SET SEARCH_PATH TO s0109v0000, s0000v0000, public;
call spSetupFinanceAssetSubscriptions();

SET SEARCH_PATH TO s0090v0000, s0000v0000, public;
CALL spexpackagedistribute();

SET SEARCH_PATH TO s0100v0000, s0000v0000, public;
CALL spexpackageimport();

DO
$$
    DECLARE
        l_schemadatawarehouse VARCHAR;
    BEGIN
        -- The first call p_schemadatawarehouse is null so it creates the DW.  p_schemadatawarehouse is an INOUT parameter
-- so it will assign it to l_schemadatawarehouse and use it in the second call as an update.
        SET search_path TO s0001v0000, s0000v0000,public;

        DROP SCHEMA IF EXISTS s0111v0000 CASCADE;
        CALL spsysDatawarehouseCreate(
                p_schemalist := 's0100v0000, s0101v0000, s0102v0000, s0103v0000, s0104v0000, s0105v0000, s0108v0000, s0109v0000',
                p_schemadatawarehouse := l_schemadatawarehouse);
        CALL spsysViewCreate(l_schemadatawarehouse);
        CALL spsysForeignKeyCacheRefresh();
       -- CALL spsysMasterDataIndexGenerate();
    END;
$$ LANGUAGE plpgsql;

-- Prepare to move the systems to different subnet servers
DO
$$
    BEGIN
        PERFORM fnsysMDSExecute('update s0000v0000.exsystem SET exsubnetserverid=11 WHERE id = 106;');
        PERFORM fnsysMDSExecute('update s0000v0000.exsystem SET exsubnetserverid=12 WHERE id = 107;');
        PERFORM fnsysMDSExecute('update s0000v0000.exsystem SET exsubnetserverid=13 WHERE id = 110;');
        PERFORM fnsysMDSExecute('update s0000v0000.exsystem SET exsubnetserverid=14 WHERE id = 120;');

        UPDATE s0090v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0100v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0101v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0102v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0103v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0104v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0105v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0106v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0107v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0108v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0109v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0110v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0111v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;
        UPDATE s0120v0000.exsystem a SET exsubnetserverid=b.exsubnetserverid FROM vwexsystemaws b WHERE b.id = a.id;

    END;
$$ LANGUAGE plpgsql;

/*
Create backup of sn000010

1. Connect to Postgres ***********************

2. Run backup
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE if exists sn000010origbak;
CREATE DATABASE sn000010origbak WITH TEMPLATE sn000010 OWNER postgres;
COMMENT ON DATABASE sn000010origbak IS '1st backup of sn000010';

 SELECT a.id,
               b.id crmcontactiduser,
               c.id syscommandid,
               a.sysdictionarytableidappliesto,
               a.rowidappliesto,
               a.rowtemporalenddate,
               a.changedate,
               a.isexported,
               a.ismaxrecordsignored,
               a.comments,
               a.syschangehistoryidundo,
               a.rowstatus
        FROM t_sysChangeHistory a
        LEFT JOIN crmcontact b
                  ON b.id = a.crmcontactiduser
        LEFT JOIN syscommand c
                  ON c.id = a.syscommandid
        WHERE NOT EXISTS(SELECT FROM syschangehistory aa WHERE aa.id = a.id);

 */