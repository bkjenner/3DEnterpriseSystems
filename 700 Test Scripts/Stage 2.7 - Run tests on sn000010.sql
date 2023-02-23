/*
Recreate based on backups

1. Reset the expackages
--truncate
SELECT fnsysMDSExecute('
truncate table s0000v0000.expackage;
truncate table s0000v0000.exgovernancedetail cascade;
truncate table s0000v0000.exgovernance cascade;
truncate table s0000v0000.exrecordgroup cascade;
truncate table s0000v0000.sysdictionarytable cascade;');

2. Switch to Postgres and make sure postgres is highlighted

3. Recreate databases from backup

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000010;
CREATE DATABASE sn000010 WITH TEMPLATE sn000010bak OWNER postgres;
ALTER DATABASE sn000010 SET search_path TO S0100V0000, S0000V0000, public;

-- DROP DATABASE IF EXISTS sn000011;
-- CREATE DATABASE sn000011 WITH TEMPLATE sn000011bak OWNER postgres;
-- ALTER DATABASE sn000011 SET search_path TO S0091V0000, S0000V0000, public;

DROP DATABASE IF EXISTS sn000012;
CREATE DATABASE sn000012 WITH TEMPLATE sn000012bak OWNER postgres;
ALTER DATABASE sn000012 SET search_path TO S0092V0000, S0000V0000, public;

-- DROP DATABASE IF EXISTS sn000013;
-- CREATE DATABASE sn000013 WITH TEMPLATE sn000013bak OWNER postgres;
-- ALTER DATABASE sn000013 SET search_path TO S0093V0000, S0000V0000, public;

DROP DATABASE IF EXISTS sn000014;
CREATE DATABASE sn000014 WITH TEMPLATE sn000014bak OWNER postgres;
ALTER DATABASE sn000014 SET search_path TO S0094V0000, S0000V0000, public;

*/
select '1. Show subscribers, record groups and subscriptions for citizen registry';
/*
SET SEARCH_PATH TO s0101v0000, s0000v0000, public;
call spsysForeignKeyCacheRefresh ();

--RecordGroup
select uid, left('.....................',(topdownlevel-1)*3)||name as name, dictionarytable TableName, Dictionarycolumn ColumnName, WhereClause from vwexrecordgroup where sysdictionarytableid!=120 order by displaysequence;

--Subscriptions
SELECT fnsysidview(a.id)                                                                   AS uid,
       ((c.name::TEXT || ' ('::TEXT) || fnsysidview(a.exrecordgroupid)::TEXT) || ')'::TEXT AS recordgroup,
       coalesce(d.translation,'All records') RecordSubscribedTo,
       ((b.name::TEXT || ' ('::TEXT) || fnsysidview(a.exsubscriberid)::TEXT) || ')'::TEXT  AS subscriber
FROM exsubscription a
LEFT JOIN vwexsubscriber b ON b.id = a.exsubscriberid
LEFT JOIN vwexrecordgroup c ON c.id = a.exrecordgroupid
LEFT JOIN sysforeignkeycache d ON d.sysdictionarytableid=c.sysdictionarytableid and d.rowid=a.rowidsubscribedto
ORDER BY c.name, d.translation, b.name;

--Subscribers
select uid, system from vwexsubscriber
where id in (select exsubscriberid from exsubscription)
order by displaysequence;

*/
select '2. Run data update test';
/*

Jimmy divorces Sally and marries Penny Boire and Penny changes her name to Aabert

Updates that occurred are:
- Change relationship status for Sally and Jimmy
- Create new relationship for Jimmy and Penny
- Change Penny's address to Jimmy's.
- Charge Jimmy and Penny for the registration changes

Exports to systems to notify them of the changes
- Penny holds title to a parcel
- CEO of a corporation
- Goes to system

 */

SET client_min_messages TO ERROR;
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_id                 BIGINT;
        l_JSON               JSON;
        l_idPenny            BIGINT;
        l_idJimmy            BIGINT := fnsysidget(101, 12);
        l_gltransactionid    BIGINT;
        l_glbatchid          BIGINT;
        l_transactiondate    DATE   := now()::date;
    BEGIN
        SET search_path TO s0101v0000, s0000v0000,public;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate(
                p_comment := 'Jimmy Aabert gets divorced from Sally and marries Penny Boire who also divorces',
                p_crmcontactiduser := fnsysidget(101, 22));

        SELECT b.id
        INTO l_id
        FROM crmrelationship b
        WHERE (b.crmcontactid1 = l_idJimmy OR b.crmcontactid2 = l_idJimmy)
          AND b.temporalenddate = '9999-12-31'
          AND b.crmrelationshiptypeid = 50;

        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := NULL::JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 120,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        SELECT id INTO l_idPenny FROM crmcontact WHERE name ILIKE 'Boire, Penny';

        SELECT b.id
        INTO l_id
        FROM crmrelationship b
        WHERE (b.crmcontactid1 = l_idPenny OR b.crmcontactid2 = l_idPenny)
          AND b.temporalenddate = '9999-12-31'
          AND b.crmrelationshiptypeid = 50;

        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := NULL::JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 120,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT l_idJimmy crmcontactid1, l_idPenny crmcontactid2, 50::BIGINT crmrelationshiptypeid) AS a;

        l_id := NULL;

        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := 120,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        UPDATE crmcontact
        SET lastname='Aabert', name='Aabert' || ', ' || firstname, sysChangeHistoryID = l_NewChangeHistoryID
        WHERE id = l_idPenny;

        INSERT INTO crmaddress (comcityid, crmaddressidinheritedfrom, crmaddresstypeid, crmcontactid, addresstext, additionalinformation, address1, address2, address3, address4, city, effectivedate, isprimaryaddress, postalzip, latitude, longitude, provincestate, syschangehistoryid)
        SELECT comcityid,
               crmaddressidinheritedfrom,
               crmaddresstypeid,
               l_idPenny crmcontactid,
               addresstext,
               additionalinformation,
               address1 || ' as of ' || fnTime(),
               address2,
               address3,
               address4,
               city,
               l_transactiondate effectivedate,
               isprimaryaddress,
               postalzip,
               latitude,
               longitude,
               provincestate,
               l_NewChangeHistoryID
        FROM crmaddress
        WHERE crmcontactid = l_idJimmy
          AND crmaddresstypeid = 50;

        INSERT INTO glbatch (glbatchtypeid, description)
        SELECT 1, 'Person Registration Fees'
        RETURNING id INTO l_glbatchid;

        INSERT INTO gltransaction (glbatchid, glpostingstatusid, gltransactiontypeid, description, transactiondate, syschangehistoryid)
        SELECT l_glbatchid, 2, 1, 'Person Registration', l_transactiondate, l_NewChangeHistoryID
        RETURNING id INTO l_gltransactionid;

        INSERT INTO glentry (glaccountid, glcostcentreid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, description, syschangehistoryid)
        SELECT fnsysidGet(0, 1300),
               fnsysidGet(100, 2),
               l_gltransactionid,
               NULL,
               NULL,
               200,
               'Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT fnsysidGet(101, 1),
               fnsysidGet(100, 2),
               l_gltransactionid,
               100,
               l_idJimmy,
               -100,
               'Divorce Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT fnsysidGet(101, 1),
               fnsysidGet(100, 2),
               l_gltransactionid,
               100,
               l_idPenny,
               -100,
               'Marriage Registration Fee',
               l_NewChangeHistoryID;

        CALL spglBatchApprove(l_glbatchid);

        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0101v0000, s0000v0000, public;

    END
$$ LANGUAGE plpgsql;

/*

1. Check results showed up on main server

--Contact
select * from (
select 's0101v0000' system, name, * from s0101v0000.crmcontact where id in (fnsysIDGet(101, 131), fnsysIDGet(101, 12))  union
select 's0102v0000' system, name, * from s0102v0000.crmcontact where id in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) union
select 's0103v0000' system, name, * from s0103v0000.crmcontact where id in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) union
select 's0105v0000' system, name, * from s0105v0000.crmcontact where id in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) union
select 's0109v0000' system, name, * from s0109v0000.crmcontact where id in (fnsysIDGet(101, 131), fnsysIDGet(101, 12))) a
 order by system;

--Address
select * from (
select 's0101v0000' system, * from s0101v0000.vwcrmaddress where crmcontactid in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) and crmaddresstypeid=50 union
select 's0102v0000' system, * from s0102v0000.vwcrmaddress where crmcontactid in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) and crmaddresstypeid=50 union
select 's0103v0000' system, * from s0103v0000.vwcrmaddress where crmcontactid in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) and crmaddresstypeid=50 union
select 's0105v0000' system, * from s0105v0000.vwcrmaddress where crmcontactid in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) and crmaddresstypeid=50 union
select 's0109v0000' system, * from s0109v0000.vwcrmaddress where crmcontactid in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) and crmaddresstypeid=50) a
 order by system;

--Relationships
select 's0101v0000' system, * from s0101v0000.vwcrmrelationship where (crmcontactid1 in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) or crmcontactid2 in (fnsysIDGet(101, 131), fnsysIDGet(101, 12))) and crmrelationshiptypeid=50 and temporalenddate='9999-12-31';

set SEARCH_PATH to s0100v0000, s0000v0000, public;
-- GL Batch
SELECT * FROM vwglbatch where id = (select max(id) from glbatch where fnsysidview(id,'s')=101);
-- GL Transactions
SELECT * FROM vwgltransaction where id = (select max(id) from gltransaction where fnsysidview(id,'s')=101);
-- GL Entries
SELECT * FROM vwglentry where gltransactionid = (select max(id) from gltransaction where fnsysidview(id,'s')=101);

2. Switch to sn000012

--Contact
select 's0107v0000' system, name, * from s0107v0000.crmcontact where id in (fnsysIDGet(101, 131), fnsysIDGet(101, 12));

--Address
select 's0107v0000' system, * from s0107v0000.vwcrmaddress where crmcontactid in (fnsysIDGet(101, 131), fnsysIDGet(101, 12)) and crmaddresstypeid=50 ;

=========================================================================================
Test 020
This test involves
- Frank subdivide parcel  SE-18-30-025-04-8 (a 160 acres parcel) into a lot with 5 acres and a new 155 acre quarter
    o Create a subdivision plan in Frank's name
    o Create a new parcel with 155 acres and another one with 5 acres
    o Deactivate the old parcel
- Then he sells the lot to Jimmy and Penny.
    o Frank become the title holder of the new quarter (less 5 acres)
    o Jimmy and Penny become the title holders for the parcel
- Jimmmy and Penny get a mortgage from Bank of Fairfax so they register
  an interest (encumberance) against the parcel
- Jimmy and Penny change their address to the new lot in the citizen registry
- Jimmy pays the registration fees

This causes the following updates to occur in other systems.
- SE-18-30-025-04-8 has an oil and gas well so the parcel information
is sent to Energy dept along with the title holders
- The municipality where the parcel resides is sent the revised
parcel information plus the name and addresses for Jimmy and Penny
- The executive office receives the financial transaction for the transfer
fees
- The bank is sent the updated information about their interest
plus information about the parcels, their title holders and their
addresses
*/
select '1. Show subscribers, record groups and subscriptions for land registry';
/*
SET SEARCH_PATH TO s0103v0000, s0000v0000, public;
call spsysForeignKeyCacheRefresh ();

--RecordGroup
select uid, left('.....................',(topdownlevel-1)*3)||name as name, dictionarytable TableName, Dictionarycolumn ColumnName, WhereClause from vwexrecordgroup where sysdictionarytableid!=120 order by displaysequence;

--Subscriptions
SELECT fnsysidview(a.id)                                                                   AS uid,
       ((c.name::TEXT || ' ('::TEXT) || fnsysidview(a.exrecordgroupid)::TEXT) || ')'::TEXT AS recordgroup,
       coalesce(d.translation,'All records') RecordSubscribedTo,
       ((b.name::TEXT || ' ('::TEXT) || fnsysidview(a.exsubscriberid)::TEXT) || ')'::TEXT  AS subscriber
FROM exsubscription a
LEFT JOIN vwexsubscriber b ON b.id = a.exsubscriberid
LEFT JOIN vwexrecordgroup c ON c.id = a.exrecordgroupid
LEFT JOIN sysforeignkeycache d ON d.sysdictionarytableid=c.sysdictionarytableid and d.rowid=a.rowidsubscribedto
ORDER BY c.name, d.translation, b.name;

--Subscribers
select uid, system from vwexsubscriber
where id in (select exsubscriberid from exsubscription)
order by displaysequence;


*/
DO -- Subdivide parcel, setup titleholders, record mortgage interest, charge fees.
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_lrplanid           BIGINT;
        l_lrparcelidmain     BIGINT;
        l_lrparcelidlot      BIGINT;
        l_lrparcelidorig     BIGINT := fnsysidget(103, 1);
        l_idPenny            BIGINT := fnsysidget(101, 131);
        l_idJimmy            BIGINT := fnsysidget(101, 12);
        l_idFrank            BIGINT := fnsysidget(101, 9);
        l_JSON               JSON;
        l_gltransactionid    BIGINT;
        l_glbatchid          BIGINT;
        l_transactiondate    DATE   := now()::date;
        l_id                 BIGINT;
    BEGIN
        SET SEARCH_PATH TO s0103v0000, s0000v0000, public;

        l_NewChangeHistoryID := fnsysChangeHistoryCreate(
                p_comment := 'Frank subdivides parcel and turns it over to Jimmy and Penny on ' ||
                             l_transactiondate::VARCHAR,
                p_crmcontactiduser := fnsysidget(101, 4));

        --Create the parcel plan
        INSERT INTO lrplan (crmcontactid, plannumber, planstatus, plantype, planmethod, registrationdate, rowstatus, syschangehistoryid)
        SELECT l_idFrank         crmcontactid,
               plannumber,
               planstatus,
               plantype,
               planmethod,
               l_transactiondate registrationdate,
               rowstatus,
               l_NewChangeHistoryID
        FROM lrplan
        WHERE id = fnsysidget(103, 1)
        RETURNING id INTO l_lrplanid;
        UPDATE lrplan SET plannumber=fnsysidview(l_lrplanid) WHERE id = l_lrplanid;

        --Create the new subdivided quarter
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT legaldescription,
                        atslocation,
                        155               acresgross,
                        surveystatus,
                        'Partial Quarter' parceltype,
                        ownership,
                        155               acresunbroken,
                        waterstatus,
                        purpose
                 FROM lrparcel
                 WHERE id = l_lrparcelidorig
                   AND temporalenddate = '9999-12-31') AS a;
        CALL spsysTemporalDataUpdate(p_Id := l_lrparcelidmain,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(103, 2),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Create the new subdivided lot
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT fnsysidview(l_lrplanid) || '-01' legaldescription,
                        atslocation,
                        5                                acresgross,
                        surveystatus,
                        'Lot'                            parceltype,
                        ownership,
                        5                                acresunbroken,
                        waterstatus,
                        purpose
                 FROM lrparcel
                 WHERE id = l_lrparcelidorig
                   AND temporalenddate = '9999-12-31') AS a;
        l_id := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_lrparcelidlot,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(103, 2),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Register Frank as the titleholder of the new parcel
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT l_lrparcelidmain lrparcelid, l_idFrank crmcontactid) AS a;
        l_id := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(103, 4),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Register Penny as the titleholder of the new lot
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT l_lrparcelidlot lrparcelid, l_idPenny crmcontactid) AS a;
        l_id := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(103, 4),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Register Jimmy as the titleholder of the new lot
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT l_lrparcelidlot lrparcelid, l_idJimmy crmcontactid) AS a;
        l_id := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(103, 4),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Deactivate Frank as title holder for the original quarter
        SELECT id
        INTO l_id
        FROM lrparceltitleholder
        WHERE crmcontactid = l_idFrank AND lrparcelid = l_lrparcelidorig
        LIMIT 1;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := NULL::JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := fnsysidget(103, 4),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Register the mortgage against the lot
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT sysdictionarytableidappliesto, l_lrparcelidlot rowidappliesto, lrinterestid, acres
                 FROM lrparcelinterest
                 WHERE lrinterestid = fnsysidget(103, 101)
                 LIMIT 1) a;
        l_id := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(103, 1),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Create the plan parcel records
        INSERT INTO lrplanparcel (lrparcelid, lrplanid, disposition, syschangehistoryid)
        VALUES (l_lrparcelidlot, l_lrplanid, 'Output', l_NewChangeHistoryID),
               (l_lrparcelidmain, l_lrplanid, 'Output', l_NewChangeHistoryID),
               (l_lrparcelidorig, l_lrplanid, 'Input', l_NewChangeHistoryID);

        --Record the original parcel as deleted
        CALL spsysTemporalDataUpdate(p_Id := l_lrparcelidorig,
                                     p_EffectiveDate := l_transactiondate::DATE,
                                     p_NewData := NULL::JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := fnsysidget(103, 2),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        l_NewChangeHistoryID := fnsysChangeHistoryCreate(
                p_comment := 'Charge Frank, Jimmy and Penny for land registration updates',
                p_crmcontactiduser := fnsysidget(101, 4));

        INSERT INTO glbatch (glbatchtypeid, description)
        SELECT 1, 'Land Registration Fees'
        RETURNING id INTO l_glbatchid;

        INSERT INTO gltransaction (glbatchid, glpostingstatusid, gltransactiontypeid, description, transactiondate, syschangehistoryid)
        SELECT l_glbatchid, 2, 1, 'Land Registration Fees', l_transactiondate, l_NewChangeHistoryID
        RETURNING id INTO l_gltransactionid;

        INSERT INTO glentry (glaccountid, glcostcentreid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, description, syschangehistoryid)
        SELECT 1300, -- Bank Account
               fnsysidGet(100,4),
               l_gltransactionid,
               NULL,
               NULL,
               200,
               'Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT fnsysidGet(103, 1),
               fnsysidGet(100, 4),
               l_gltransactionid,
               100,
               l_idJimmy,
               -200,
               'Plan Registration Fee',
               l_NewChangeHistoryID;

        CALL spglBatchApprove(l_glbatchid);

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto, effectivedate, expirydate, syschangehistoryid)
        SELECT exsubscriberid, exrecordgroupid, l_lrparcelidlot, effectivedate, expirydate, l_NewChangeHistoryID
        FROM exsubscription
        WHERE rowidsubscribedto = l_lrparcelidorig
          AND exrecordgroupid = (
                                    SELECT id
                                    FROM exrecordgroup
                                    WHERE name ILIKE 'land parcel');

        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto, effectivedate, expirydate, syschangehistoryid)
        SELECT exsubscriberid, exrecordgroupid, l_lrparcelidmain, effectivedate, expirydate, l_NewChangeHistoryID
        FROM exsubscription
        WHERE rowidsubscribedto = l_lrparcelidorig
          AND exrecordgroupid = (
                                    SELECT id
                                    FROM exrecordgroup
                                    WHERE name ILIKE 'land parcel');

        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET search_path TO s0101v0000, s0000v0000,public;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate(
                p_comment := 'Jimmy and Penny update their address',
                p_crmcontactiduser := fnsysidget(101, 22));

        UPDATE crmaddress
        SET address1='RR3 SE-18-30-025-04-8', Address2='Lot Plan ' || fnsysidview(l_lrplanid),
            address3='As of ' || fnTime(), postalzip='T8N9N9', syschangehistoryid=l_NewChangeHistoryID
        WHERE crmcontactid IN (l_idJimmy, l_idPenny)
          AND crmaddresstypeid = 50;

        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0103v0000, s0000v0000, public;
        CALL spsysForeignKeyCacheRefresh();

        --RAISE NOTICE 'lrparcelidmain % lrparcelidlot % lrparcelidorig % lrplanid % glbatchid %', l_lrparcelidmain, l_lrparcelidlot, l_lrparcelidorig, l_lrplanid, l_glbatchid;

    END
$$ LANGUAGE plpgsql;
/*

1. Check results showed up on land registry

SET SEARCH_PATH TO s0103v0000, s0000v0000, public;
-- Contacts
SELECT * FROM vwcrmcontact WHERE id in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a';
-- Addresses
SELECT * FROM vwcrmaddress WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a' and crmaddresstypeid=50;
--Parcels
select * from vwlrparcel where temporalenddate='9999-12-31' and id in (SELECT lrparcelid FROM lrplanparcel WHERE lrplanid = (SELECT id FROM lrplan WHERE crmcontactid = fnsysidget(101,9) order by id desc limit 1));
-- Parcel Title Holders
SELECT * FROM vwlrparceltitleholder WHERE crmcontactid in (fnsysidget(101,9),fnsysidget(101,12), fnsysidget(101,131)) AND temporalstartdate <= now()::DATE AND temporalenddate >= now()::DATE and rowstatus='a' order by titleholder, temporalstartdate;
-- Plans
SELECT * FROM vwlrplan WHERE crmcontactid = fnsysidget(101,9) order by id desc limit 1;
-- Plans->Plan Parcels
SELECT * FROM vwlrplanparcel WHERE lrplanid = (SELECT id FROM lrplan WHERE crmcontactid = fnsysidget(101,9) order by id desc limit 1);
--Interests
select * from vwlrinterest where id in (select lrinterestid from vwlrparcelinterest order by id desc limit 1) and temporalenddate='9999-12-31';
--ParcelInterests
select * from vwlrparcelinterest order by id desc limit 1;
-- GL Entries
SELECT * FROM vwglentry WHERE rowidchargedto in (fnsysidget(101,12), fnsysidget(101,131)) and sysdictionarytableidchargedto=100 and rowstatus='a';

2. Check if the energy department got the update
set SEARCH_PATH to s0104v0000, s0000v0000, public;
-- Contacts
SELECT * FROM vwcrmcontact WHERE id in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a';
-- Addresses
SELECT * FROM vwcrmaddress WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a' and crmaddresstypeid=50;
--Parcels
select * from vwlrparcel where id in (1030000000045257,1030000000045256,1030000000000001) and temporalenddate = '9999-12-31' order by id, temporalstartdate;
-- Parcel Title Holders
SELECT * FROM vwlrparceltitleholder WHERE crmcontactid in (fnsysidget(101,9),fnsysidget(101,12), fnsysidget(101,131)) AND temporalenddate = '9999-12-31'::DATE order by parcel, titleholder;

3. Check if the executive group go the updated transaction

set SEARCH_PATH to s0100v0000, s0000v0000, public;
-- GL Batch
SELECT * FROM vwglbatch where id = (select max(id) from glbatch where fnsysidview(id,'s')=103);
-- GL Transactions
SELECT * FROM vwgltransaction where id = (select max(id) from gltransaction where fnsysidview(id,'s')=103);
-- GL Entries
SELECT * FROM vwglentry where gltransactionid = (select max(id) from gltransaction where fnsysidview(id,'s')=103);

4. Switch to sn000014 (Bank) and check if they got the update

set SEARCH_PATH to s0120v0000, s0000v0000, public;
--Interests
select * from vwlrinterest where id in (select lrinterestid from vwlrparcelinterest order by id desc limit 1) and temporalenddate='9999-12-31';
--ParcelInterests
select * from vwlrparcelinterest order by id desc limit 1;
--Parcels
select * from vwlrparcel where id in (1030000000045257) and temporalenddate='9999-12-31';
-- Parcel Title Holders
SELECT * FROM vwlrparceltitleholder WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) AND temporalenddate = '9999-12-31'::DATE and rowstatus='a' order by id, temporalstartdate;
-- Contacts
SELECT * FROM vwcrmcontact WHERE id in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a';
-- Addresses
SELECT * FROM vwcrmaddress WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a' and crmaddresstypeid=50;

5. Switch to sn000012 (Municipality) and see if they got the update

set SEARCH_PATH to s0107v0000, s0000v0000, public;
-- Contacts
SELECT * FROM vwcrmcontact WHERE id in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a';
-- Addresses
SELECT * FROM crmaddress WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a' and crmaddresstypeid=50;
--Parcels
select * from vwlrparcel where id in (1030000000045257,1030000000045256,1030000000000001) AND temporalenddate = '9999-12-31'::DATE;
-- Parcel Title Holders
SELECT * FROM vwlrparceltitleholder WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) AND temporalenddate = '9999-12-31'::DATE and rowstatus='a' order by id, temporalstartdate;


=========================================================================================
Test 030
This test involves the municipality updating the taxroll for the new quarter, creating a taxroll for the new lot
and adding assessment.

These updates need to automatically flow to the municipal department.
*/
select '1. Show subscribers, record groups and subscriptions for municipal';
/*

SET SEARCH_PATH TO s0107v0000, s0000v0000, public;
call spsysForeignKeyCacheRefresh ();

--RecordGroup
select uid, left('.....................',(topdownlevel-1)*3)||name as name, dictionarytable TableName, Dictionarycolumn ColumnName, WhereClause from vwexrecordgroup where sysdictionarytableid!=120 order by displaysequence;

--Subscriptions
SELECT fnsysidview(a.id)                                                                   AS uid,
       ((c.name::TEXT || ' ('::TEXT) || fnsysidview(a.exrecordgroupid)::TEXT) || ')'::TEXT AS recordgroup,
       coalesce(d.translation,'All records') RecordSubscribedTo,
       ((b.name::TEXT || ' ('::TEXT) || fnsysidview(a.exsubscriberid)::TEXT) || ')'::TEXT  AS subscriber
FROM exsubscription a
LEFT JOIN vwexsubscriber b ON b.id = a.exsubscriberid
LEFT JOIN vwexrecordgroup c ON c.id = a.exrecordgroupid
LEFT JOIN sysforeignkeycache d ON d.sysdictionarytableid=c.sysdictionarytableid and d.rowid=a.rowidsubscribedto
ORDER BY c.name, d.translation, b.name;

--Subscribers
select uid, system from vwexsubscriber
where id in (select exsubscriberid from exsubscription)
order by displaysequence;

*/
DO -- Setup taxroll and assessments which will flow back to Municipal department
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_oldChangeHistoryID BIGINT;
        l_mttaxrollid           BIGINT;
        l_lrparcelidmain     BIGINT;
        l_lrparcelidlot      BIGINT;
        l_lrparcelidorig     BIGINT := fnsysidget(103, 1);
        l_idJimmy            BIGINT := fnsysidget(101, 12);
        l_JSON               JSON;
        l_transactiondate    DATE   := now()::date;
        l_id                 BIGINT;
    BEGIN
        SET SEARCH_PATH TO s0107v0000, s0000v0000, public;

        l_NewChangeHistoryID := fnsysChangeHistoryCreate(
                p_comment := 'New parcels are setup with a taxroll and assessments are added on ' ||
                             l_transactiondate::VARCHAR,
                p_crmcontactiduser := fnsysidget(107, 1));

        select max(syschangehistoryid) into l_oldchangehistoryid from lrparcel;
        select id into l_lrparcelidlot from lrparcel where syschangehistoryid=l_oldchangehistoryid and acresgross=5 limit 1;
        select id into l_lrparcelidmain from lrparcel where syschangehistoryid=l_oldchangehistoryid and acresgross=155 limit 1;

        INSERT INTO mttaxroll (crmcontactid, status, type, disposition, syschangehistoryid)
        select l_idJimmy, 'Active', 'Titled', 'TL - Titled', l_NewChangeHistoryID
        returning id into l_mttaxrollid;

        UPDATE mttaxroll set taxrollnumber='TL-'||fnsysidview(l_mttaxrollid) where id=l_mttaxrollid;

        update lrparcel set mttaxrollid=l_mttaxrollid, syschangehistoryid=l_NewChangeHistoryID where id=l_lrparcelidlot
        and temporalenddate='9999-12-31';

        update lrparcel set mttaxrollid=(select mttaxrollid from lrparcel where id=l_lrparcelidorig and temporalenddate='9999-12-31'), syschangehistoryid=l_NewChangeHistoryID
        where id=l_lrparcelidmain and temporalenddate='9999-12-31';

        --Setup assessment for lot
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT l_lrparcelidlot lrparcelid, 5000 land, 3000 building, '2022-12-31'::date assessmentdate, '7-Farm/Residence Site' landuse ) AS a;
        l_id := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(105, 2),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        --Setup assessment for main parcel
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT l_lrparcelidmain lrparcelid, 15000 land, 0 building, '2022-12-31'::date assessmentdate, '11-Farmland/Pasture' landuse ) AS a;
        l_id := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := fnsysidget(105, 2),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0092v0000, s0000v0000, public;
        CALL spexpackagedistribute();

        --RAISE NOTICE 'lrparcelidmain % lrparcelidlot % lrparcelidorig % mttaxrollid %', l_lrparcelidmain, l_lrparcelidlot, l_lrparcelidorig, l_mttaxrollid;

    END
$$ LANGUAGE plpgsql;
/*

1. Check results on sn00012

SET SEARCH_PATH TO s0107v0000, s0000v0000, public;
-- Contacts
SELECT * FROM vwcrmcontact WHERE id in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a';
-- Addresses
SELECT * FROM vwcrmaddress WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a' and crmaddresstypeid=50;
-- Taxrolls
select * from vwmttaxroll where id=1070000000037534
-- Parcels
select * from lrparcel where id in (1030000000045256, 1030000000045257,1030000000000001) and temporalenddate='9999-12-31';

2. Check results on sn00010

SET SEARCH_PATH TO s0105v0000, s0000v0000, public;
-- Contacts
SELECT * FROM vwcrmcontact WHERE id in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a';
-- Addresses
SELECT * FROM vwcrmaddress WHERE crmcontactid in (fnsysidget(101,12), fnsysidget(101,131)) and rowstatus='a' and crmaddresstypeid=50;
-- Taxrolls
select * from vwmttaxroll where id=1070000000037534
-- Parcels
select * from lrparcel where id in (1030000000045256, 1030000000045257,1030000000000001) and temporalenddate='9999-12-31';

Check results
--Switch to sn000010
Select * from s0103v0000.lrparcel where id=fnsysidget(103, 1) order by temporalstartdate;
select * from s0103v0000.syschangehistory order by id desc

--Switch to sn000012

select * from s0107v0000.lrparcel where id=fnsysidget(103, 1) order by temporalstartdate;

=========================================================================================
Test 040
This test involves updating the data warehouse with all the updates that have been done and then
running queries against the database to check the results.

*/

SET client_min_messages TO NOTICE;
DO
$$
    DECLARE
        l_schemadatawarehouse VARCHAR := 's0111v0000';
    BEGIN
        SET SEARCH_PATH TO s0111v0000, s0000v0000, public;
        CALL spsysDatawarehouseCreate(
                p_schemalist := 's0100v0000, s0101v0000, s0102v0000, s0103v0000, s0104v0000, s0105v0000, s0108v0000, s0109v0000',
                p_schemadatawarehouse := l_schemadatawarehouse);
    END;
$$ LANGUAGE plpgsql;
CALL spsysForeignKeyCacheRefresh();
CALL spsysMasterDataIndexGenerate();

select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Penny', p_level:=1);
select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Penny', p_level:=1, p_querydate:='2022-12-10');
select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Penny', p_level:=4, p_querydate:='2022-12-10');

select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Jimmy', p_level:=1);

select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert,%', p_level:=3);
select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert%', p_level:=3, p_foreigntable:='og%');

select fnsysMasterDataQuery (p_table:='crmcontact', p_whereclause:='a.lastname ilike ''aab%'' and year(a.birthdate) > 1970', p_level:=1);
select fnsysMasterDataQuery (p_table:='lrparcel', p_whereclause:='a.acresunbroken < 10', p_level:=1);
select fnsysMasterDataQuery (p_queryfilter:='SE-18-30-025-04-8', p_level:=3);

-- Check out parcel plans
select fnsysMasterDataQuery (p_queryfilter:='103-1-1', p_level:=3);
--copy paste the ids into a list
select fnsysMasterDataQuery (p_table:='lrparcel', p_whereclause:='a.id in (select lrparcelid from lrplanparcel aa where lrplanid=fnsysidget(103,1))', p_level:=3);
select fnsysMasterDataQuery (p_queryfilter:='103-001-01-01', p_level:=3);
-- grab data on encana
select fnsysMasterDataQuery (p_queryfilter:='Encana Infrastructure Resources%', p_level:=3);
select fnsysMasterDataQuery (p_table:='ogwell', p_whereclause:='a.id in (1040000000000881,1040000000000161,1040000000000150,1040000000000245,1040000000000049,1040000000008547,1040000000000164,1040000000000166,1040000000000159,1040000000000758,1040000000004955,1040000000000001,1040000000000246,1040000000000149)', p_level:=3);
select fnsysMasterDataQuery (p_table:='actproject', p_whereclause:='a.id=1090000000001199', p_level:=3);
select fnsysMasterDataQuery (p_table:='actproject', p_whereclause:='a.contact ilike ''Saif Hlovyak''', p_level:=3);
/*
=========================================================================================
Test 050
This test involves transferring goverance from one entity to the next
*/

SET SEARCH_PATH TO s0107v0000, s0000v0000, public;

--RecordGroup
SELECT *
FROM vwexrecordgroup
ORDER BY displaysequence;

--Transfer Governance to other municipality
CALL spexGoveranceTransfer(p_exRecordGroupID := 1070000000000001
    , p_id := 1070000000000001
    , p_exSystemID := 106);

insert into exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto)
select 1070000000000002, 1070000000000001, 1070000000000001;

call spexpackageexport();

-- Check governance on main server
SELECT * FROM vwexGovernanceAWS ORDER BY exrecordgroup;

-- Check governor of record
SELECT fnexisGovernor(1050000000000001, 1070000000000001);

/*
=========================================================================================
Test 030
This test changes a record grouping so that additional data is included.  In this case,
we have an existing group which consists of a contact and their address.  This group is extended
to include a spousal relationship and the details of the spouse.

As a result, any system that already has a subscription to a contact will automatically get
a spousal relationship and the spouse details.  Also, any time this data changes they will
get an update.
*/

DO -- Test exporting spousal relationships
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_id                 BIGINT;
    BEGIN

        SET SEARCH_PATH TO s0101v0000, s0000v0000, public;
        SELECT exrecordgroupidparent
        INTO l_id
        FROM exrecordgroup
        WHERE sysdictionarycolumnid = 11900
        ORDER BY id
        LIMIT 1;

        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Add spousal relationships to contact ');
        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, whereclause, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        SELECT l_id,
               120,
               12710,
               'Spousal Relationhip (Primary)',
               'crmrelationshiptypeid=50',
               2,
               3,
               'a',
               l_NewChangeHistoryID
        UNION
        SELECT l_id,
               120,
               12720,
               'Spousal Relationhip (Secondary)',
               'crmrelationshiptypeid=50',
               2,
               4,
               'a',
               l_NewChangeHistoryID;

        PERFORM fnsysHierarchyUpdate('exrecordgroup', 'id');

        CALL spexPackageExport(l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0101v0000, s0000v0000, public;

    END;
$$ LANGUAGE plpgsql;
/*

=========================================================================================
Test 040
This test involves changing a parcel to have updated temporal data for a given
time period.

The test verifies that the temporal data management function works properly and that
the changes get properly communicated to the systems that have an interest in the record.

*/

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_lrparcelidorig     BIGINT := fnsysidget(103, 1);
        l_JSON               JSON;
        l_transactiondate date := '2022-12-09';
    BEGIN
        SET SEARCH_PATH TO s0103v0000, s0000v0000, public;

        l_NewChangeHistoryID := fnsysChangeHistoryCreate(
                p_comment := 'Change parcel 1 to crown effective ' ||
                             l_transactiondate::VARCHAR,
                p_crmcontactiduser := fnsysidget(101, 4));

        --Update existing quarter
        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT legaldescription,
                        atslocation,
                        acresgross,
                        surveystatus,
                        parceltype,
                        'Crown' ownership,
                        acresunbroken,
                        waterstatus,
                        purpose
                 FROM lrparcel
                 WHERE id = l_lrparcelidorig
                   AND temporalenddate = '9999-12-31') AS a;
        CALL spsysTemporalDataUpdate(p_Id := l_lrparcelidorig,
                                     p_EffectiveDate := l_transactiondate,
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := fnsysidget(103, 2),
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        --RAISE NOTICE 'lrparcelidmain % lrparcelidlot % lrparcelidorig % lrplanid % glbatchid %', l_lrparcelidmain, l_lrparcelidlot, l_lrparcelidorig, l_lrplanid, l_glbatchid;

    END
$$ LANGUAGE plpgsql;

/*

Other tests
- add a task to a project that is being shared and then have consulting company perform activities
- change master data and have it be distributed
- change a corporate name
- change oil well that resides on property
- add a column to core table and populate it
- change a name, and or address and have it flow through to the bank
- create updated assessments and have them flow through to municipal dept
- create oil and gas company that exchanges data with oil co
- add in spousal relationship

Additional development
- make contact data temporal
- depersonalize contact data
*/