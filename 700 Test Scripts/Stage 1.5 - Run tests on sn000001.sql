/*
This file is part of the 3D Enterprise System Platform. The 3D Enterprise System Platform is free
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
This script contains test for the stage 1 test environment.
---------------------------------------
Instructions

Recreate based on backups

1. Switch to Postgres and make sure postgres is highlighted

2. Re-run Setup Master Data Server
-- Not doing so can cause failures

3. Recreate databases from backup

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname like 'sn%'
AND datname != 'sn000000'
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS sn000001;
CREATE DATABASE sn000001 WITH TEMPLATE sn000001bak OWNER postgres;
ALTER DATABASE sn000001 SET search_path TO S0001V0000, S0000V0000, public;

DROP DATABASE IF EXISTS sn000002;
CREATE DATABASE sn000002 WITH TEMPLATE sn000002bak OWNER postgres;
ALTER DATABASE sn000002 SET search_path TO S0004V0000, S0000V0000, public;

========================================================================================
Test 0.

This tests adds a remote system to a group that will receive many updates throughout this test script.

Prior to running this test, sn000003 must be setup.

Running it twice will cause a unique constraint to fire.

Note: the exPackageImport is automatically executed when new records are added to exHistory by the distribute process

Tests - a quote in the subscriber name as well

*/
SET SEARCH_PATH TO DEFAULT;
--SET client_min_messages TO notice;
SET client_min_messages TO warning;
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        DECLARE
    BEGIN

        /*
               When we want to track change history, we call the following procedure to setup the change history
               event.
         */
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Setup subscription to system on another computer');

/*
        When an organization needs to add a new destination system to its list of systems that it exchanges data with
        it needs to be setup locally.  This fnsysMDSGetSystem is called to set this system up.  If the
        destination system is secured, the organization will need to provide an encryption key.
 */
        PERFORM fnsysMDSGetSystem(21);

/*
        The following statement sets up a record in the subscriber table for system 21.  System 21 is getting added to
        subscriber group 1.
 */
        INSERT INTO exsubscriber (exsubscriberidparent, exsystemid, name, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (fnsysIDGet(1, 2), 21, 'Organization 1.4.21''s', NULL, NULL, 'a', l_NewChangeHistoryID);

        -- Updates the subscriber hierarchy after the new record is added
        PERFORM fnsysHierarchyUpdate('exsubscriber', 'exsystemid');

    END;
$$ LANGUAGE plpgsql;
/*
 -- Shows the subscriber table with the new entry
select uid, left('......',(topdownlevel-1)*3)||name as name, system from vwexsubscriber order by displaysequence;

=========================================================================================
Test 1
 Create a subscription to a record for "Clayton Barnes for subscriber group 1.
 This will cause "Clayton Barnes's record to be distributed to System 5, 6 and 21.
 Note that System 5 and 6 exists on sn000001 and System 21 exists on sn000003

 This tests:
 - will it properly process a subscription to a single record that contains child data
 - will is distribute to multiple servers
 */

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN
        -- Setup the change history event
        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Create subscription to Jacquie McNinch for System 5 & 6 & 21');
--         Create the subscription
        INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, rowidsubscribedto, syschangehistoryid)
        VALUES (fnsysIDGet(1, 1), fnsysIDGet(1, 1), fnsysIDGet(1, 128), l_NewChangeHistoryID);
        -- Call the package export.  This procedure performs many functions related to export data
        -- In short, it checks if any of the subscription related data has changed (groups, subscribers
        -- subscriptions) and then exports whatever needs to be exported.  It also checks if any master
        -- data has changed and distributes that as well.  It will export header data for
        -- foreign key references.  This data is communicated to the package distribute process.
        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        -- This procedure runs the distribute process on the 0002 server which pulls down
        -- packages from the master subnet server and then combines it with data it has
        -- received directly from the Package Export process.  If data retrieved from the
        -- export process is found that needs to be communicated to a different server
        -- it will be written to the Master subnet server.  Normally this process would not
        -- be called directly.  Instead it would run on an automated schedule.
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*

2. Check data on sn000001
--Contact
select 's0001v0000' System, UID, Name, crmgenderid, sysCHangeHistoryID from s0001v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0005v0000' System, UID, Name, crmgenderid, sysCHangeHistoryID from s0005v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0006v0000' System, UID, Name, crmgenderid, sysCHangeHistoryID from s0006v0000.vwcrmcontact where id=fnsysIDGet(1, 128);

--Address
select * from (
select 's0001v0000' System, UID, Address1, sysCHangeHistoryID from s0001v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0005v0000' System, UID, Address1, sysCHangeHistoryID from s0005v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0006v0000' System, UID, Address1, sysCHangeHistoryID from s0006v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128)
) a order by System, UID;

3. Check data on sn000003

select 's0021v0000' SystemName, Name, crmgenderid, sysCHangeHistoryID from s0021v0000.vwcrmcontact where id=fnsysIDGet(1, 128);
select 's0021v0000' SystemName, id, Address1, sysCHangeHistoryID from s0021v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128);

=========================================================================================
Test 2
 Create subscription to the employee group for a contact named Anderson Esopenko for System 4 on sn00002.
 Note this group contains the following data

Employee
..Employee Detail
..Address
..Email
..Phone
..BillingAccount
....GL Entry

This tests
- will it properly traverse a record group with a three level hierarchy.
- will it distribute updates to local and remote servers

1. Setup the subscription
*/

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Create subscription to Anderson Esopenko for System 4 on sn00002');
        -- Create the subscription
        INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, rowidsubscribedto, syschangehistoryid)
        VALUES (fnsysIDGet(1, 5), fnsysIDGet(1, 8), fnsysIDGet(1, 72), l_NewChangeHistoryID);
        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*
2. switch to sn000002 and distribute

set SEARCH_PATH to s0003v0000, s0000v0000, public;
CALL spexpackagedistribute();
set SEARCH_PATH to default;

3. Check the results
--Contact
select Name, * from s0004v0000.vwcrmcontact where id=fnsysIDGet(1, 72);
--Address
select * from s0004v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 72);
--AddressPhone
select * from s0004v0000.vwcrmaddressphone where crmcontactid=fnsysIDGet(1, 72);
--BillingAccount
select * from s0004v0000.vwglbillingaccount where crmcontactid=fnsysIDGet(1, 72);
--GlEntry
select * from s0004v0000.vwglentry where rowidchargedto=(select id from s0004v0000.vwglbillingaccount where crmcontactid=fnsysIDGet(1, 72));
--GLTransaction
select * from s0004v0000.vwgltransaction where id in (select gltransactionid from glentry where rowidchargedto=(select id from s0004v0000.vwglbillingaccount where crmcontactid=fnsysIDGet(1, 72)));

4. switch back to sn000001

=========================================================================================
Test 3
Setup subscription to ratetype table for system 5, 6 and 21.

This tests:
- will it process a subscription to a table
- will it filter out the PST record
- will it communicate these updates to local and remote servers

1. Setup the subscription
*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Setup subscription to ratetype table for system 5, 6 and 21');
        INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, rowidsubscribedto, syschangehistoryid)
        VALUES (fnsysIDGet(1, 11), fnsysIDGet(1, 1), NULL, l_NewChangeHistoryID);
        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;
/*
2. Check the results
--glratetype
select * from (
select 's0001v0000' system,* from s0001v0000.vwglratetype union
select 's0005v0000' system,* from s0005v0000.vwglratetype union
select 's0006v0000' system,* from s0006v0000.vwglratetype
) a order by system, id;

3. Switch to sn000003

select * from s0021v0000.vwglratetype order by id;

=========================================================================================
Test 4
Setup subscription to all glrate records (temporal) for system 5, 6 and 21.  This is going
to be used to test temporal updates to records

This tests:
- will it transfer subscriptions for temporal data
- will it transfer to both local and remote servers

1. Setup the subscription

*/

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Setup subscription to all glrate records (temporal) for system 5, 6 and 21');
        INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, rowidsubscribedto, syschangehistoryid)
        VALUES (fnsysIDGet(1, 10), fnsysIDGet(1, 1), NULL, l_NewChangeHistoryID);
        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;
/*
2. Check for the results

--glrate
select * from (
select 's0005v0000' System, * from s0005v0000.vwglrate union all
select 's0006v0000' System, * from s0006v0000.vwglrate
) a
order by system, id, temporalstartdate;

3. Switch to sn000003

select * from s0021v0000.vwglrate order by id;

=========================================================================================
Test 5
 Setup subscription to GHY support project (which includes allocations) to a group which includes system 4 and 6.
 This tests:
 - will it properly distribute updates to two different servers
 - will it move child data in a record group
 - will it setup the foreign key references for contact data for project allocations
*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_exSubscriptionID   BIGINT;
    BEGIN

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Setup subscription to GHY support project group which includes system 4 and 6');
        INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, rowidsubscribedto, syschangehistoryid)
        VALUES (fnsysIDGet(1, 9), fnsysIDGet(1, 5), fnsysIDGet(1, 149770), l_NewChangeHistoryID)
        RETURNING ID INTO l_exSubscriptionID;

        INSERT INTO exsubscriptionredaction (exsubscriptionid, sysdictionarycolumnidredacted, redactedsql, redactedvalue, redactedtranslation, syschangehistoryid)
        VALUES (l_exSubscriptionID, fnsysIDGet(0, 10690), '(select crmcontactidcompany from glsetup)', '10000000000095', 'M1 Software Inc.', l_NewChangeHistoryID);

        INSERT INTO exsubscriptionredaction (exsubscriptionid, sysdictionarycolumnidredacted, redactedsql, redactedvalue, redactedtranslation, syschangehistoryid)
        VALUES (l_exSubscriptionID, fnsysIDGet(0, 10550), 'null', 'null', 'null', l_NewChangeHistoryID);

        INSERT INTO exsubscriptionredaction (exsubscriptionid, sysdictionarycolumnidredacted, redactedsql, redactedvalue, redactedtranslation, syschangehistoryid)
        VALUES (l_exSubscriptionID, fnsysIDGet(0, 10590), '''Redact Project Name''', 'Redact Project Name', 'Redact Project Name', l_NewChangeHistoryID);

        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;
/*
--Project
select * from s0001v0000.vwactproject where id=fnsysIDGet(1,149770 );
--ProjectResourceAllocation
select * from s0001v0000.vwactprojectresourceallocation where actprojectid=fnsysIDGet(1,149770 );
--Contact
select * from s0001v0000.vwcrmcontact order by id;

--Project
select * from s0006v0000.vwactproject where id=fnsysIDGet(1,149770 );
--ProjectResourceAllocation
select * from s0006v0000.vwactprojectresourceallocation where actprojectid=fnsysIDGet(1,149770 );
--Contact
select * from s0006v0000.vwcrmcontact order by id;

2. Switch to sn000002

set SEARCH_PATH to s0003v0000, s0000v0000, public;
CALL spexpackagedistribute();
set SEARCH_PATH to default;

3. Check for data

--Project
select * from s0004v0000.vwactproject where id=fnsysIDGet(1,149770 );
--ProjectResourceAllocation
select * from s0004v0000.vwactprojectresourceallocation where actprojectid=fnsysIDGet(1,149770 );
--Contact
select * from s0004v0000.vwcrmcontact order by id;

4. Switch to sn000001

=========================================================================================
Test 6
This test adds system 7 into a hierarchical system group at the third level.  This should
cause system 7 to get contact data, glrate and glratetype data

This tests:
- If we add a system to a group will it automatically cause it to get all the data
the group gets
- If we add it to the 3rd level, will it properly traverse the hierarchy to find it

 */

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN

        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Add system 7 to a subscription group 1');
        INSERT INTO exsubscriber (exsubscriberidparent, exsystemid, name, displaysequence, topdownlevel, rowstatus, syschangehistoryid)
        VALUES (fnsysIDGet(1, 2), 7, 'Organization 1.4.7', NULL, NULL, 'a', l_NewChangeHistoryID);
        PERFORM fnsysHierarchyUpdate('exsubscriber', 'exsystemid');

        CALL spexPackageExport(p_sysChangeHistoryID := l_NewChangeHistoryID);

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*

--Contact
select * from s0007v0000.vwcrmcontact where id=fnsysIDGet(1, 128);
--Phone
select * from s0007v0000.vwcrmaddressphone where crmcontactid=fnsysIDGet(1, 128);
--Email
select * from s0007v0000.vwcrmaddressemail where crmcontactid=fnsysIDGet(1, 128);
--GlRate
select * from s0007v0000.vwglrate order by id;
--GlRateType
select * from s0007v0000.vwglratetype order by id;


=========================================================================================
Test 7
This test checks if special characters cause an issue.

 */

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN
        --TODO Is undo losing a '?

        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Special character test');
        UPDATE crmcontact
        SET name=name || ' as of ' || NOW()::TIME::VARCHAR || ' '' " : !@#$%^&*()`~;{}[]-+|\<>?/ ', sysChangeHistoryID=l_NewChangeHistoryID
        WHERE id = fnsysIDGet(1, 128);

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export updated contact and address'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END
$$ LANGUAGE plpgsql;

/*

--Contact
select * from (
select 's0001v0000' System, Name, sysCHangeHistoryID from s0001v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0005v0000' System, Name, sysCHangeHistoryID from s0005v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0006v0000' System, Name, sysCHangeHistoryID from s0006v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0007v0000' System, Name, sysCHangeHistoryID from s0007v0000.vwcrmcontact where id=fnsysIDGet(1, 128)
) a order by System

=========================================================================================
Test 8
Now undo the previous change and make sure the undo doesnt have an issue with special
characters

*/

DO
$$
    DECLARE
        l_syschangehistoryid BIGINT;
    BEGIN

        SELECT MAX(syschangehistoryid)
        INTO l_syschangehistoryid
        FROM syschangehistoryrow;

        CALL spsysChangeHistoryUndo(p_sysChangeHistoryIDUndo := l_syschangehistoryid,
                                    p_sysChangeHistoryIDNew := fnsysChangeHistoryCreate('Undo of special character change'));

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export undo of special character change'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexPackageDistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;
/*

--Contact
select * from (
select 's0001v0000' System, Name, sysCHangeHistoryID from s0001v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0005v0000' System, Name, sysCHangeHistoryID from s0005v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0006v0000' System, Name, sysCHangeHistoryID from s0006v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0007v0000' System, Name, sysCHangeHistoryID from s0007v0000.vwcrmcontact where id=fnsysIDGet(1, 128)
) a order by system

=========================================================================================
Test 9
This test updates the name for Clayton Barnes to include an x and two new new addresses for him.

This test:
1. Will updates flow through to local and remote subnets
2. If an address is added (which is second level entry in the subscription group) will it flow through to the systems
3. If an address is added that is not of the appropriate type will it be screened out

-- Show all systems that will get an update for this record
SELECT DISTINCT b.exsystemid
FROM exsubscriptiondetail a
JOIN exsubscriber b
     ON b.id = a.exsubscriberid
WHERE rowidsubscribedto=fnsysIDGet(1, 128)
ORDER BY b.exsystemid

*/

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Update contact name and include new address record');
        UPDATE crmcontact
        SET name=leftfind(name, ' as of ') || ' as of ' || NOW()::TIME::VARCHAR, crmgenderid=20, sysChangeHistoryID=l_NewChangeHistoryID
        WHERE id = fnsysIDGet(1, 128);

        INSERT INTO crmaddress (comcityid, crmaddressidinheritedfrom, crmaddresstypeid, crmcontactid, additionalinformation, address1, address2, address3, address4, city, effectivedate, isprimaryaddress, postalzip, provincestate, syschangehistoryid)
        SELECT comcityid,
               crmaddressidinheritedfrom,
               crmaddresstypeid,
               crmContactID,
               additionalinformation,
               'New address as of ' || NOW()::TIME::VARCHAR,
               address2,
               address3,
               address4,
               city,
               effectivedate,
               isprimaryaddress,
               postalzip,
               provincestate,
               l_NewChangeHistoryID
        FROM crmaddress c
        WHERE crmContactID = fnsysIDGet(1, 128)
        LIMIT 1;

        -- The following address should not be broadcast because it is not of type 30 (which is business)
        INSERT INTO crmaddress (comcityid, crmaddressidinheritedfrom, crmaddresstypeid, crmcontactid, additionalinformation, address1, address2, address3, address4, city, effectivedate, isprimaryaddress, postalzip, provincestate, syschangehistoryid)
        SELECT comcityid,
               crmaddressidinheritedfrom,
               10 crmaddresstypeid,
               crmContactID,
               additionalinformation,
               'New address as of ' || NOW()::TIME::VARCHAR,
               address2,
               address3,
               address4,
               city,
               effectivedate,
               isprimaryaddress,
               postalzip,
               provincestate,
               l_NewChangeHistoryID
        FROM crmaddress c
        WHERE crmContactID = fnsysIDGet(1, 128)
        LIMIT 1;

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export updated contact and address'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*
2. Check for data on sn000001

--Contact
select * from (
select 's0005v0000' System, Name, crmgenderid, sysCHangeHistoryID from s0005v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0006v0000' System, Name, crmgenderid, sysCHangeHistoryID from s0006v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0007v0000' System, Name, crmgenderid, sysCHangeHistoryID from s0007v0000.vwcrmcontact where id=fnsysIDGet(1, 128)
) a order by system

--Address
select * from (
select 's0001v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0001v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0005v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0005v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0006v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0006v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0007v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0007v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128)
) a order by System, UID

3. Check for data on sn000003

select 's0021v0000' System, Name, crmgenderid, sysCHangeHistoryID from s0021v0000.vwcrmcontact where id=fnsysIDGet(1, 128)
select 's0021v0000' System, id, Address1, sysCHangeHistoryID from s0021v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128)

=========================================================================================
Test 10
This test undo of previous change. This should cause the name to be restored and
remove the addresses that were added.

This tests:
- If we undo a change, will the update flow through to the local and public subnet server

*/

DO
$$
    DECLARE
        l_syschangehistoryid BIGINT;
    BEGIN

        -- Set change history id on delete to a new change history record

        SELECT max(syschangehistoryid)
        INTO l_syschangehistoryid
        FROM syschangehistoryrow
        WHERE rowidappliesto = 10000000000128
          AND sysdictionarytableidappliesto = 100
          AND actiontype != 'refresh';

        CALL spsysChangeHistoryUndo(p_sysChangeHistoryIDUndo := l_syschangehistoryid,
                                    p_sysChangeHistoryIDNew := fnsysChangeHistoryCreate('Undo of name address changes '));

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Undo of name address changes '));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexPackageDistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*
2. Check for data on sn000001

--Contact
select * from (
select 's0005v0000' System, Name, crmgenderid, sysCHangeHistoryID from s0005v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0006v0000' System, Name, crmgenderid, sysCHangeHistoryID from s0006v0000.vwcrmcontact where id=fnsysIDGet(1, 128) union
select 's0007v0000' System, Name, crmgenderid, sysCHangeHistoryID from s0007v0000.vwcrmcontact where id=fnsysIDGet(1, 128)
) a order by system

--Address
select * from (
select 's0001v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0001v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0005v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0005v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0006v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0006v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128) union
select 's0007v0000' System, UID, Address1, crmaddresstypeid, sysCHangeHistoryID from s0007v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 128)
) a order by System, UID

=========================================================================================
Test 11
This test makes a temporal update to glrate to change the rate to .051.  This should cause temporal updates to
get distributed to system 5,6,7 and 21.

This tests:
- If we make a temporal update, will the segments get properly communicated to the local and public servers

 */

DO
$$
    DECLARE
        l_ID   BIGINT;
        l_JSON JSON;
    BEGIN

        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT NULL comprovincestateid, 1 glratetypeid, 'GST Description' description, 0.051 rate) AS a;

        l_id = fnsysidget(1,3);
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2020-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := fnsysChangeHistoryCreate('Update GST Rate Effective 2020'));

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export updated temporal segment'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexPackageDistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*

2. Check results on sn000001

--GLRate
select * from (
select 's0001v0000' System, * from s0001v0000.vwglrate where id=fnsysidget(1,3) union
select 's0005v0000' System, * from s0005v0000.vwglrate where id=fnsysidget(1,3) union
select 's0006v0000' System, * from s0006v0000.vwglrate where id=fnsysidget(1,3) union
select 's0007v0000' System, * from s0007v0000.vwglrate where id=fnsysidget(1,3)
) a
order by System, temporalstartdate


3. Switch to sn000003

select * from s0021v0000.vwglrate order by id;

=========================================================================================
Test 12
This test delete of glrate 1.  This should cause updates to flow to system 5, 6, 7 and 21.

This tests:
- If we physically delete a record, will the update flow through to the local and public subnet server

*/

DO
$$
    BEGIN

        -- Set change history id on delete to a new change history record
        PERFORM fnsysChangeHistorySetParameters(p_sysChangeHistoryIDForDelete := fnsysChangeHistoryCreate('Test Delete of Rate Type 1'));
        DELETE FROM glrate WHERE id = fnsysidget(1,1);
        -- reset parameters to null
        PERFORM fnsysChangeHistorySetParameters();

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export delete of records'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexPackageDistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*


2. Check results on sn000001

--GLRate
select * from (
select 's0001v0000' System, * from s0001v0000.vwglrate where id=fnsysidget(1,1) union
select 's0005v0000' System, * from s0005v0000.vwglrate where id=fnsysidget(1,1) union
select 's0007v0000' System, * from s0007v0000.vwglrate where id=fnsysidget(1,1) union
select 's0006v0000' System, * from s0006v0000.vwglrate where id=fnsysidget(1,1)
) a
order by System, temporalstartdate

3. Check results on sn000003

select * from s0021v0000.vwglrate where id=fnsysidget(1,1);


=========================================================================================
Test 13
This test undo of delete of glrate 1.  This should glrate to reappear in system 5, 6, 7 and 21.

This tests:
- If we undo a change, will the update flow through to the local and public subnet server

*/

DO
$$
    DECLARE
        l_syschangehistoryid BIGINT;
    BEGIN

        -- Set change history id on delete to a new change history record

        SELECT MAX(syschangehistoryid) INTO l_syschangehistoryid FROM syschangehistoryrow
        where operationtype = 'delete';

        CALL spsysChangeHistoryUndo(p_sysChangeHistoryIDUndo := l_syschangehistoryid,
                                    p_sysChangeHistoryIDNew := fnsysChangeHistoryCreate('Test undo of Delete of Rate Type 1'));

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export undelete of records'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexPackageDistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;
/*

-- Select * from syschangehistory order by id desc
2. Check results on sn000001

--GLRate
select * from (
select 's0001v0000' System, * from s0001v0000.vwglrate where id=fnsysidget(1,1) union
select 's0005v0000' System, * from s0005v0000.vwglrate where id=fnsysidget(1,1) union
select 's0007v0000' System, * from s0007v0000.vwglrate where id=fnsysidget(1,1) union
select 's0006v0000' System, * from s0006v0000.vwglrate where id=fnsysidget(1,1)
) a
order by System, temporalstartdate

3. Check results on sn000003

select * from s0021v0000.vwglrate where id=fnsysidget(1,1);


=========================================================================================
Test 14

Creates a new gender type, updates Anderson Esopenko's record with it and inserts a new address record
These updates should show up in systems 4 which is on sn000002

This tests:
- If we setup a new foreign key record and reference it, will it flow through to the
destination system when it is referenced.
- If we add a new address for a contact, will it flow through to the destination
system as a result of the record group.
- Tests that we can pass updates to columns that have quotes in it.

1. Apply Update

 */

DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_newcrmgenderid     BIGINT;
    BEGIN

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Update Anderson Esopenko with a new gender id, insert a new address and update an existing one');

        INSERT INTO crmgender (description)
        VALUES ('New gender as of ' || NOW()::TIME::VARCHAR)
        RETURNING id INTO l_newcrmgenderid;

        UPDATE crmcontact
        SET name=name || ' as of ' || NOW()::TIME::VARCHAR || '''', crmgenderid=l_newcrmgenderid, sysChangeHistoryID=l_NewChangeHistoryID
        WHERE id = fnsysIDGet(1, 72);

        INSERT INTO crmaddress (comcityid, crmaddressidinheritedfrom, crmaddresstypeid, crmcontactid, additionalinformation, address1, address2, address3, address4, city, effectivedate, isprimaryaddress, postalzip, provincestate, syschangehistoryid)
        SELECT comcityid,
               crmaddressidinheritedfrom,
               crmaddresstypeid,
               crmContactID,
               additionalinformation,
               'New address as of ' || NOW()::TIME::VARCHAR,
               address2,
               address3,
               address4,
               city,
               effectivedate,
               isprimaryaddress,
               postalzip,
               provincestate,
               l_NewChangeHistoryID
               --  select *
        FROM crmaddress c
        WHERE crmContactID = fnsysIDGet(1, 72);

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export updated contact and address'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*
--Gender
select 's0001v0000' System, * from s0001v0000.vwcrmgender
--Contact
select 's0001v0000' System, Name, gender, crmgenderid, sysCHangeHistoryID from s0001v0000.vwcrmcontact where id=fnsysIDGet(1, 72);
--Address
select 's0001v0000' System, id, Address1, sysCHangeHistoryID from s0001v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 72)

2. Connect to sn000002

3. Check data

set SEARCH_PATH to s0003v0000, s0000v0000, public;
CALL spexpackagedistribute();
set SEARCH_PATH to default;

select 's0004v0000' System, * from s0004v0000.vwcrmgender
select 's0004v0000' System, Name, gender, crmgenderid, sysCHangeHistoryID from s0004v0000.vwcrmcontact where id=fnsysIDGet(1, 72);
select 's0004v0000' System, id, Address1, sysCHangeHistoryID from s0004v0000.vwcrmaddress where crmcontactid=fnsysIDGet(1, 72)

=========================================================================================
Test 15
So far we have only done a two level subscription for master data.  This tests a three level subscription where
Org A creates subscriptions to master data, then one of the child entities creates a subscription to
a grand child entity.

To verify this, Systems 5,6,7 and 21 have subscriptions to GL RateType which is governed by
system 1.  Now System 4 is going to become a subscriber to System 5's Rate Type records and then we are going
to have System 1 make an update to a record plus we are going to have System 5 add its
own record.  All these updates should flow through to System 4
for Organization B

This tests:
- hierarchical subscriptions
- quotes in record group names

1. Run update
*/
DO
$$
    DECLARE
        l_exSubscriberID  BIGINT;
        DECLARE
        l_exrecordgroupid BIGINT;

    BEGIN

        SET SEARCH_PATH TO s0005v0000, s0000v0000, public;

        l_exSubscriberID := fnsysMDSGetSystem(4);

        INSERT INTO exrecordgroup (sysdictionarytableid, name)
        VALUES (397, 'GL Rate Type''s')
        RETURNING id INTO l_exrecordgroupid;

        INSERT INTO exsubscription (exrecordgroupid, exsubscriberid, rowidsubscribedto, syschangehistoryid)
        VALUES (l_exrecordgroupid, l_exSubscriberID, NULL, fnsysChangeHistoryCreate('Make system 4 a subscriber to the rate type table in System 5'));

        INSERT INTO glratetype (description, syschangehistoryid)
        VALUES ('System 5s new ratetype 2 as of ' || NOW()::TIME::VARCHAR, fnsysChangeHistoryCreate('Create Ratetype 2 for System 5'));

        SET SEARCH_PATH TO DEFAULT;
        INSERT INTO glratetype (description, syschangehistoryid)
        VALUES ('System 1s new ratetype as of ' || NOW()::TIME::VARCHAR, fnsysChangeHistoryCreate('Create Ratetype for System 1'));
        CALL spexPackageExport(fnsysChangeHistoryCreate('Export new rate type for System 1'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

        SET SEARCH_PATH TO s0005v0000, s0000v0000, public;
        CALL spexPackageExport(fnsysChangeHistoryCreate('Export new rate types for System 5'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;
/*
--RateType
select 's0001v0000' System, * from s0001v0000.vwglratetype union
select 's0005v0000' System, * from s0005v0000.vwglratetype;

2. Connect to sn000002

3. Check data

set SEARCH_PATH to s0003v0000, s0000v0000, public;
CALL spexpackagedistribute();
set SEARCH_PATH to default;

select 's0004v0000' System, * from s0004v0000.vwglratetype;

4. Connect to sn000001

NEGATIVE TESTS

- insert a new column that has a not null constraint

- drop a table that a system we are exporting into
- drop a column
- insert a column that does not have a default value
- update a record that we have subscribed to
- make a recursive subscription
- have a non-subnet server run that subnet server distribute
- have a header record include a column that has a foreign key reference that wont be satisfied.
- kill the subnet server
- create a change history event with foreign key references to tables, columns, performed by and
commands that will not be satisfied in the destination system.

STRESS TEST
- have a system create a subscription to a master table with many rows

=========================================================================================
Test 17

This is a negative test that adds a short code column to the crmaddresstype table for s0007v0000 and makes it not
null.  Then we cause a new addresstype type to be created.

 */

SET SEARCH_PATH TO DEFAULT;
DO
$$
    DECLARE
        l_NewChangeHistoryID  BIGINT;
        l_newcrmaddresstypeid BIGINT;
    BEGIN
        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Negative test to cause a new addresstype type to get created but fail in system 7 because we have change short code to be not null');

        -- At this point this record group only exports business address.  We need to change it to export all addresses.
        UPDATE exrecordgroup e
        SET whereclause = NULL, name='Address', syschangehistoryid=l_NewChangeHistoryID
        WHERE id = fnsysIDGet(1, 2);

        INSERT INTO crmaddresstype (description)
        VALUES ('New Address Type as of ' || NOW()::TIME::VARCHAR)
        RETURNING id INTO l_newcrmaddresstypeid;

        UPDATE crmaddress a
        SET address1=leftfind(address1, 'as of ') || ' as of ' || NOW()::TIME::VARCHAR,
            crmaddresstypeid=l_newcrmaddresstypeid, sysChangeHistoryID=l_NewChangeHistoryID
        WHERE a.id = (
                         SELECT MAX(id)
                         FROM crmaddress
                         WHERE crmcontactid = fnsysIDGet(1, 128));

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export updated contact'));

        IF isColumnFound('s0007v0000.crmaddresstype', 'shortcode') = FALSE
        THEN
            ALTER TABLE s0007v0000.crmaddresstype
                ADD ShortCode VARCHAR;
            UPDATE s0007v0000.crmaddresstype SET shortcode='';
            ALTER TABLE s0007v0000.crmaddresstype
                ALTER COLUMN shortcode SET NOT NULL;
        END IF;

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*

2. Check data on sn000001.s0007v0000

SET SEARCH_PATH TO s0007v0000, s0000v0000, public;
SELECT * FROM vwexmessage order by id desc;
SELECT * FROM vwcrmaddresstype;

3. Change to Not null

a. ALTER TABLE s0007v0000.crmaddresstype ALTER COLUMN shortcode DROP NOT NULL;

b. ALTER TABLE s0007v0000.crmaddresstype ALTER COLUMN shortcode SET DEFAULT '';

4. Re-run import

-- If a batch id is specifically references on the import then it will run it again even if
-- it has warning messages
DO
$$
    DECLARE
        l_exhistorybatchid BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_exhistorybatchid FROM exhistorybatch WHERE id < fnsysIDGet(2, 0);
        CALL spexpackageimport(l_exhistorybatchid);
    END;
$$;

5. Check results

SELECT * FROM vwexmessage order by id desc;
SELECT * FROM s0007v0000.vwcrmaddresstype;

5. Reset Search Path

SET SEARCH_PATH TO default;

=========================================================================================
Test 18

This is a negative test that drops a column in the recipient system s0007v0000.  Then we cause a new addresstype type to be created.

 */

SET SEARCH_PATH TO DEFAULT;
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
    BEGIN

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Negative test to cause a new address type to get created but fail in system 7 because we have change short code to be not null');

        UPDATE crmaddress a
        SET address1=leftfind(address1, 'as of ') || ' as of ' || NOW()::TIME::VARCHAR, sysChangeHistoryID=l_NewChangeHistoryID
        WHERE a.id = (
                         SELECT MAX(id)
                         FROM crmaddress
                         WHERE crmcontactid = fnsysIDGet(1, 128));

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export updated contact'));

        IF isColumnFound('s0007v0000.crmaddress', 'address4')
        THEN
            SET SEARCH_PATH TO s0007v0000, s0000v0000, public;
            DELETE FROM sysdictionarycolumn s WHERE name = 'address4';
            CALL spsysSchemaUpdate();
            CALL spsyschangehistoryrefreshtriggers();
            SET SEARCH_PATH TO DEFAULT;
        END IF;

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*

2. Check data on sn000001.s0007v0000


SET SEARCH_PATH TO s0007v0000, s0000v0000, public;
SELECT * FROM vwexmessage order by id desc;
-- when I drop address4 with cascade entire view gets dropped
SELECT * FROM crmaddress where crmcontactid = fnsysIDGet(1, 128);

=========================================================================================
Test 19

This tests the override message capability

3. Update exmessage message to ignore warning message

a. Override messages

update exmessage set isoverridden=true;
CALL spexpackageimport();

b. Override with message override record
*/
DO
$$
    BEGIN
        SET SEARCH_PATH TO s0007v0000, s0000v0000, public;

        INSERT INTO exmessageoverride (sysdictionarytableid, sysmessageid, description)
        SELECT sysdictionarytableid, sysmessageid, description
        FROM exmessage
        WHERE sysdictionarytableid = 110;
        CALL spexpackageimport();

        SET SEARCH_PATH TO DEFAULT;
    END;
$$ LANGUAGE plpgsql;
/*
c. Override through parameter call

CALL spexpackageimport(NULL, TRUE, FALSE, fnsysChangeHistoryCreate('Test Import Change History') );

4. Check results

SELECT * FROM s0007v0000.vwexmessage order by id desc;
SELECT * FROM s0007v0000.crmaddress where crmcontactid = fnsysIDGet(1, 128);
SELECT fnsysidview(id) uid, fnsysidview(crmcontactid) contact, address1, city, fnsysidview(crmaddresstypeid) crmaddresstypeid FROM s0007v0000.crmaddress

5. Reset Search Path

SET SEARCH_PATH TO default;

=========================================================================================
Test 20

Change system 7 to drop the address phone type column and the address phone type table and
then export a record to it.

Tests
- This will cause two error conditions to happen.
  - table is missing
  - column is missing

 */

SET SEARCH_PATH TO DEFAULT;
DO
$$
    DECLARE
        l_NewChangeHistoryID  BIGINT;
        l_newcrmaddresstypeid BIGINT;
    BEGIN

        IF iscolumnfound('s0007v0000.crmaddressphone', 'crmaddressphonetypeid')
        THEN
            SET SEARCH_PATH TO s0007v0000, s0000v0000, public;
            UPDATE sysdictionarycolumn
            SET rowstatus='d'
            WHERE (LOWER(name) = 'crmAddressPhoneTypeID' AND sysdictionarytableid = 112)
               OR sysdictionarytableid = 113;
            UPDATE sysdictionarytable s SET rowstatus='d' WHERE id = 113;
            CALL spsysschemaupdate();
            CALL spsyschangehistoryrefreshtriggers(TRUE);
            SET SEARCH_PATH TO DEFAULT;
        END IF;

        INSERT INTO crmaddressphonetype (description)
        VALUES ('New Address Phone Type as of ' || NOW()::TIME::VARCHAR)
        RETURNING id INTO l_newcrmaddresstypeid;

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Add new address phone type and refresh through to system that dropped the table ');

        UPDATE crmaddressphone a
        SET crmaddressphonetypeid=l_newcrmaddresstypeid, phone=phone || ' as of ' || NOW()::TIME::VARCHAR,
            sysChangeHistoryID=l_NewChangeHistoryID
        WHERE a.id = (
                         SELECT MAX(id)
                         FROM crmaddressphone
                         WHERE crmcontactid = fnsysIDGet(1, 128));

        CALL spexPackageExport(p_sysChangeHistoryID := fnsysChangeHistoryCreate('Export updated phone type'));

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        CALL spexpackagedistribute();
        SET SEARCH_PATH TO DEFAULT;

    END;
$$ LANGUAGE plpgsql;

/*

2. Check data on sn000001.s0007v0000

SELECT * FROM s0007v0000.vwexmessage order by id;
SELECT * FROM s0007v0000.crmaddressphone where crmcontactid = fnsysIDGet(1, 128);

=========================================================================================
Test 21

This tests the override message capability

*/
DO
$$
    BEGIN
        SET SEARCH_PATH TO s0007v0000, s0000v0000, public;

        INSERT INTO exmessageoverride (sysdictionarytableid, sysmessageid, description)
        SELECT DISTINCT sysdictionarytableid, sysmessageid, description
        FROM exmessage
        WHERE isoverridden = FALSE
          AND messageseverity = 2;
        CALL spexpackageimport();

        SET SEARCH_PATH TO DEFAULT;
    END;
$$ LANGUAGE plpgsql;
/*
b. Update exmessage message to ignore warning message

update exmessage set isoverridden=true;
CALL spexpackageimport();

4. Check results
SELECT * FROM s0007v0000.vwexmessage order by id desc;
SELECT * FROM s0007v0000.crmaddressphone where crmcontactid = fnsysIDGet(1, 128);
SET SEARCH_PATH TO default;

=========================================================================================
Test 21

This tests adding a vehicle registration to the s0005v0000 system and then
setting up a charge to it.  This is done so we can test merging schemas

*/

DO
$$
    DECLARE
        l_NewChangeHistoryID      BIGINT;
        l_sysdictionarytableid    BIGINT;
        l_GLAccountID             BIGINT;
        l_vrVehicleRegistrationID BIGINT;
        l_vrVehicleID             BIGINT;
        l_gltransactionid         BIGINT;
        l_glbatchid               BIGINT;
        l_JSON                    JSON;
    BEGIN
        SET SEARCH_PATH TO s0005v0000, s0000v0000, public;

        IF NOT fnIfTableExists('vrvehicle')
        THEN
            l_NewChangeHistoryID :=
                    fnsysChangeHistoryCreate('Add new address phone type and refresh through to system that dropped the table ');
            INSERT INTO sysdictionarytable (name, normalizedname, changehistoryscope, ischangehistoryused, istabletemporal, systemmodule, tabletype, translation, rowstatus, syschangehistoryid)
            SELECT 'vrvehicle',
                   'vrvehicle',
                   'insert, update, delete',
                   TRUE,
                   FALSE,
                   'vr',
                   'data',
                   'serialnumber',
                   'a',
                   l_NewChangeHistoryID
            RETURNING id INTO l_sysdictionarytableid;

            INSERT INTO sysdictionarycolumn (name, label, sysdictionarytableid, sysdictionarytableidforeign, datalength, datatype, decimals, defaultvalue, ischangehistoryused, isheadercolumn, isnullable, isincludedinuniqueconstraint, purpose, rowstatus, syschangehistoryid)
            SELECT 'id'          name,
                   'id'          label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'bigint'      datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   FALSE         ischangehistoryused,
                   FALSE         isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'primary key' purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'make'       name,
                   'Make'       label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'varchar'    datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'model'      name,
                   'Model'      label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'varchar'    datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'year'       name,
                   'Year'       label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'int'        datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'serialnumber'  name,
                   'Serial Number' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT    sysdictionarytableidforeign,
                   NULL::INT       datalength,
                   'varchar'       datatype,
                   NULL::INT       decimals,
                   NULL            defaultvalue,
                   TRUE            ischangehistoryused,
                   TRUE            isheadercolumn,
                   FALSE           isnullable,
                   FALSE           isincludedinuniqueconstraint,
                   'data'          purpose,
                   'a'             rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'color'      name,
                   'Color'      label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'varchar'    datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'rowstatus'  name,
                   'Row Status' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'char'       datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   FALSE        ischangehistoryused,
                   FALSE        isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'system'     purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'syschangehistoryid'     name,
                   'Last Change History Id' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT             sysdictionarytableidforeign,
                   NULL::INT                datalength,
                   'bigint'                 datatype,
                   NULL::INT                decimals,
                   NULL                     defaultvalue,
                   FALSE                    ischangehistoryused,
                   FALSE                    isheadercolumn,
                   FALSE                    isnullable,
                   FALSE                    isincludedinuniqueconstraint,
                   'audit'                  purpose,
                   'a'                      rowstatus,
                   l_NewChangeHistoryID;

            INSERT INTO sysdictionarytable (name, normalizedname, changehistoryscope, ischangehistoryused, istabletemporal, systemmodule, tabletype, rowstatus, syschangehistoryid)
            SELECT 'vrvehicleregistration',
                   'vrvehicleregistration',
                   'insert, update, delete',
                   TRUE,
                   TRUE,
                   'vr',
                   'data',
                   'a',
                   l_NewChangeHistoryID
            RETURNING id INTO l_sysdictionarytableid;

            INSERT INTO sysdictionarycolumn (name, label, sysdictionarytableid, sysdictionarytableidforeign, datalength, datatype, decimals, defaultvalue, ischangehistoryused, isheadercolumn, isnullable, isincludedinuniqueconstraint, purpose, rowstatus, syschangehistoryid)
            SELECT 'id'          name,
                   'id'          label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'bigint'      datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   FALSE         ischangehistoryused,
                   FALSE         isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'primary key' purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'vrvehicleid'                       name,
                   'Vehicle'                           label,
                   l_sysdictionarytableid,
                   l_sysdictionarytableid - 1 ::BIGINT sysdictionarytableidforeign,
                   NULL::INT                           datalength,
                   'bigint'                            datatype,
                   NULL::INT                           decimals,
                   NULL                                defaultvalue,
                   TRUE                                ischangehistoryused,
                   TRUE                                isheadercolumn,
                   FALSE                               isnullable,
                   FALSE                               isincludedinuniqueconstraint,
                   'foreign key'                       purpose,
                   'a'                                 rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'crmcontactid'     name,
                   'Registered Owner' label,
                   l_sysdictionarytableid,
                   100 ::BIGINT       sysdictionarytableidforeign,
                   NULL::INT          datalength,
                   'bigint'           datatype,
                   NULL::INT          decimals,
                   NULL               defaultvalue,
                   TRUE               ischangehistoryused,
                   TRUE               isheadercolumn,
                   FALSE              isnullable,
                   FALSE              isincludedinuniqueconstraint,
                   'foreign key'      purpose,
                   'a'                rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'temporalstartdate'           name,
                   'Registration Effective Date' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT                  sysdictionarytableidforeign,
                   NULL::INT                     datalength,
                   'date'                        datatype,
                   NULL::INT                     decimals,
                   NULL                          defaultvalue,
                   TRUE                          ischangehistoryused,
                   TRUE                          isheadercolumn,
                   FALSE                         isnullable,
                   FALSE                         isincludedinuniqueconstraint,
                   'temporal'                    purpose,
                   'a'                           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'temporalenddate'          name,
                   'Registration Expiry Date' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT               sysdictionarytableidforeign,
                   NULL::INT                  datalength,
                   'date'                     datatype,
                   NULL::INT                  decimals,
                   NULL                       defaultvalue,
                   TRUE                       ischangehistoryused,
                   TRUE                       isheadercolumn,
                   FALSE                      isnullable,
                   FALSE                      isincludedinuniqueconstraint,
                   'temporal'                 purpose,
                   'a'                        rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'rowstatus'  name,
                   'Row Status' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'char'       datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   FALSE        ischangehistoryused,
                   FALSE        isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'system'     purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'syschangehistoryid'     name,
                   'Last Change History Id' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT             sysdictionarytableidforeign,
                   NULL::INT                datalength,
                   'bigint'                 datatype,
                   NULL::INT                decimals,
                   NULL                     defaultvalue,
                   FALSE                    ischangehistoryused,
                   FALSE                    isheadercolumn,
                   FALSE                    isnullable,
                   FALSE                    isincludedinuniqueconstraint,
                   'audit'                  purpose,
                   'a'                      rowstatus,
                   l_NewChangeHistoryID;

            INSERT INTO sysdictionarycolumn (name, label, sysdictionarytableid, sysdictionarytableidforeign, datalength, datatype, decimals, defaultvalue, ischangehistoryused, isheadercolumn, isnullable, isincludedinuniqueconstraint, purpose, rowstatus, syschangehistoryid)
            SELECT 'driverlicensenumber'   name,
                   'Driver License Number' label,
                   100,
                   NULL::BIGINT            sysdictionarytableidforeign,
                   NULL::INT               datalength,
                   'varchar'               datatype,
                   NULL::INT               decimals,
                   NULL                    defaultvalue,
                   TRUE                    ischangehistoryused,
                   TRUE                    isheadercolumn,
                   FALSE                   isnullable,
                   FALSE                   isincludedinuniqueconstraint,
                   'data'                  purpose,
                   'a'                     rowstatus,
                   l_NewChangeHistoryID;

            CALL spsysschemaupdate();
            CALL spsyschangehistoryrefreshtriggers(TRUE);

            INSERT INTO glaccount (glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (2700, 60, 1000, NULL, 5, 1, NULL, 'Vehicle Registration Revenue', TRUE, TRUE, NULL, '4100', 71, 'a', l_NewChangeHistoryID)
            RETURNING id INTO l_glaccountid;

            INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (fnsysidGet(1, 36), 1900, 30, NULL, NULL, 3, 2, NULL, 'GST Payable', TRUE, FALSE, NULL, '2150', 40, 'a', NULL);

            INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (fnsysidGet(1, 37), fnsysidGet(1, 36), 30, NULL, NULL, 4, 1, NULL, 'GST Collected', TRUE, TRUE, NULL, '2150', 41, 'a', NULL);

            INSERT INTO sysmultilinktablerule(sysdictionarycolumnidsource, sysdictionarycolumniddest, sysdictionarytableid, description, seqno, whereclause, rowstatus, syschangehistoryid)
            SELECT sysdictionarycolumnidsource,
                   sysdictionarycolumniddest,
                   l_sysdictionarytableid  sysdictionarytableid,
                   'Vehicle Registrations' Description,
                   seqno + 1,
                   whereclause,
                   rowstatus,
                   l_NewChangeHistoryID
            FROM sysmultilinktablerule
            WHERE id = 1000;
        END IF;

        INSERT INTO vrVehicle (make, model, serialnumber, color, rowstatus, syschangehistoryid, year)
        SELECT 'Ford',
               'F-150',
               '1J4GW48S94C420221',
               'white',
               'a',
               l_NewChangeHistoryID,
               2020
        RETURNING id
            INTO l_vrVehicleID;

        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT fnsysidGet(1, 158119) crmcontactid, l_vrVehicleID vrvehicleid) AS a;
        CALL spsysTemporalDataUpdate(p_Id := l_vrVehicleRegistrationID,
                                     p_EffectiveDate := '2022-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := l_sysdictionarytableid,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        CALL spsysTemporalDataUpdate(p_Id := l_vrVehicleRegistrationID,
                                     p_EffectiveDate := '2023-01-01',
                                     p_NewData := NULL,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := l_sysdictionarytableid,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Create vehicle registration transaction ');

        INSERT INTO glbatch (glbatchtypeid, description)
        SELECT 1, 'Vehicle Registration'
        RETURNING id INTO l_glbatchid;

        INSERT INTO gltransaction (glbatchid, glpostingstatusid, gltransactiontypeid, description, transactiondate, syschangehistoryid)
        SELECT l_glbatchid, 2, 1, 'Vehicle Registration', NOW()::DATE, l_NewChangeHistoryID
        RETURNING id INTO l_gltransactionid;

        INSERT INTO glentry (glaccountid, glcostcentreid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, description, syschangehistoryid)
        SELECT fnsysidGet(0, 1400),
               0,
               l_gltransactionid,
               321,
               fnsysidGet(1, 351233),
               105,
               'Vehicle Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT fnsysidGet(1, 37),
               0,
               l_gltransactionid,
               NULL,
               NULL,
               -5,
               'Vehicle Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT l_GLAccountID,
               0,
               l_gltransactionid,
               l_sysdictionarytableid,
               l_vrVehicleRegistrationID,
               -100,
               'Vehicle Registration Fee',
               l_NewChangeHistoryID;

        CALL spglBatchApprove(l_glbatchid);

        SET SEARCH_PATH TO DEFAULT;
    END;
$$ LANGUAGE plpgsql;
/*
=========================================================================================
Test 22

This tests adding a land registry to the s0006v0000 system and then
setting up a charge to it.  This is done so we can test merging schemas

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID         BIGINT;
        l_sysdictionarytableid       BIGINT;
        l_GLAccountID                BIGINT;
        l_lrLandParcelRegistrationID BIGINT;
        l_lrLandParcelID             BIGINT;
        l_gltransactionid            BIGINT;
        l_glbatchid                  BIGINT;
        l_JSON                       JSON;
    BEGIN
        SET SEARCH_PATH TO s0006v0000, s0000v0000, public;

        IF NOT fnIfTableExists('lrlandparcel')
        THEN
            l_NewChangeHistoryID :=
                    fnsysChangeHistoryCreate('Add new address phone type and refresh through to system that dropped the table ');
            INSERT INTO sysdictionarytable (name, normalizedname, changehistoryscope, ischangehistoryused, istabletemporal, systemmodule, tabletype, translation, rowstatus, syschangehistoryid)
            SELECT 'lrlandparcel',
                   'lrlandparcel',
                   'insert, update, delete',
                   TRUE,
                   FALSE,
                   'lr',
                   'data',
                   'purpose',
                   'a',
                   l_NewChangeHistoryID
            RETURNING id INTO l_sysdictionarytableid;

            INSERT INTO sysdictionarycolumn (name, label, sysdictionarytableid, sysdictionarytableidforeign, datalength, datatype, decimals, defaultvalue, ischangehistoryused, isheadercolumn, isnullable, isincludedinuniqueconstraint, purpose, rowstatus, syschangehistoryid)
            SELECT 'id'          name,
                   'id'          label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'bigint'      datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   FALSE         ischangehistoryused,
                   FALSE         isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'primary key' purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'description' name,
                   'Description' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'varchar'     datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   TRUE          ischangehistoryused,
                   TRUE          isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'data'        purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'landparceltype'   name,
                   'Land Parcel Type' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT       sysdictionarytableidforeign,
                   NULL::INT          datalength,
                   'varchar'          datatype,
                   NULL::INT          decimals,
                   NULL               defaultvalue,
                   TRUE               ischangehistoryused,
                   TRUE               isheadercolumn,
                   FALSE              isnullable,
                   FALSE              isincludedinuniqueconstraint,
                   'data'             purpose,
                   'a'                rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'size'       name,
                   'Size'       label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   10::INT      datalength,
                   'decimal'    datatype,
                   2::INT       decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'purpose'    name,
                   'Purpose'    label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'varchar'    datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'gpslocation'  name,
                   'GPS Location' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT   sysdictionarytableidforeign,
                   NULL::INT      datalength,
                   'varchar'      datatype,
                   NULL::INT      decimals,
                   NULL           defaultvalue,
                   TRUE           ischangehistoryused,
                   TRUE           isheadercolumn,
                   FALSE          isnullable,
                   FALSE          isincludedinuniqueconstraint,
                   'data'         purpose,
                   'a'            rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'rowstatus'  name,
                   'Row Status' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'char'       datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   FALSE        ischangehistoryused,
                   FALSE        isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'system'     purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'syschangehistoryid'     name,
                   'Last Change History Id' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT             sysdictionarytableidforeign,
                   NULL::INT                datalength,
                   'bigint'                 datatype,
                   NULL::INT                decimals,
                   NULL                     defaultvalue,
                   FALSE                    ischangehistoryused,
                   FALSE                    isheadercolumn,
                   FALSE                    isnullable,
                   FALSE                    isincludedinuniqueconstraint,
                   'audit'                  purpose,
                   'a'                      rowstatus,
                   l_NewChangeHistoryID;

            INSERT INTO sysdictionarytable (name, normalizedname, changehistoryscope, ischangehistoryused, istabletemporal, systemmodule, tabletype, rowstatus, syschangehistoryid)
            SELECT 'lrlandparcelregistration',
                   'lrlandparcelregistration',
                   'insert, update, delete',
                   TRUE,
                   TRUE,
                   'lr',
                   'data',
                   'a',
                   l_NewChangeHistoryID
            RETURNING id INTO l_sysdictionarytableid;

            INSERT INTO sysdictionarycolumn (name, label, sysdictionarytableid, sysdictionarytableidforeign, datalength, datatype, decimals, defaultvalue, ischangehistoryused, isheadercolumn, isnullable, isincludedinuniqueconstraint, purpose, rowstatus, syschangehistoryid)
            SELECT 'id'          name,
                   'id'          label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'bigint'      datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   FALSE         ischangehistoryused,
                   FALSE         isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'primary key' purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'lrlandparcelid'                    name,
                   'LandParcel'                        label,
                   l_sysdictionarytableid,
                   l_sysdictionarytableid - 1 ::BIGINT sysdictionarytableidforeign,
                   NULL::INT                           datalength,
                   'bigint'                            datatype,
                   NULL::INT                           decimals,
                   NULL                                defaultvalue,
                   TRUE                                ischangehistoryused,
                   TRUE                                isheadercolumn,
                   FALSE                               isnullable,
                   FALSE                               isincludedinuniqueconstraint,
                   'foreign key'                       purpose,
                   'a'                                 rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'crmcontactid'     name,
                   'Registered Owner' label,
                   l_sysdictionarytableid,
                   100 ::BIGINT       sysdictionarytableidforeign,
                   NULL::INT          datalength,
                   'bigint'           datatype,
                   NULL::INT          decimals,
                   NULL               defaultvalue,
                   TRUE               ischangehistoryused,
                   TRUE               isheadercolumn,
                   FALSE              isnullable,
                   FALSE              isincludedinuniqueconstraint,
                   'foreign key'      purpose,
                   'a'                rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'temporalstartdate'           name,
                   'Registration Effective Date' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT                  sysdictionarytableidforeign,
                   NULL::INT                     datalength,
                   'date'                        datatype,
                   NULL::INT                     decimals,
                   NULL                          defaultvalue,
                   TRUE                          ischangehistoryused,
                   TRUE                          isheadercolumn,
                   FALSE                         isnullable,
                   FALSE                         isincludedinuniqueconstraint,
                   'temporal'                    purpose,
                   'a'                           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'temporalenddate'          name,
                   'Registration Expiry Date' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT               sysdictionarytableidforeign,
                   NULL::INT                  datalength,
                   'date'                     datatype,
                   NULL::INT                  decimals,
                   NULL                       defaultvalue,
                   TRUE                       ischangehistoryused,
                   TRUE                       isheadercolumn,
                   FALSE                      isnullable,
                   FALSE                      isincludedinuniqueconstraint,
                   'temporal'                 purpose,
                   'a'                        rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'rowstatus'  name,
                   'Row Status' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'char'       datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   FALSE        ischangehistoryused,
                   FALSE        isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'system'     purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'syschangehistoryid'     name,
                   'Last Change History Id' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT             sysdictionarytableidforeign,
                   NULL::INT                datalength,
                   'bigint'                 datatype,
                   NULL::INT                decimals,
                   NULL                     defaultvalue,
                   FALSE                    ischangehistoryused,
                   FALSE                    isheadercolumn,
                   FALSE                    isnullable,
                   FALSE                    isincludedinuniqueconstraint,
                   'audit'                  purpose,
                   'a'                      rowstatus,
                   l_NewChangeHistoryID;

            CALL spsysschemaupdate();
            CALL spsyschangehistoryrefreshtriggers(TRUE);

            INSERT INTO glaccount (glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (2700, 60, 1000, NULL, 5, 1, NULL, 'Land Parcel Registration Revenue', TRUE, TRUE, NULL, '4100', 71, 'a', l_NewChangeHistoryID)
            RETURNING id INTO l_glaccountid;

            INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (fnsysidGet(1, 36), 1900, 30, NULL, NULL, 3, 2, NULL, 'GST Payable', TRUE, FALSE, NULL, '2150', 40, 'a', NULL);

            INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (fnsysidGet(1, 37), fnsysidGet(1, 36), 30, NULL, NULL, 4, 1, NULL, 'GST Collected', TRUE, TRUE, NULL, '2150', 41, 'a', NULL);

            INSERT INTO sysmultilinktablerule(sysdictionarycolumnidsource, sysdictionarycolumniddest, sysdictionarytableid, description, seqno, whereclause, rowstatus, syschangehistoryid)
            SELECT sysdictionarycolumnidsource,
                   sysdictionarycolumniddest,
                   l_sysdictionarytableid      sysdictionarytableid,
                   'Land Parcel Registrations' Description,
                   seqno + 1,
                   whereclause,
                   rowstatus,
                   l_NewChangeHistoryID
            FROM sysmultilinktablerule
            WHERE id = 1000;
        END IF;

        INSERT INTO lrLandParcel (description, landparceltype, purpose, gpslocation, rowstatus, syschangehistoryid, size)
        SELECT 'SW-12-02-02-4',
               'Undeveloped',
               'Farmland',
               '47.284784, -110.622318',
               'a',
               l_NewChangeHistoryID,
               160
        RETURNING id
            INTO l_lrLandParcelID;

        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT fnsysidGet(1, 158119) crmcontactid, l_lrLandParcelID lrlandparcelid) AS a;
        CALL spsysTemporalDataUpdate(p_Id := l_lrLandParcelRegistrationID,
                                     p_EffectiveDate := '2022-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := l_sysdictionarytableid,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Create landparcel registration transaction ');

        INSERT INTO glbatch (glbatchtypeid, description)
        SELECT 1, 'Land Parcel Registration'
        RETURNING id INTO l_glbatchid;

        INSERT INTO gltransaction (glbatchid, glpostingstatusid, gltransactiontypeid, description, transactiondate, syschangehistoryid)
        SELECT l_glbatchid, 2, 1, 'Land Parcel Registration', NOW()::DATE, l_NewChangeHistoryID
        RETURNING id INTO l_gltransactionid;

        INSERT INTO glentry (glaccountid, glcostcentreid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, description, syschangehistoryid)
        SELECT fnsysidGet(0, 1400),
               0,
               l_gltransactionid,
               321,
               fnsysidGet(1, 351233),
               525,
               'Land Parcel Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT fnsysidGet(1, 37),
               0,
               l_gltransactionid,
               NULL,
               NULL,
               -25,
               'Land Parcel Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT l_GLAccountID,
               0,
               l_gltransactionid,
               l_sysdictionarytableid,
               l_lrLandParcelRegistrationID,
               -500,
               'Land Parcel Registration Fee',
               l_NewChangeHistoryID;

        CALL spglBatchApprove(l_glbatchid);

        SET SEARCH_PATH TO DEFAULT;
    END;
$$ LANGUAGE plpgsql;
/*
=========================================================================================
Test 23

This tests adding a well registry to the s0007v0000 system and then
setting up a charge to it.  This is done so we can test merging schemas
*/
DO
$$
    DECLARE
        l_NewChangeHistoryID      BIGINT;
        l_sysdictionarytableid    BIGINT;
        l_GLAccountID             BIGINT;
        l_wrWellRegistrationID    BIGINT;
        l_wrWellID                BIGINT;
        l_gltransactionid         BIGINT;
        l_glbatchid               BIGINT;
        l_sysmultilinktableruleid BIGINT;
        l_JSON                    JSON;
    BEGIN
        SET SEARCH_PATH TO s0007v0000, s0000v0000, public;

        IF NOT fnIfTableExists('wrwell')
        THEN
            l_NewChangeHistoryID :=
                    fnsysChangeHistoryCreate('Add new address phone type and refresh through to system that dropped the table ');
            INSERT INTO sysdictionarytable (name, normalizedname, changehistoryscope, ischangehistoryused, istabletemporal, systemmodule, tabletype, translation, rowstatus, syschangehistoryid)
            SELECT 'wrwell',
                   'wrwell',
                   'insert, update, delete',
                   TRUE,
                   FALSE,
                   'wr',
                   'data',
                   'purpose',
                   'a',
                   l_NewChangeHistoryID
            RETURNING id INTO l_sysdictionarytableid;

            INSERT INTO sysdictionarycolumn (name, label, sysdictionarytableid, sysdictionarytableidforeign, datalength, datatype, decimals, defaultvalue, ischangehistoryused, isheadercolumn, isnullable, isincludedinuniqueconstraint, purpose, rowstatus, syschangehistoryid)
            SELECT 'id'          name,
                   'id'          label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'bigint'      datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   FALSE         ischangehistoryused,
                   FALSE         isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'primary key' purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'description' name,
                   'Description' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'varchar'     datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   TRUE          ischangehistoryused,
                   TRUE          isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'data'        purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'welltype'   name,
                   'Well Type'  label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'varchar'    datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'status'     name,
                   'Status'     label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'varchar'    datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'purpose'    name,
                   'Purpose'    label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'varchar'    datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   TRUE         ischangehistoryused,
                   TRUE         isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'data'       purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'gpslocation'  name,
                   'GPS Location' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT   sysdictionarytableidforeign,
                   NULL::INT      datalength,
                   'varchar'      datatype,
                   NULL::INT      decimals,
                   NULL           defaultvalue,
                   TRUE           ischangehistoryused,
                   TRUE           isheadercolumn,
                   FALSE          isnullable,
                   FALSE          isincludedinuniqueconstraint,
                   'data'         purpose,
                   'a'            rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'rowstatus'  name,
                   'Row Status' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'char'       datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   FALSE        ischangehistoryused,
                   FALSE        isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'system'     purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'syschangehistoryid'     name,
                   'Last Change History Id' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT             sysdictionarytableidforeign,
                   NULL::INT                datalength,
                   'bigint'                 datatype,
                   NULL::INT                decimals,
                   NULL                     defaultvalue,
                   FALSE                    ischangehistoryused,
                   FALSE                    isheadercolumn,
                   FALSE                    isnullable,
                   FALSE                    isincludedinuniqueconstraint,
                   'audit'                  purpose,
                   'a'                      rowstatus,
                   l_NewChangeHistoryID;

            INSERT INTO sysdictionarytable (name, normalizedname, changehistoryscope, ischangehistoryused, istabletemporal, systemmodule, tabletype, rowstatus, syschangehistoryid)
            SELECT 'wrwellregistration',
                   'wrwellregistration',
                   'insert, update, delete',
                   TRUE,
                   TRUE,
                   'wr',
                   'data',
                   'a',
                   l_NewChangeHistoryID
            RETURNING id INTO l_sysdictionarytableid;

            INSERT INTO sysdictionarycolumn (name, label, sysdictionarytableid, sysdictionarytableidforeign, datalength, datatype, decimals, defaultvalue, ischangehistoryused, isheadercolumn, isnullable, isincludedinuniqueconstraint, purpose, rowstatus, syschangehistoryid)
            SELECT 'id'          name,
                   'id'          label,
                   l_sysdictionarytableid,
                   NULL::BIGINT  sysdictionarytableidforeign,
                   NULL::INT     datalength,
                   'bigint'      datatype,
                   NULL::INT     decimals,
                   NULL          defaultvalue,
                   FALSE         ischangehistoryused,
                   FALSE         isheadercolumn,
                   FALSE         isnullable,
                   FALSE         isincludedinuniqueconstraint,
                   'primary key' purpose,
                   'a'           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'wrwellid'                          name,
                   'Well'                              label,
                   l_sysdictionarytableid,
                   l_sysdictionarytableid - 1 ::BIGINT sysdictionarytableidforeign,
                   NULL::INT                           datalength,
                   'bigint'                            datatype,
                   NULL::INT                           decimals,
                   NULL                                defaultvalue,
                   TRUE                                ischangehistoryused,
                   TRUE                                isheadercolumn,
                   FALSE                               isnullable,
                   FALSE                               isincludedinuniqueconstraint,
                   'foreign key'                       purpose,
                   'a'                                 rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'crmcontactid'     name,
                   'Registered Owner' label,
                   l_sysdictionarytableid,
                   100 ::BIGINT       sysdictionarytableidforeign,
                   NULL::INT          datalength,
                   'bigint'           datatype,
                   NULL::INT          decimals,
                   NULL               defaultvalue,
                   TRUE               ischangehistoryused,
                   TRUE               isheadercolumn,
                   FALSE              isnullable,
                   FALSE              isincludedinuniqueconstraint,
                   'foreign key'      purpose,
                   'a'                rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'temporalstartdate'           name,
                   'Registration Effective Date' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT                  sysdictionarytableidforeign,
                   NULL::INT                     datalength,
                   'date'                        datatype,
                   NULL::INT                     decimals,
                   NULL                          defaultvalue,
                   TRUE                          ischangehistoryused,
                   TRUE                          isheadercolumn,
                   FALSE                         isnullable,
                   FALSE                         isincludedinuniqueconstraint,
                   'temporal'                    purpose,
                   'a'                           rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'temporalenddate'          name,
                   'Registration Expiry Date' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT               sysdictionarytableidforeign,
                   NULL::INT                  datalength,
                   'date'                     datatype,
                   NULL::INT                  decimals,
                   NULL                       defaultvalue,
                   TRUE                       ischangehistoryused,
                   TRUE                       isheadercolumn,
                   FALSE                      isnullable,
                   FALSE                      isincludedinuniqueconstraint,
                   'temporal'                 purpose,
                   'a'                        rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'rowstatus'  name,
                   'Row Status' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT sysdictionarytableidforeign,
                   NULL::INT    datalength,
                   'char'       datatype,
                   NULL::INT    decimals,
                   NULL         defaultvalue,
                   FALSE        ischangehistoryused,
                   FALSE        isheadercolumn,
                   FALSE        isnullable,
                   FALSE        isincludedinuniqueconstraint,
                   'system'     purpose,
                   'a'          rowstatus,
                   l_NewChangeHistoryID
            UNION ALL
            SELECT 'syschangehistoryid'     name,
                   'Last Change History Id' label,
                   l_sysdictionarytableid,
                   NULL::BIGINT             sysdictionarytableidforeign,
                   NULL::INT                datalength,
                   'bigint'                 datatype,
                   NULL::INT                decimals,
                   NULL                     defaultvalue,
                   FALSE                    ischangehistoryused,
                   FALSE                    isheadercolumn,
                   FALSE                    isnullable,
                   FALSE                    isincludedinuniqueconstraint,
                   'audit'                  purpose,
                   'a'                      rowstatus,
                   l_NewChangeHistoryID;

            CALL spsysschemaupdate();
            CALL spsyschangehistoryrefreshtriggers(TRUE);

            INSERT INTO glaccount (glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (2700, 60, l_sysmultilinktableruleid, NULL, 5, 1, NULL, 'Well Registration Revenue', TRUE, TRUE, NULL, '4100', 71, 'a', l_NewChangeHistoryID)
            RETURNING id INTO l_glaccountid;

            INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (fnsysidGet(1, 36), 1900, 30, NULL, NULL, 3, 2, NULL, 'GST Payable', TRUE, FALSE, NULL, '2150', 40, 'a', NULL);

            INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
            VALUES (fnsysidGet(1, 37), fnsysidGet(1, 36), 30, NULL, NULL, 4, 1, NULL, 'GST Collected', TRUE, TRUE, NULL, '2150', 41, 'a', NULL);

        END IF;

        INSERT INTO wrWell (description, welltype, purpose, gpslocation, rowstatus, syschangehistoryid, status)
        SELECT 'AXWI100010600917W400',
               'Pumpjack',
               'oil',
               '47.283454, -110.622318',
               'a',
               l_NewChangeHistoryID,
               'Flowing'
        RETURNING id
            INTO l_wrWellID;

        SELECT ROW_TO_JSON(a)
        INTO l_json
        FROM (
                 SELECT fnsysidGet(1, 158119) crmcontactid, l_wrWellID wrwellid) AS a;
        CALL spsysTemporalDataUpdate(p_Id := l_wrWellRegistrationID,
                                     p_EffectiveDate := '2022-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := l_sysdictionarytableid,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);


        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Create well registration transaction ');

        INSERT INTO glbatch (glbatchtypeid, description)
        SELECT 1, 'Well Registration'
        RETURNING id INTO l_glbatchid;

        INSERT INTO gltransaction (glbatchid, glpostingstatusid, gltransactiontypeid, description, transactiondate, syschangehistoryid)
        SELECT l_glbatchid, 2, 1, 'Well Registration', NOW()::DATE, l_NewChangeHistoryID
        RETURNING id INTO l_gltransactionid;

        INSERT INTO glentry (glaccountid, glcostcentreid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, description, syschangehistoryid)
        SELECT fnsysidGet(0, 1400),
               0,
               l_gltransactionid,
               321,
               fnsysidGet(1, 351233),
               1050,
               'Well Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT fnsysidGet(1, 37),
               0,
               l_gltransactionid,
               NULL,
               NULL,
               -50,
               'Well Registration Fee',
               l_NewChangeHistoryID
        UNION ALL
        SELECT l_GLAccountID,
               0,
               l_gltransactionid,
               l_sysdictionarytableid,
               l_wrWellRegistrationID,
               -1000,
               'Well Registration Fee',
               l_NewChangeHistoryID;

        CALL spglBatchApprove(l_glbatchid);

        SET SEARCH_PATH TO DEFAULT;
    END;
$$ LANGUAGE plpgsql;
/*
=========================================================================================
Test 21

This tests merging the databases into a new schema

*/
DO
$$
    DECLARE
        l_schemadatawarehouse VARCHAR;
    BEGIN
        -- The first call p_schemadatawarehouse is null so it creates the DW.  p_schemadatawarehouse is an INOUT parameter
-- so it will assign it to l_schemadatawarehouse and use it in the second call as an update.
        CALL spsysDatawarehouseCreate(p_schemalist := 's0001v0000, s0005v0000, s0006v0000, s0007v0000',
                                      p_schemadatawarehouse := l_schemadatawarehouse);
        CALL spsysViewCreate(l_schemadatawarehouse);
        SET SEARCH_PATH TO DEFAULT;
    END;
$$ LANGUAGE plpgsql;
/*

SET SEARCH_PATH TO s0008v0000, s0000v0000, public;
select * from vwglentry where rowidchargedto=10000000351233 and sysdictionarytableidchargedto=321;
*/
DO
$$
    DECLARE
        l_schemadatawarehouse VARCHAR := 's0008v0000';
    BEGIN

        UPDATE s0007v0000.crmcontact
        SET name=leftfind(name, ' as of ') || ' as of ' || NOW()::TIME::VARCHAR,
            sysChangeHistoryID=fnsysChangeHistoryCreate('Test if updates flow to Data warehouse');

        CALL spsysDatawarehouseCreate(p_schemalist := 's0001v0000, s0005v0000, s0006v0000, s0007v0000',
                                      p_schemadatawarehouse := l_schemadatawarehouse);
    END;
$$ LANGUAGE plpgsql;
/*
-- only the updates to v0007 records should flow through
SELECT a.id, a.name v0007name, b.name v0008name FROM s0007v0000.crmcontact a
join s0008v0000.crmcontact b on b.id=a.id;

QUERIES

--Record Group
SELECT uid, LEFT('......', (topdownlevel - 1) * 3) || name AS name, dictionarytable, whereclause
FROM vwexrecordgroup
ORDER BY displaysequence;

--Subscriber
SELECT uid, LEFT('......', (topdownlevel - 1) * 3) || name AS name, system
FROM vwexsubscriber
ORDER BY displaysequence;

--Subscription
SELECT *
FROM exsubscription;

--Change History
SELECT *
FROM vwsyschangehistory
ORDER BY id DESC

--Change History Row
SELECT *
FROM vwsyschangehistoryrow
ORDER BY id DESC

-- Find all subscribers to data
SELECT DISTINCT b.exsystemid,
                c.name                              SubscriptionData,
                fnsysIDView(a.rowidsubscribedto)    rowidsubscribedto,
                fnsysIDView(c.sysdictionarytableid) sysdictionarytableid
FROM exsubscriptiondetail a
JOIN exsubscriber b
     ON b.id = a.exsubscriberid
JOIN exrecordgroup c
     ON c.id = a.exrecordgroupid
ORDER BY c.name,
         fnsysIDView(a.rowidsubscribedto)

-- Show all systems that will get an update for a particular record
SELECT DISTINCT b.exsystemid
FROM exsubscriptiondetail a
JOIN exsubscriber b
     ON b.id = a.exsubscriberid
WHERE rowidsubscribedto = fnsysIDGet(1, 128)
ORDER BY b.exsystemid

select * from (
select 's0001v0000' system, * from s0001v0000.exsubscriptiondetail union
select 's0005v0000' system, * from s0005v0000.exsubscriptiondetail union
select 's0006v0000' system, * from s0006v0000.exsubscriptiondetail union
select 's0007v0000' system, * from s0007v0000.exsubscriptiondetail union
select 's0009v0000' system, * from s0009v0000.exsubscriptiondetail
) a
 order by system;

             */