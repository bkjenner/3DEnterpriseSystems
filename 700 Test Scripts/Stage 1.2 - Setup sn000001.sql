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
This script sets of the main subnet server sn00001 for the stage 1 test environment
*/
DROP SCHEMA IF EXISTS s0002v0000 CASCADE;
DROP SCHEMA IF EXISTS s0005v0000 CASCADE;
DROP SCHEMA IF EXISTS s0006v0000 CASCADE;
DROP SCHEMA IF EXISTS s0007v0000 CASCADE;
DROP SCHEMA IF EXISTS s0008v0000 CASCADE;

DO
$$
    DECLARE
        l_JSON JSON;
        l_id   BIGINT;
        l_sql  VARCHAR;
    BEGIN

        -- Added this in on 11/26/2021 because I was losing connections
        IF DBLINK_GET_CONNECTIONS() IS NOT NULL
        THEN
            PERFORM DBLINK_DISCONNECT('aws');
        END IF;

        PERFORM fnsysMDSConnect();

        SET SEARCH_PATH TO DEFAULT;

        SELECT STRING_AGG(REPLACE(REPLACE(REPLACE('
CREATE OR REPLACE VIEW l_tablename AS l_viewdefinition
', 'l_tablename', table_name), 'l_viewdefinition', view_definition), 'c_', ''), '')
        INTO l_sql
        FROM information_schema.views
        WHERE table_schema = 's0000v0000';

        EXECUTE (l_SQL);

        SET session_replication_role = REPLICA;
        DELETE FROM exHistory;
        DELETE FROM exHistoryBatch;
        DELETE FROM exSubscriptionDetail;
        DELETE FROM exSubscription;
        DELETE FROM exRecordGroup;
        DELETE FROM exSubscriber;
        DELETE FROM syschangehistorycolumn;
        DELETE FROM sysChangeHistoryRow;
        DELETE FROM sysChangeHistory;
        DELETE FROM glrate;
        DELETE FROM exsystem;
        DELETE FROM exSubnetServer;

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS;

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws AS v;

        COPY exSubscriber FROM 'c:\Temp\BMSData\exSubscriber.csv' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY exRecordGroup FROM 'c:\Temp\BMSData\exRecordGroup.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        l_id = NULL;
        l_JSON := '{
          "comprovincestateid": 470,
          "glratetypeid": 3,
          "description": "Ontario HST",
          "rate": 0.1300
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '1999-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := NULL
            );

        l_id = NULL;
        l_JSON := '{
          "comprovincestateid": 340,
          "glratetypeid": 3,
          "description": "New Brunswick HST",
          "rate": 0.1500
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '1999-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := NULL
            );

        l_id = NULL;
        l_JSON := '{
          "comprovincestateid": null,
          "glratetypeid": 1,
          "description": "Gst description",
          "rate": 0.0500
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '1999-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := NULL
            );

        SET client_min_messages = WARNING;
        PERFORM fnsysIDSetVal('exHistory');
        PERFORM fnsysIDSetVal('exHistoryBatch');
        PERFORM fnsysIDSetVal('exSubscriptionDetail');
        PERFORM fnsysIDSetVal('exSubscription');
        PERFORM fnsysIDSetVal('exRecordGroup');
        PERFORM fnsysIDSetVal('exSubscriber');
        PERFORM fnsysIDSetVal('syschangehistorycolumn');
        PERFORM fnsysIDSetVal('sysChangeHistoryRow');
        PERFORM fnsysIDSetVal('sysChangeHistory');
        PERFORM fnsysIDSetVal('glrate');
        PERFORM fnsysIDSetVal('exsystem');
        PERFORM fnsysIDSetVal('exSubnetServer');
        SET client_min_messages = NOTICE;

        CALL spsysschemacreate(
                p_firstname := '',
                p_lastname := '',
                p_username := '',
                p_email := '',
                p_organizationname := 'Subnet Server 1',
                p_systemname := 'System 2',
                p_IsSubnetServer := TRUE,
                p_exSystemID := 2
            );

        PERFORM SET_CONFIG('search_path', 's0002v0000,' || CURRENT_SETTING('search_path'), TRUE);

        INSERT INTO exSubnetServer (id, name, systemidSubnetServer, rowstatus)
        SELECT id, name, systemidSubnetServer, rowstatus
        FROM vwexSubnetServerAWS a
        WHERE NOT EXISTS(SELECT FROM exSubnetServer aa WHERE aa.id = a.id);

        INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus)
        SELECT id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus
        FROM vwexsystemaws a
        WHERE NOT EXISTS(SELECT FROM exsystem aa WHERE aa.id = a.id);

        CALL spsysschemacreate(
                p_firstname := 'Donald',
                p_lastname := 'Mundle',
                p_username := 'DMundle',
                p_email := 'd.mundle@hotmail.com',
                p_organizationname := 'Organization 5',
                p_systemname := 'System 5',
                p_exSystemID := 5);

        CALL spsysschemacreate(
                p_firstname := 'Denis',
                p_lastname := 'Halep',
                p_username := 'DHalep',
                p_email := 'dhalep@shaw.ca',
                p_organizationname := 'Organization 6',
                p_systemname := 'System 6',
                p_exSystemID := 6);

        CALL spsysschemacreate(
                p_firstname := 'Donald',
                p_lastname := 'Morgan',
                p_username := 'DMorgan',
                p_email := 'dkmorgan@gmail.com',
                p_organizationname := 'Organization 7',
                p_systemname := 'System 7',
                p_exSystemID := 7);

        -- Delete glrate because it is used for subscriptions and testing
        DELETE FROM s0002v0000.glratetype;
        DELETE FROM s0005v0000.glratetype;
        DELETE FROM s0006v0000.glratetype;
        DELETE FROM s0007v0000.glratetype;
        DELETE FROM s0002v0000.glrate;
        DELETE FROM s0005v0000.glrate;
        DELETE FROM s0006v0000.glrate;
        DELETE FROM s0007v0000.glrate;

        SET SEARCH_PATH TO DEFAULT;
        DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
        CREATE TRIGGER exHistoryCallImport
            AFTER INSERT
            ON exHistory
            FOR EACH STATEMENT
        EXECUTE PROCEDURE trexHistoryImportCall();

        SET SEARCH_PATH TO s0002v0000, s0000v0000, public;
        DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
        CREATE TRIGGER exHistoryCallImport
            AFTER INSERT
            ON exHistory
            FOR EACH STATEMENT
        EXECUTE PROCEDURE trexHistoryImportCall();

        SET SEARCH_PATH TO s0005v0000, s0000v0000, public;
        DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
        CREATE TRIGGER exHistoryCallImport
            AFTER INSERT
            ON exHistory
            FOR EACH STATEMENT
        EXECUTE PROCEDURE trexHistoryImportCall();

        SET SEARCH_PATH TO s0006v0000, s0000v0000, public;
        DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
        CREATE TRIGGER exHistoryCallImport
            AFTER INSERT
            ON exHistory
            FOR EACH STATEMENT
        EXECUTE PROCEDURE trexHistoryImportCall();

        SET SEARCH_PATH TO s0007v0000, s0000v0000, public;
        DROP TRIGGER IF EXISTS exHistoryCallImport ON exHistory;
        CREATE TRIGGER exHistoryCallImport
            AFTER INSERT
            ON exHistory
            FOR EACH STATEMENT
        EXECUTE PROCEDURE trexHistoryImportCall();

        SET SEARCH_PATH TO DEFAULT;

    END

$$ LANGUAGE plpgsql;


