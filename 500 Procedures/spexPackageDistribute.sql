CREATE OR REPLACE PROCEDURE S0000V0000.spexPackageDistribute(p_debug BOOLEAN DEFAULT FALSE)
AS
$$
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
This procedure takes data packages that have been received on a subnet server and distributes them to
local schemas and also distributes to other subnet servers.  To accomplish this it performs the following steps

- Get all packages from the AWS server that are intended for this subnet server
and insert the records into the exHistory and changehistory tables
- Distribute the data to all systems on the current SubnetServer
- If there were any exhistory records that were produced for a different
subnet server other than the current one then put them on the AWS server.

CHANGE LOG
20211021 Blair Kjenner	Initial Code

PARAMETERS
None

SAMPLE CALL

call spexPackageDistribute();

*/
DECLARE
    e_Context                 TEXT;
    e_Msg                     TEXT;
    e_State                   TEXT;
    l_Error                   RECORD;
    l_Rec                     RECORD;
    l_SQL                     VARCHAR;
    l_currentSearchPath       VARCHAR;
    l_now                     TIMESTAMP;
    l_package                 JSON;
    l_expackageid             BIGINT;
    l_packagetext             TEXT;
    l_currentexSubnetServerid INT;
BEGIN

    l_now := NOW();

    l_currentexSubnetServerid := fnsysCurrentSubnetServerID();

    IF fnsyscurrentsystemid() != fnsyscurrentSubnetServersystemid()
    THEN
        RAISE SQLSTATE '51075';
    END IF;

    DROP TABLE IF EXISTS t_jsonimport;
    CREATE TEMP TABLE t_jsonimport
    (
        tablename VARCHAR,
        package   json
    );

    -- Import data packages from the subnet server
    LOOP
        SELECT id, package::JSON
        INTO l_expackageid, l_package
        FROM vwexpackageaws
        WHERE exSubnetServerid = l_currentexSubnetServerid
          AND readdate IS NULL
        ORDER BY ID
        LIMIT 1;

        EXIT WHEN l_package IS NULL;

        TRUNCATE TABLE t_jsonimport;
        INSERT INTO t_jsonimport (tablename, package)
        SELECT *
        FROM JSON_POPULATE_RECORDSET(NULL::t_jsonimport, l_package);

        SELECT package::JSON INTO l_package FROM t_jsonimport WHERE TableName = 'syschangehistory';
        INSERT INTO syschangehistory(
            id,
            crmcontactiduser,
            syscommandid,
            sysdictionarytableidappliesto,
            rowidappliesto,
            rowtemporalenddate,
            changedate,
            isexported,
            ismaxrecordsignored,
            COMMENTS,
            syschangehistoryidundo,
            rowstatus)
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
        FROM JSON_POPULATE_RECORDSET(NULL::syschangehistory, l_package) A
        LEFT JOIN crmcontact b
                  ON b.id = a.crmcontactiduser
        LEFT JOIN syscommand c
                  ON c.id = a.syscommandid
        WHERE a.id NOT IN (
                              SELECT id
                              FROM syschangehistory);

        SELECT package::JSON INTO l_package FROM t_jsonimport WHERE TableName = 'syschangehistoryrow';
        INSERT INTO syschangehistoryrow
        SELECT *
        FROM JSON_POPULATE_RECORDSET(NULL::syschangehistoryrow, l_package) A
        WHERE a.id NOT IN (
                              SELECT id
                              FROM syschangehistoryrow);

        SELECT package::JSON INTO l_package FROM t_jsonimport WHERE TableName = 'syschangehistorycolumn';
        -- If there are no changes it will cause an issue dont insert
        IF l_package IS NOT NULL
        THEN
            INSERT INTO syschangehistorycolumn
            SELECT *
            FROM JSON_POPULATE_RECORDSET(NULL::syschangehistorycolumn, l_package) A
            WHERE a.id NOT IN (
                                  SELECT id
                                  FROM syschangehistorycolumn)
              AND EXISTS(SELECT FROM sysdictionarycolumn aa WHERE aa.id = a.sysdictionarycolumnid);
        END IF;

        SELECT package::JSON INTO l_package FROM t_jsonimport WHERE TableName = 'exhistorybatch';
        INSERT INTO exHistoryBatch
        SELECT *
        FROM JSON_POPULATE_RECORDSET(NULL::exHistoryBatch, l_package) A
        WHERE a.id NOT IN (
                              SELECT id
                              FROM exHistoryBatch);

        SELECT package::JSON INTO l_package FROM t_jsonimport WHERE TableName = 'exhistory';
        INSERT INTO exHistory
        SELECT *
        FROM JSON_POPULATE_RECORDSET(NULL::exHistory, l_package) A
        WHERE a.id NOT IN (
                              SELECT id
                              FROM exHistory);

        IF l_exPackageID IS NOT NULL
        THEN
            l_sql := FORMAT('
        UPDATE s0000v0000.exPackage SET readdate=now()::timestamp WHERE id = %s;
        ', l_exPackageID);
            PERFORM fnsysMDSExecute(l_sql);
        END IF;

    END LOOP;

    l_currentSearchPath := CURRENT_SETTING('search_path');

    DROP TABLE IF EXISTS t_exHistoryBatchDist;
    CREATE TEMP TABLE t_exHistoryBatchDist
    AS
    SELECT *
    FROM exHistoryBatch A
    WHERE a.DistributionDate IS NULL;

    FOR l_rec IN SELECT DISTINCT b.id                                               exSystemID,
                                 fnsysCurrentSchema(b.id::INT, b.productionversion) destschema
                 FROM exhistory a
                 JOIN exsystem b
                      ON b.id = a.exsystemiddestination
                 JOIN t_exhistorybatchDist c
                      ON c.id = a.exhistorybatchid
                 WHERE b.exSubnetServerid = l_currentexSubnetServerid
                   AND c.DistributionDate IS NULL
                   AND b.id != fnsysCurrentSystemID()
    LOOP

        DROP TABLE IF EXISTS t_exHistoryDist;
        CREATE TEMP TABLE t_exHistoryDist
        AS
        SELECT a.*
        FROM exHistory A
        JOIN t_exHistoryBatchDist b
             ON A.exhistorybatchid = b.id
        WHERE a.exsystemiddestination = l_rec.exsystemid;

        DROP TABLE IF EXISTS t_sysChangeHistoryRow;
        CREATE TEMP TABLE t_sysChangeHistoryRow
        AS
        SELECT a.*
        FROM sysChangeHistoryRow A
        WHERE a.id in (select aa.syschangehistoryrowidexported from t_exHistoryDist aa);

        DROP TABLE IF EXISTS aasyschangehistory;
        CREATE TABLE aasyschangehistory
        AS
        SELECT *
        FROM syschangehistory A
        WHERE a.id IN (
                          SELECT syschangehistoryid
                          FROM t_sysChangeHistoryRow
                          UNION
                          -- Need to also include change
                          -- history batch records
                          SELECT sysChangeHistoryID
                          FROM t_exHistoryBatchDist);

        DROP TABLE IF EXISTS t_syschangehistory;
        CREATE TEMP TABLE t_syschangehistory
        AS
        SELECT *
        FROM syschangehistory A
        WHERE a.id IN (
                          SELECT syschangehistoryid
                          FROM t_sysChangeHistoryRow
                          UNION
                          -- Need to also include change
                          -- history batch records
                          SELECT sysChangeHistoryID
                          FROM t_exHistoryBatchDist);

        DROP TABLE IF EXISTS t_sysChangeHistoryColumn;
        CREATE TEMP TABLE t_sysChangeHistoryColumn
        AS
        SELECT *
        FROM sysChangeHistoryColumn A
        WHERE a.syschangehistoryrowid IN (
                                             SELECT id
                                             FROM t_sysChangeHistoryRow);

        PERFORM SET_CONFIG('search_path', l_rec.destschema || ', ' || l_currentSearchPath, TRUE);
            raise notice 'schema %', l_rec.destschema;

        INSERT INTO syschangehistory (
            id,
            crmcontactiduser,
            syscommandid,
            sysdictionarytableidappliesto,
            rowidappliesto,
            rowtemporalenddate,
            changedate,
            isexported,
            ismaxrecordsignored,
            COMMENTS,
            syschangehistoryidundo,
            rowstatus)
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

        INSERT INTO syschangehistoryrow (
            id,
            syschangehistoryid,
            sysdictionarytableidappliesto,
            rowidappliesto,
            rowtemporalenddate,
            changedate,
            actiontype,
            operationtype,
            olddata,
            newdata,
            isprocessed)
        SELECT id,
               syschangehistoryid,
               sysdictionarytableidappliesto,
               rowidappliesto,
               rowtemporalenddate,
               changedate,
               actiontype,
               operationtype,
               olddata,
               newdata,
               isprocessed
        FROM t_sysChangeHistoryRow a
        WHERE NOT EXISTS(SELECT FROM syschangehistoryrow aa WHERE aa.id = a.id);

        INSERT INTO syschangehistorycolumn (
            id,
            syschangehistoryrowid,
            sysdictionarycolumnid,
            sysdictionarytableidbefore,
            sysdictionarytableidafter,
            rawdatabefore,
            rawdataafter,
            translateddatabefore,
            translateddataafter)
        SELECT id,
               syschangehistoryrowid,
               sysdictionarycolumnid,
               sysdictionarytableidbefore,
               sysdictionarytableidafter,
               rawdatabefore,
               rawdataafter,
               translateddatabefore,
               translateddataafter
        FROM t_sysChangeHistoryColumn a
        WHERE NOT EXISTS(SELECT FROM syschangehistorycolumn aa WHERE aa.id = a.id)
          AND EXISTS(SELECT FROM sysdictionarycolumn aa WHERE aa.id = a.sysdictionarycolumnid);

        INSERT INTO exHistoryBatch (id, createdate, distributiondate, applieddate, syschangehistoryid)
        SELECT id, createdate, l_now, applieddate, syschangehistoryid
        FROM t_exHistoryBatchDist a
        WHERE EXISTS(SELECT FROM t_exHistoryDist aa WHERE aa.exhistorybatchid = a.id)
          AND NOT EXISTS(SELECT FROM exHistoryBatch aa WHERE aa.id = a.id);

        INSERT INTO exHistory(
            id, exHistoryBatchId, syschangehistoryrowidexported, exsystemiddestination, source)
        SELECT id, exHistoryBatchId, syschangehistoryrowidexported, exsystemiddestination, source
        FROM t_exHistoryDist a
        WHERE NOT EXISTS(SELECT FROM exHistory aa WHERE aa.id = a.id);

        PERFORM SET_CONFIG('search_path', l_currentSearchPath, TRUE);

    END LOOP;

    DROP TABLE IF EXISTS t_jsonexport;
    CREATE TEMP TABLE t_jsonexport
    (
        tablename VARCHAR,
        package   json
    );

    FOR l_rec IN SELECT DISTINCT b.exSubnetServerid,
                                 fnsysCurrentSubnetServerName(b.exSubnetServerid::INT) destdataservice
                 FROM exhistory a
                 JOIN exsystem b
                      ON b.id = a.exsystemiddestination
                 JOIN t_exHistoryBatchDist c
                      ON c.id = a.exhistorybatchid
                 WHERE b.exSubnetServerid != fnsyscurrentSubnetServerid()
                   AND c.DistributionDate IS NULL
    LOOP

        DROP TABLE IF EXISTS t_exHistoryDist;
        CREATE TEMP TABLE t_exHistoryDist
        AS
        SELECT a.*
        FROM exHistory A
        JOIN exsystem b
             ON b.id = a.exsystemiddestination
        JOIN t_exHistoryBatchDist c
             ON c.id = a.exhistorybatchid
        WHERE b.exSubnetServerid = l_rec.exSubnetServerid;

        DROP TABLE IF EXISTS t_sysChangeHistoryRow;
        CREATE TEMP TABLE t_sysChangeHistoryRow
        AS
        SELECT a.*
        FROM sysChangeHistoryRow A
        WHERE id in (select syschangehistoryrowidexported from t_exHistoryDist);

        DROP TABLE IF EXISTS t_syschangehistory;
        CREATE TEMP TABLE t_syschangehistory
        AS
        SELECT *
        FROM syschangehistory A
        WHERE a.id IN (
                          SELECT syschangehistoryid
                          FROM t_sysChangeHistoryRow
                          UNION
                          -- Need to also include change
                          -- history batch records
                          SELECT sysChangeHistoryID
                          FROM t_exHistoryBatchDist);

        DROP TABLE IF EXISTS t_sysChangeHistoryColumn;
        CREATE TEMP TABLE t_sysChangeHistoryColumn
        AS
        SELECT *
        FROM sysChangeHistoryColumn A
        WHERE a.syschangehistoryrowid IN (
                                             SELECT id
                                             FROM t_sysChangeHistoryRow);

        DROP TABLE IF EXISTS t_jsonexport;
        CREATE TEMP TABLE t_jsonexport
        AS
        SELECT 'exhistorybatch' tablename, JSONB_AGG(ROW_TO_JSON(a)) package
        FROM t_exHistoryBatchDist AS a
        UNION
        SELECT 'exhistory' tablename, JSONB_AGG(ROW_TO_JSON(a)) package
        FROM t_exHistoryDist AS a
        UNION
        SELECT 'syschangehistoryrow', JSONB_AGG(ROW_TO_JSON(a)) package
        FROM t_syschangehistoryrow AS a
        UNION
        SELECT 'syschangehistorycolumn', JSONB_AGG(ROW_TO_JSON(a)) package
        FROM t_syschangehistorycolumn AS a
        UNION
        SELECT 'syschangehistory', JSONB_AGG(ROW_TO_JSON(a)) package
        FROM t_syschangehistory AS a;

        SELECT JSONB_AGG(ROW_TO_JSON(a))
        INTO l_package
        FROM t_jsonexport AS a;

        l_packagetext := CAST(l_package AS TEXT);
        PERFORM fnsysMDSPutPackage(p_exSubnetServerID := l_rec.exSubnetServerid, p_Package := l_PackageText);

    END LOOP;

    UPDATE exHistoryBatch A
    SET DistributionDate = l_now
    WHERE a.id IN (
                      SELECT id
                      FROM t_exHistoryBatchDist);

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_state = RETURNED_SQLSTATE,
            e_msg = MESSAGE_TEXT,
            e_context = PG_EXCEPTION_CONTEXT;
        l_error := fnsysError(e_state, e_msg, e_context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', l_Error.Message;
        ELSE
            RAISE NOTICE '%', l_Error.Message ;
        END IF;

END ;
$$ LANGUAGE plpgsql


