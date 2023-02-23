CREATE OR REPLACE PROCEDURE S0000V0000.spexPackageImport(p_exHistoryBatchID BIGINT DEFAULT NULL,
                                                         p_OverrideWarningConditions BOOLEAN DEFAULT FALSE,
                                                         p_debug BOOLEAN DEFAULT FALSE,
                                                         p_sysChangeHistoryID BIGINT DEFAULT NULL)
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
This procedure imports data from a subscriber based on the following process.
- check error conditions with importing the data and generates error messages
- imports the data
Each of these tasks are described below

ERROR CHECKING PROCESS
Generate message records based on the following conditions.
1.Record has been updated internally
- System checks the previous change history event for any of the records in the change history transaction being
processed to see if the record was changed locally.
2.Columns have been added to the recipient system
3.Data format does not match the data format in the local system and (no default values have been established
for new columns and the columns are not nullable), an error message will be generated
4.Column added by governor system
- If the data format does not match the data format of the recipient system as a result of the governor system adding
columns, the user will be warned.  This error can be overridden.
7.Column has been changed to mandatory on recipient system and no value has been supplied, and no default has been
specified.
8.Data format is different between recipient and sender because one of them has made a change

UPDATE PROCESS
1. Check for error conditions.  If any exist, update the associated message records and exit.
2. Insert missing foreign key values.  Note the changehistory id of the current change history event will be used on
the new record so the update
3. Process the change history event (exactly like an undo but uses the new data).
4. Change the status of the change history event to processed.
5. Notify the subnet server that the package was processed so it can notify the governor.

CHANGE LOG
20211029 Blair Kjenner	Initial Code

PARAMETERS

p_exHistoryBatchID   - DEFAULTS TO NULL.  If null, all outstanding batches (with no errors) are imported.
p_OverrideWarningConditions - DEFAULTS to FALSE - If TRUE warning conditions will be ignored (but error conditions will
                              not be,
p_debug BOOLEAN             - DEFAULT FALSE - causes SQL script to be output

SAMPLE CALL

call spexPackageimport(null, true, true);

*/
DECLARE
    e_Context       TEXT;
    e_Msg           TEXT;
    e_State         TEXT;
    l_Error         RECORD;
    l_SQL           VARCHAR;
    l_tRec          RECORD;
    l_Data          JSON;
    l_exmessagesave JSON;
BEGIN

    DROP TABLE IF EXISTS t_table;
    CREATE TEMP TABLE t_table
    (
        TableName  VARCHAR,
        ColumnList VARCHAR,
        UpdateList VARCHAR,
        Source     VARCHAR,
        PRIMARY KEY (TableName, Source)
    );

    DROP TABLE IF EXISTS t_column;
    CREATE TEMP TABLE t_column
    (
        DatabaseColumn  VARCHAR,
        ImportColumn    VARCHAR,
        OrdinalPosition INT
    );

    DROP TABLE IF EXISTS t_exmessage;
    CREATE TEMP TABLE t_exmessage
    (
        exHistoryBatchId     BIGINT,
        exhistoryid          BIGINT,
        sysdictionarytableid BIGINT,
        sysmessageid         BIGINT,
        messageseverity      INT,
        description          VARCHAR
    );

    -- set the message override to true if it is false and there is a matching message
    -- in the message override table
    UPDATE exmessage a
    SET isoverridden= TRUE, syschangehistoryid=p_sysChangeHistoryID
    FROM exhistorybatch b
    WHERE b.id = a.exhistorybatchid
      AND b.applieddate IS NULL
      AND a.isoverridden = FALSE
      AND EXISTS(SELECT
                 FROM exmessageoverride aa
                 WHERE aa.sysdictionarytableid = aa.sysdictionarytableid
                   AND aa.description = a.description
                   AND COALESCE(aa.sysmessageid, 0) = COALESCE(a.sysmessageid, 0));

    -- Process a batch if it is specifically referenced on p_exHistoryBatchID
    -- or if it has not been applied and does not have errors associated with it
    -- that have not been overridden.
    DROP TABLE IF EXISTS t_exhistorybatchimport;
    CREATE TEMP TABLE t_exhistorybatchimport AS
    SELECT id
    FROM exhistorybatch a
    WHERE (a.id = p_exHistoryBatchID AND a.applieddate IS NULL)
       OR (p_exHistoryBatchID IS NULL
        AND a.id IN (
                        SELECT DISTINCT bb.id
                        FROM exhistory aa
                        JOIN exhistorybatch bb
                             ON bb.id = aa.exhistorybatchid
                                 AND aa.exsystemiddestination = fnsysCurrentSystemID()
                                 AND bb.AppliedDate IS NULL
                                 AND NOT EXISTS(SELECT
                                                FROM exmessage aaa
                                                WHERE aaa.exhistorybatchid = bb.id
                                                  AND (aaa.messageseverity = 1
                                                    OR (aaa.messageseverity = 2
                                                        AND p_OverrideWarningConditions = FALSE
                                                        AND aaa.isoverridden = FALSE)))));


    DROP TABLE IF EXISTS t_exhistoryimport;
    CREATE TEMP TABLE t_exhistoryimport
    AS
    SELECT a.*
    FROM exhistory a
        -- We are only interested in exhistory records that are intended for the system
        -- we are importing to.  Otherwise, for systems like the data server that
        -- see many records go through it, if there is even one record in a batch
        -- that is intended for the data server, everything will get imported.
    WHERE a.exsystemiddestination = fnsysCurrentSystemID()
      AND a.exhistorybatchid IN (
                                    SELECT id
                                    FROM t_exhistorybatchimport aa);

    CREATE INDEX t_exhistoryimport_index
        ON t_exhistoryimport (syschangehistoryrowidexported, ID);

    -- if the batch includes tables that have been dropped, then create a message and link it to the batch record
    INSERT INTO t_exmessage (exHistoryBatchId, sysdictionarytableid, sysMessageID, messageseverity, description)
    WITH tablename AS (
                          SELECT c.relname tablename
                          FROM pg_catalog.pg_class c
                          LEFT JOIN pg_catalog.pg_namespace n
                                    ON n.oid
                                        = c.relnamespace
                          WHERE n.nspname = CURRENT_SCHEMA()
                            AND c.relkind = 'r')
    SELECT DISTINCT a.exhistorybatchid, b.sysdictionarytableidappliesto, d.id, 2, REPLACE(d.description, '%', c.name)
    FROM t_exhistoryimport A
    JOIN      syschangehistoryrow b
              ON b.id = a.syschangehistoryrowidexported
    LEFT JOIN sysdictionarytable c
              ON c.id = b.sysdictionarytableidappliesto
    LEFT JOIN sysmessage d
              ON d.state = '51076'
    WHERE COALESCE(c.rowstatus, 'd') = 'd'
        -- Table does not exist
       OR c.name NOT IN (
                            SELECT *
                            FROM tablename);

    -- if there are no records to import then exit.
    IF NOT EXISTS(SELECT FROM t_exhistoryimport)
    THEN
        RETURN;
    END IF;

    -- If we are reprocessing a batch we delete any messages associated with the batch except for the overridden ones.
    -- If the error continues to exist, it will be recreated
    DELETE
    FROM exmessage e
    WHERE exHistoryBatchId IN (
                                  SELECT DISTINCT exHistoryBatchId
                                  FROM t_exhistoryimport)
      AND isOverridden = FALSE;

    -- Loop through all tables that are found in the exhistoryimport.  For each table, get the table name/id
    -- batch id and source. The sources that can be due to a change history event happening the source system,
    -- a new subscription or a foreign key reference being satisfied.
    -- The high level purpose of this loop is to check the mapping of columns in the import table to the
    -- destination table and to create warnings if there are mapping differences.  As well, we are building
    -- the update command that we will use in the next step.
    -- Note: if we are importing data just because it is a foreign key reference then we will see mapping
    -- differences because we only import header columns for foreign key references.
    FOR l_tRec IN
        SELECT LOWER(dt.Name) dtName,
               h.exHistoryBatchId,
               dt.id          sysdictionarytableid,
               h.source
        FROM sysChangeHistoryRow cht
        JOIN sysDictionaryTable dt
             ON dt.id = cht.sysdictionarytableidappliesto
        JOIN t_exhistoryimport H
             ON H.syschangehistoryrowidexported = cht.id
        WHERE dt.rowstatus = 'a'
          AND fnIfTableExists(dt.name)
        GROUP BY dt.name, h.exHistoryBatchId, dt.id, h.source
    LOOP

        /*
	    Since we are only checking for differences then we only need to get the
        one record for each table and source.  We can assume that all records
        for a given table and source will have the same mapping for a given batch
         */
        SELECT COALESCE(newdata, olddata)
        INTO l_Data
        FROM syschangehistoryrow cht
        JOIN t_exhistoryimport h
             ON H.syschangehistoryrowidexported = cht.id
        WHERE cht.sysdictionarytableidappliesto = l_tRec.sysdictionarytableid
          AND h.source = l_tRec.Source
        LIMIT 1;

        -- This statement inserts all columns found in the input data
        -- into t_column except for the ID and sysChangeHistoryId columns
        TRUNCATE TABLE t_column;
        -- Get the column list from the import data
        INSERT INTO t_column (ImportColumn)
        SELECT LOWER(key)
        FROM JSON_EACH(l_data)
        WHERE LOWER(key) NOT IN ('id', 'syschangehistoryid');

        -- Now we are going to check the columns in the input table to the
        -- columns in the destinate table and update the column name and
        -- ordinal position
        UPDATE t_column a
        SET DatabaseColumn=b.column_name, OrdinalPosition=b.Ordinal_Position
        FROM (
                 SELECT column_name, ordinal_position
                 FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_NAME ILIKE l_tRec.dtName
                   AND TABLE_SCHEMA = CURRENT_SCHEMA()) b
        WHERE a.ImportColumn = b.column_name;

        -- Insert any column names that are in the database but not in
        -- the Import Column list
        INSERT INTO t_column (DatabaseColumn, OrdinalPosition)
        SELECT column_name, ordinal_position
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME ILIKE l_tRec.dtName
          AND TABLE_SCHEMA = CURRENT_SCHEMA()
          AND COLUMN_NAME NOT IN
              ('id', 'syschangehistoryid')
          AND COLUMN_NAME NOT IN (
                                     SELECT ImportColumn
                                     FROM t_column);

        -- Create a record in t_table for each record.  It will contain column list and
        -- update list which we will use when we create the dynamic sql
        INSERT INTO t_table (TableName, ColumnList, UpdateList, Source)
        SELECT *
        FROM (
                 SELECT LOWER(l_tRec.dtName)                                        TableName,
                        STRING_AGG(DatabaseColumn, ', ')                            ColumnList,
                        STRING_AGG(DatabaseColumn || '=b.' || DatabaseColumn, ', ') UpdateList,
                        MIN(l_tRec.Source)                                          Source
                 FROM (
                          SELECT *
                          FROM t_column
                          WHERE DatabaseColumn IS NOT NULL
                            AND ImportColumn IS NOT NULL
                          ORDER BY OrdinalPosition) a) aa
        WHERE NOT EXISTS(SELECT FROM t_table aaa WHERE aaa.tablename = aa.tablename AND aaa.source = aa.Source);

        -- Create a message for each column that is found in the import table that does not exist in the
        -- destination table
        INSERT INTO t_exmessage (exHistoryBatchId, sysdictionarytableid, sysMessageID, messageseverity, description)
        SELECT l_tRec.exHistoryBatchId,
               l_tRec.sysdictionarytableid,
               a.id,
               2,
               REPLACE(a.description, '%', b.extracolumnlist)
        FROM sysmessage A
        JOIN (
                 SELECT STRING_AGG(ImportColumn, ', ') ExtraColumnList
                 FROM t_column
                 WHERE DatabaseColumn IS NULL) b
             ON b.extracolumnlist IS NOT NULL
        WHERE a.state = '51071';

        -- We are not interested in checking tables where foreign key records
        -- were included in the package because we only include header columns
        -- for those records
        IF l_tRec.Source != 'f'
        THEN
            -- Create a message for each column in the destination table that was not found in the
            -- import table
            INSERT INTO t_exmessage (exHistoryBatchId, sysdictionarytableid, sysMessageID, messageseverity, description)
            SELECT l_tRec.exHistoryBatchId,
                   l_tRec.sysdictionarytableid,
                   a.id,
                   2,
                   REPLACE(a.description, '%', b.extracolumnlist)
            FROM sysmessage A
            JOIN (
                     SELECT STRING_AGG(DatabaseColumn, ', ') ExtraColumnList
                     FROM t_column
                     WHERE ImportColumn IS NULL) b
                 ON b.extracolumnlist IS NOT NULL
            WHERE a.state = '51072';
        END IF;

    END LOOP;

    DROP TABLE IF EXISTS t_sysChangeHistoryParm;
    CREATE TEMP TABLE t_sysChangeHistoryParm
    (
        ActionType                  VARCHAR,
        sysChangeHistoryIdForDelete BIGINT
    );
    -- This causes change history not to recreate changehistoryrow records for any data
    -- that changes (since we already have those records)
    INSERT INTO t_sysChangeHistoryParm (actiontype) VALUES ('import');

    -- Now we are going to import the updates for each record in the exHistoryImport table
    -- for the batches that we are going to process.
    -- We are going to use the ColumnList and UpdateList that we created for each Table/Source
    -- in the first step in the process.
    -- We are going to process the records in the following order (batchid, source (f - Foreign key first, r - Refreshes then u - Updates), row id)
    FOR l_tRec IN
        SELECT cht.id                   chtID,
               dt.name                  dtName,
               cht.Rowidappliesto       chtRowID,
               LOWER(cht.OperationType) chtOperationType,
               cht.rowtemporalenddate   chtRowTemporalEnddate,
               cht.olddata              OldData,
               cht.newdata              NewData,
               H.id                     ExHistoryID,
               H.exHistoryBatchId,
               H.source,
               dt.id                    sysDictionaryTableid,
               tt.ColumnList,
               tt.UpdateList
        FROM sysChangeHistoryRow cht
        JOIN sysDictionaryTable dt
             ON dt.id = cht.sysdictionarytableidappliesto
        JOIN t_exhistoryimport H
             ON H.syschangehistoryrowidexported = cht.id
        JOIN t_table tt
             ON LOWER(tt.TableName) = LOWER(dt.name) AND tt.source = h.source
        ORDER BY h.exhistorybatchid, h.source, cht.id
    LOOP
        -- This starts the process with creating an initial statement in the
        -- l_sql variable that sets session_replication_role to replica
        -- This causes SQL not to check for foreign key reference issues until the
        -- end.  This is necessary is cause there is a sequencing issue where a
        -- foreign key reference is not added until after a record that references it
        l_sql := '
-- Current Schema: '' || CURRENT_SCHEMA() || ''
DO
$TEMP$
DECLARE
    e_Msg                TEXT;
    e_State              TEXT;
    l_OldData            json;
    l_NewData            json;
BEGIN
    SET session_replication_role = replica;
';

        -- If the operation type is a delete then add dynamic code to perform the delete
        IF l_tRec.chtOperationType = 'delete'
        THEN
            -- START DYNAMIC CODE
            l_sql := l_sql || '

    -- If the record exists then delete it otherwise create a message to indicate
    -- the record is already delete.  NOTE: variables like p_tablename and
    -- p_temporalcondition will be replaced with data in a later step
    IF EXISTS (SELECT FROM p_tablename a WHERE a.id=p_id p_temporalcondition)
    THEN
        DELETE FROM p_tablename AS a WHERE a.id=p_id p_temporalcondition;
    ELSE
        IF NOT EXISTS (SELECT FROM exmessage a
                        join sysmessage b on b.id=a.sysmessageid
                        WHERE a.exhistoryid=p_exhistoryid
                        AND b.state=''51073'')
        THEN
            INSERT INTO t_exmessage (exHistoryBatchId, sysdictionarytableid, exhistoryid, sysmessageid, messageseverity, description)
            SELECT p_exhistorybatchid, p_sysdictionarytableid, p_exhistoryid, id, 2, description from sysmessage
            WHERE state=''51073'';
        END IF;
    END IF;';
            --END DYNAMIC CODE
        ELSE
            -- If it is not a delete, then add the following dynamic code to the l_sql variable

            -- START DYNAMIC CODE
            l_sql := l_sql || '

    -- OldData represents the before image of a change and NewData represents the after image of a change
    l_NewData := cast(p_newdata as json);
    l_OldData := cast(p_olddata as json);

    IF l_OldData is not null
    THEN
        -- The following IF statement is comparing the OldData to the
        -- actual record in the database to see if it is different.  If it
        -- is then it means it was updated locally and the user should be
        -- warned.  Only create one message per record.
        IF (SELECT count(0) CompareCount FROM
             (SELECT p_columnlist
              FROM p_tablename a WHERE a.id=p_id p_temporalcondition
              UNION
              SELECT p_columnlist
              FROM json_populate_recordset(null::p_tablename,l_OldData)) a) = 2
        THEN
            IF NOT EXISTS (SELECT FROM exmessage a
                            join sysmessage b on b.id=a.sysmessageid
                            WHERE a.exhistoryid=p_exhistoryid
                            AND b.state=''51074'')
            THEN
                INSERT INTO t_exmessage (exHistoryBatchId, sysdictionarytableid, exhistoryid, sysmessageid, messageseverity, description)
                SELECT p_exhistorybatchid, p_sysdictionarytableid, p_exhistoryid, id, 2, description from sysmessage
                WHERE state=''51074'';
            END IF;
        END IF;
    END IF;

    -- If the user has instructed the procedure to override warning conditions
    -- or there are no warning messages or there are warning messages but they
    -- have been overridden, then make the update
    IF p_overridewarningconditions
    OR NOT EXISTS (SELECT FROM t_exmessage a
                    LEFT JOIN exmessage b on b.exHistoryBatchID=a.exHistoryBatchID
                                    and coalesce(b.exHistoryID,0) = coalesce(a.exHistoryID,0)
                                    and b.sysDictionaryTableID = a.sysDictionaryTableID
                                    and b.sysMessageID = a.sysMessageID
                    WHERE a.exHistoryBatchID = p_exhistorybatchid
                    AND coalesce(b.isOverridden, FALSE) = false)
    THEN
        -- If the record exists then update it (unless it is a foreign key reference)
        -- otherwise insert it.  NOTE: We are going to be substituting variables like
        -- p_tablename and p_temporalcondition in a later step.
        IF EXISTS (SELECT FROM p_tablename a WHERE a.id=p_id p_temporalcondition)
        THEN
            -- If the record was imported as a result of a foreign key
            -- dont update it.
            IF ''p_source'' != ''f''
            THEN
                Update p_tablename a set p_updatelist, syschangehistoryid=b.syschangehistoryid
                FROM (SELECT * FROM json_populate_recordset(null::p_tablename,l_NewData) aa) b
                WHERE a.id=p_id p_temporalcondition;
            END IF;
        ELSE
            INSERT INTO p_tablename (id, p_columnlist, syschangehistoryid)
            SELECT id, p_columnlist, syschangehistoryid FROM json_populate_recordset(null::p_tablename,l_NewData);
        END IF;
    END IF;
';
            --END DYNAMIC CODE

        END IF;
        -- Insert the trailing code for the dynamic SQL

        -- START DYNAMIC CODE
        l_sql := l_sql || '
    SET session_replication_role = origin;
    -- If any exceptions are encountered, the following statement will cause the error message to get inserted
    -- into the exMessage table.  Exceptions are caused if errors like foreign key reference errors are generated
    EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_msg = MESSAGE_TEXT, e_state = RETURNED_SQLSTATE;
        insert into t_exmessage (exHistoryBatchId, exhistoryid, sysdictionarytableid, messageseverity, description)
        values ( p_exhistorybatchid, p_exhistoryid, p_sysdictionarytableid, 1, ''Error - '' || e_msg || ''('' || e_state || '')'');
END;
$TEMP$
LANGUAGE plpgsql;';
        -- END DYNAMIC CODE

        -- Now we are going to substitute all our 'p_' variables with data that we collected at the beginning of the loop (eg. l_trec.dtname)
        l_sql :=
                REPLACE(
                        REPLACE(
                                REPLACE
                                    (REPLACE(
                                             REPLACE(
                                                     REPLACE(
                                                             REPLACE(
                                                                     REPLACE(
                                                                             REPLACE(
                                                                                     REPLACE(
                                                                                             REPLACE(
                                                                                                     REPLACE(
                                                                                                             l_sql
                                                                                                         ,
                                                                                                             'p_tablename',
                                                                                                             l_trec.dtname)
                                                                                                 , 'p_columnlist',
                                                                                                     l_trec.columnlist)
                                                                                         , 'p_updatelist',
                                                                                             l_trec.updatelist)
                                                                                 , 'p_id', l_trec.chtrowid::VARCHAR)
                                                                         , 'p_olddata',
                                                                             COALESCE('''[' ||
                                                                                      fixquote(l_trec.olddata::VARCHAR) ||
                                                                                      ']''', 'null'))
                                                                 , 'p_newdata',
                                                                     COALESCE('''[' ||
                                                                              fixquote(l_trec.newdata::VARCHAR) ||
                                                                              ']''', 'null'))
                                                         , 'p_temporalcondition', COALESCE(
                                                                             ' AND a.temporalenddate=''' ||
                                                                             l_trec.chtrowtemporalenddate::VARCHAR ||
                                                                             '''::date', ''))
                                                 , 'p_exhistorybatchid', l_trec.exHistoryBatchId::VARCHAR)
                                         , 'p_exhistoryid', l_trec.exhistoryid::VARCHAR)
                                    , 'p_sysdictionarytableid', l_trec.sysdictionarytableid::VARCHAR)
                            , 'p_source', l_tRec.source)
                    , 'p_overridewarningconditions', CASE WHEN p_OverrideWarningConditions THEN 'TRUE'
                                                          ELSE 'FALSE'
                                                          END);
        -- Execute the SQL
        IF p_DEBUG = TRUE
        THEN
            RAISE NOTICE '%', l_sql;
        END IF;

        EXECUTE (l_SQL);

    END LOOP;

    DROP TABLE IF EXISTS t_sysChangeHistoryParm;

    -- If any exception messages got created as a result of processing the
    -- batch, they will be stored in t_exMessage.
    IF EXISTS(SELECT FROM t_exMessage)
    THEN
        -- If there are severe errors or warning errors have not been overridden
        -- then raise an exception otherwise commit
        IF EXISTS(SELECT
                  FROM t_exmessage a
                  LEFT JOIN exmessage b
                            ON b.exHistoryBatchId = a.exHistoryBatchId
                                AND b.sysdictionarytableid = a.sysdictionarytableid
                                AND COALESCE(b.exhistoryid, 0) = COALESCE(a.exhistoryid, 0)
                                AND b.description = a.description
                      -- If it is a warning but is overridden then that's okay otherwise raise exception
                  WHERE NOT (a.messageseverity = 2 AND (b.isoverridden = TRUE OR
                                                        p_OverrideWarningConditions = TRUE))
                      -- but if there is a severe error then we need to raise an exception
                     OR a.messageseverity = 1)
        THEN
            -- Since we are rolling back, we need to temporarily save JSON to a variable
            SELECT JSONB_AGG(ROW_TO_JSON(a))
            INTO l_exmessagesave
            FROM t_exmessage AS a;
            RAISE SQLSTATE '59999';
        ELSE
            -- If we get to this side of the IF then we are committing the data.  We are also recording any messages that we
            -- found while processing the batch
            INSERT INTO exmessage (exHistoryBatchId, exhistoryid, sysdictionarytableid, sysmessageid, messageseverity, description, isoverridden, syschangehistoryid)
            SELECT exHistoryBatchId,
                   exhistoryid,
                   sysdictionarytableid,
                   sysmessageid,
                   messageseverity,
                   description,
                   COALESCE(CASE WHEN messageseverity = 2 THEN p_OverrideWarningConditions
                                 END, FALSE),
                   p_sysChangeHistoryID
            FROM t_exmessage a
            WHERE NOT EXISTS(SELECT
                             FROM exmessage aa
                             WHERE aa.exHistoryBatchId = a.exHistoryBatchId
                               AND aa.sysdictionarytableid = a.sysdictionarytableid
                               AND COALESCE(aa.exhistoryid, 0) = COALESCE(a.exhistoryid, 0)
                               AND aa.description = a.description);

        END IF;
    END IF;

    -- We are now going to check the messages that got added to the batch to see if we need
    -- to override any based on messages that got added to the message override table
    UPDATE exmessage a
    SET isoverridden= TRUE
    WHERE EXISTS(SELECT
                 FROM exmessageoverride aa
                 WHERE aa.sysdictionarytableid = aa.sysdictionarytableid
                   AND aa.description = a.description
                   AND COALESCE(aa.sysmessageid, 0) = COALESCE(a.sysmessageid, 0)
        )
      AND a.isoverridden = FALSE
      AND a.exhistorybatchid IN (
                                    SELECT id
                                    FROM t_exhistorybatchimport);

    -- When an import runs it could import data that needs to be exported to another
    -- system.
    IF EXISTS(SELECT
              FROM exsubscriptiondetail a
              JOIN (
                       SELECT DISTINCT b.sysdictionarytableidappliesto, b.rowidappliesto
                       FROM t_exhistoryimport a
                       JOIN syschangehistoryrow b
                            ON a.syschangehistoryrowidexported = b.id) b
                   ON b.sysdictionarytableidappliesto = a.sysdictionarytableidsubscribedto
                       AND b.rowidappliesto = a.rowidsubscribedto)
    THEN
        CALL spexPackageExport(p_syschangehistoryid);
    END IF;

    -- If we do not have any messages (or all the messages are overridden
    -- then indicate the batch has been applied.
    IF NOT EXISTS(SELECT
                  FROM exmessage a
                  WHERE a.isoverridden = FALSE
                    AND a.exhistorybatchid IN (
                                                  SELECT id
                                                  FROM t_exhistorybatchimport))
    THEN
        UPDATE exHistoryBatch a
        SET AppliedDate = NOW()::TIMESTAMP
        FROM t_exhistoryimport b
        WHERE a.id = b.exhistorybatchid;
    END IF;
EXCEPTION
    WHEN SQLSTATE '59999' THEN
        -- Insert back any messages that exist other than the ones that have
        -- been overridden
        RAISE NOTICE ' message data %', l_exmessagesave;
        INSERT INTO exmessage (exHistoryBatchId, exhistoryid, sysdictionarytableid, sysmessageid, messageseverity, description, isoverridden, syschangehistoryid)
        SELECT exHistoryBatchId,
               exhistoryid,
               sysdictionarytableid,
               sysmessageid,
               messageseverity,
               description,
               COALESCE(CASE WHEN messageseverity = 2 THEN p_OverrideWarningConditions
                             END, FALSE),
               p_sysChangeHistoryID
        FROM JSON_POPULATE_RECORDSET(NULL::exmessage, l_exmessagesave) a
        WHERE NOT EXISTS(SELECT
                         FROM exmessage aa
                         WHERE aa.exHistoryBatchId = a.exHistoryBatchId
                           AND aa.sysdictionarytableid = a.sysdictionarytableid
                           AND COALESCE(aa.exhistoryid, 0) = COALESCE(a.exhistoryid, 0)
                           AND aa.description = a.description);
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
END
$$ LANGUAGE plpgsql;

