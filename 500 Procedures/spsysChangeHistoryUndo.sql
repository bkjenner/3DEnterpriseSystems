CREATE OR REPLACE PROCEDURE S0000V0000.spsysChangeHistoryUndo(p_sysChangeHistoryIDUndo BIGINT,
                                                              p_sysChangeHistoryIDNew BIGINT,
                                                              p_debug BOOLEAN DEFAULT FALSE)
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
This procedure allows an existing change history record to be undone.  That means all updates that were
performed as a part of a change history event will be reversed to their original state prior to the change occurring.

The procedure will verify there are no change history events that will need to be undone first.

Change history undo events are tracked that same as normal change history events


CHANGE LOG
20210916 Blair Kjenner	Initial Code

PARAMETERS
p_sysChangeHistoryIDUndo - ID of the sysChangeHistory record to be undone
p_sysChangeHistoryIDNew  - ID of the new sysChangeHistory record to be used for the undo changes
p_debug - DEFAULT FALSE - TRUE outputs the script without executing it. FALSE executes the script.

SAMPLE CALL
DO
$TEST$
    DECLARE
        l_sysChangeHistoryIDNew BIGINT;
        l_sysChangeHistoryIDUndo BIGINT;
    BEGIN
        INSERT INTO syschangehistory (crmcontactiduser, changedate, comments, rowstatus, sysdictionarytableidappliesto, Rowidappliesto, sysChangeHistoryIDUndo)
        SELECT a.crmcontactiduser, NOW()::TIMESTAMP, 'undo the last change recorded that is not an undo', 'a', a.sysdictionarytableidappliesto, a.Rowidappliesto, a.id
        FROM sysChangeHistory a
        LEFT JOIN sysChangeHistory b on b.sysChangeHistoryIDUndo=a.id
        WHERE a.sysChangeHistoryIDUndo is null
        AND b.id is null
        ORDER BY ID desc
        LIMIT 1
        RETURNING ID, sysChangeHistoryIDUndo INTO l_sysChangeHistoryIDNew, l_sysChangeHistoryIDUndo;

        call spsysChangeHistoryUndo(p_sysChangeHistoryIDUndo:=l_sysChangeHistoryIDUndo, p_sysChangeHistoryIDNew:=l_sysChangeHistoryIDNew);
    END
$TEST$
*/
DECLARE
    e_Context                 TEXT;
    e_Msg                     TEXT;
    e_State                   TEXT;
    l_ChangeHistoryIDUndo     BIGINT;
    l_ColumnList              VARCHAR;
    l_Error                   RECORD;
    l_SubsequentChangeMessage VARCHAR;
    l_SQL                     VARCHAR := '';
    l_TableSQL                VARCHAR;
    l_tRec                    RECORD;
    l_UpdateList              VARCHAR;
BEGIN

    SELECT sysChangeHistoryIdUndo
    INTO l_ChangeHistoryIDUndo
    FROM sysChangeHistory
    WHERE id = p_sysChangeHistoryIDUndo;

    IF l_ChangeHistoryIDUndo IS NOT NULL
    THEN
        -- Record has already been undone
        RAISE SQLSTATE '51058';
    END IF;

    -- Check if there was a change to the change History record that occurred After the transaction being undone. if so
    -- the subsequent change will need to be undone first.
    SELECT 'Table:' || c.name ||
           ' Row:' || fnsysGetTranslation(a.sysdictionarytableidappliesto, a.RowIDAppliesTo) ||
           ' Change History ID:' || a.syschangehistoryid::VARCHAR ||
           ' Change Date:' || b.changedate::VARCHAR
    INTO l_SubsequentChangeMessage
    FROM sysChangeHistoryRow a
    JOIN sysChangeHistory b
         ON b.id = a.sysChangeHistoryId
    JOIN sysDictionaryTable C
         ON C.id = a.sysdictionarytableidappliesto
    JOIN
         (
             SELECT DISTINCT aa.syschangehistoryid,
                             aa.sysdictionarytableidappliesto,
                             aa.RowIDAppliesTo
             FROM sysChangeHistoryRow aa
             WHERE aa.syschangehistoryid = p_sysChangeHistoryIDUndo) D
         ON D.sysdictionarytableidappliesto = a.sysdictionarytableidappliesto AND
            a.Rowidappliesto = D.RowIDAppliesTo
    WHERE b.sysChangeHistoryIdUndo IS NULL
      AND a.syschangehistoryid > d.syschangehistoryid
        --TODO I need to change update to a command like export
      AND ((OperationType = 'insert' AND olddata IS NULL AND newdata IS NOT NULL) OR
           (OperationType = 'update' AND olddata IS NOT NULL AND newdata IS NOT NULL) OR
           (OperationType = 'delete' AND olddata IS NOT NULL AND newdata IS NULL))
    ORDER BY a.syschangehistoryid
    LIMIT 1;

    IF l_SubsequentChangeMessage IS NOT NULL
    THEN
        --Record has change that must be undone prior to this change being undone
        RAISE SQLSTATE '51059' USING MESSAGE = l_SubsequentChangeMessage;
    END IF;

    IF NOT EXISTS(SELECT
                  FROM sysChangeHistoryRow
                      -- Only can undo insert, update and delete actions - not
                      -- fk insert or export
                  WHERE OperationType IN ('insert', 'update', 'delete')
                    AND syschangehistoryid = p_sysChangeHistoryIDUndo)
    THEN
        --Nothing to undo
        RAISE SQLSTATE '51077';
    END IF;

    FOR l_tRec IN SELECT cht.id                 chtID,
                         dt.name                dtName,
                         cht.Rowidappliesto     chtRowID,
                         cht.OperationType      chtOperationType,
                         cht.rowtemporalenddate chtRowTemporalEnddate,
                         cht.olddata,
                         cht.newdata
                  FROM sysChangeHistoryRow cht
                  JOIN sysDictionaryTable dt
                       ON dt.id = cht.sysdictionarytableidappliesto
                  WHERE OperationType in ('insert','update','delete')
                    AND cht.syschangehistoryid = p_sysChangeHistoryIDUndo
                  ORDER BY cht.id DESC
    LOOP

        -- Get the columns names
        SELECT STRING_AGG(column_name, ', '), STRING_AGG(column_name || '=b.' || column_name, ', ')
        INTO l_ColumnList, l_UpdateList
        FROM (
                 SELECT column_name
                 FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_NAME ILIKE l_tRec.dtName
                   AND TABLE_SCHEMA = CURRENT_SCHEMA()
                   AND column_name NOT IN
                       ('id', 'syschangehistoryid')
                 ORDER BY ordinal_position) AS a;

        IF l_tRec.chtOperationType = 'insert'
        THEN
            l_tableSQL := '
IF NOT EXISTS (SELECT FROM l_tablename a WHERE a.id=p_id l_temporalcondition)
THEN
    -- Record has already been deleted
    RAISE SQLSTATE ''51060'' USING MESSAGE = p_id;
END IF;

PERFORM fnsysChangeHistorySetParameters(p_actiontype:=''undo'',p_sysChangeHistoryIDForDelete:=p_syschangehistoryidnew);
DELETE FROM l_tablename AS a WHERE a.id=p_id l_temporalcondition;';
        END IF;

        IF l_tRec.chtOperationType = 'delete'
        THEN
            l_tableSQL := '

IF EXISTS (SELECT FROM l_tablename a WHERE a.id=p_id l_temporalcondition)
THEN
    -- Record has already been inserted
    RAISE SQLSTATE ''51061'' USING MESSAGE = p_id;
END IF;

l_OldData := cast(''[p_olddata]'' as json);

INSERT INTO l_tablename (id, l_columnlist, sysChangeHistoryId)
SELECT id, l_columnlist, p_syschangehistoryidnew FROM json_populate_recordset(null::l_tablename,l_olddata);';
        END IF;

        IF l_tRec.chtOperationType = 'update'
            AND l_tRec.olddata IS NOT NULL
            AND l_tRec.newdata IS NOT NULL
        THEN
            l_tableSQL := '

l_OldData := cast(''[p_olddata]'' as json);
l_NewData := cast(''[p_newdata]'' as json);

IF (SELECT count(0) CompareCount FROM
	 (SELECT l_columnlist
	  FROM l_tablename a WHERE a.id=p_id l_temporalcondition
	  UNION
	  SELECT l_columnlist
	  FROM json_populate_recordset(null::l_tablename,l_newdata)) a) = 2
THEN
    -- Original record has been modified
    RAISE SQLSTATE ''51062'' USING MESSAGE = p_id;
END IF;

Update l_tablename a set l_updatelist, syschangehistoryid=p_syschangehistoryidnew
FROM (SELECT * FROM json_populate_recordset(null::l_tablename,l_olddata) aa) b
WHERE a.id=p_id l_temporalcondition;';
        END IF;

        l_TableSQL :=
                REPLACE(
                        REPLACE(
                                REPLACE(
                                        REPLACE(
                                                REPLACE(
                                                        REPLACE(
                                                                REPLACE(
                                                                        REPLACE(
                                                                                l_TableSQL
                                                                            , 'l_tablename', l_trec.dtname)
                                                                    , 'l_columnlist', l_columnlist)
                                                            , 'l_updatelist', l_updatelist)
                                                    , 'p_id', l_trec.chtrowid::VARCHAR)
                                            , 'p_olddata', COALESCE(fixquote(l_trec.olddata::VARCHAR), ''))
                                    , 'p_newdata', COALESCE(fixquote(l_trec.newdata::VARCHAR), ''))
                            , 'l_temporalcondition', COALESCE(
                                                ' AND a.temporalenddate=''' || l_trec.chtrowtemporalenddate::VARCHAR ||
                                                '''::date', ''))
                    , 'p_syschangehistoryidnew', p_syschangehistoryidnew::VARCHAR);
        IF p_debug = TRUE
        THEN
            RAISE NOTICE 'l_tablename % p_id % p_temporalenddate % p_syschangehistoryidnew %',l_trec.dtname, l_trec.chtrowid, l_trec.chtrowtemporalenddate, p_syschangehistoryidnew;
        END IF;
        l_sql := l_sql || l_TableSQL;
    END LOOP;

    l_SQL := '
DO
$TEMP$
DECLARE
    e_Context            TEXT;
    e_Msg                TEXT;
    e_State              TEXT;
    l_Error              RECORD;
    l_OldData            json;
    l_newdata            json;
BEGIN
    ' || l_SQL || '
    -- Drop parameter table
    PERFORM fnsysChangeHistorySetParameters();
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_state = RETURNED_SQLSTATE,
            e_msg = MESSAGE_TEXT,
            e_context = PG_EXCEPTION_CONTEXT;
        l_error := fnsysError(e_state, e_msg, e_context);
        RAISE NOTICE '' % '', l_Error.Message;
END
$TEMP$
LANGUAGE plpgsql;';

    IF p_DEBUG = TRUE
    THEN
        RAISE NOTICE '%', l_sql;
    ELSE
        EXECUTE (l_SQL);
        UPDATE sysChangeHistory SET sysChangeHistoryIdUndo=p_sysChangeHistoryIDNew WHERE id = p_sysChangeHistoryIDUndo;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_state = RETURNED_SQLSTATE,
            e_msg = MESSAGE_TEXT,
            e_context = PG_EXCEPTION_CONTEXT;
        l_error := fnsysError(e_state, e_msg, e_context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', 'Unable to undo change history due to error - ' || e_msg;
        ELSE
            RAISE NOTICE '%', l_Error.Message ;
        END IF;

END;
$$ LANGUAGE plpgsql
