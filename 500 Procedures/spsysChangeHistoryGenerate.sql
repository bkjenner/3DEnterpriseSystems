CREATE OR REPLACE PROCEDURE S0000V0000.spsysChangeHistoryGenerate()
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

This procedure takes sysChangeHistoryRow records that were generated as a result of the change history triggers
and generates the Change History Column data.  The Change History Column data provides a before and after image
of any columns that changes.  For foreign key refererences, the procedure will translate the foreign key
based on the translation for the table that is specified in the data dictionary.  For example, if the
crmContactID change from 123 to 321, the procedure would look up the contact name for 123 and 321 and capture
the change.  This information is helpful to users that need to see a user friendly version of changes that
occur.

This procedure also creates user friendly versions of temporal changes.  For example, Rate changed from .05 to .06
effective Jan 1.

20210913 Blair Kjenner	Initial Code

call spsyschangehistorygenerate()

*/
DECLARE
    l_RawDataAfter               VARCHAR;
    l_TranslatedDataAfter        VARCHAR;
    l_RawDataBefore              VARCHAR;
    l_TranslatedDataBefore       VARCHAR;
    l_ChangeCount                INT;
    l_dRec                       RECORD;
    l_cRec                       RECORD;
    l_sql                        VARCHAR;
    l_string                     VARCHAR;
    l_sysDictionaryTableIDBefore BIGINT;
    l_sysDictionaryTableIDAfter  BIGINT;
BEGIN
    /*
    DELETE FROM sysChangeHistoryColumn;
    PERFORM SETVAL('sysChangeHistoryColumn_id_seq', 1);
    UPDATE sysChangeHistoryRow SET ISProcessed= FALSE;
    */
    -- A copy of all the records to process is taken at the beginning so we dont
    -- get unexpected results if new records are added while we are processing
    DROP TABLE IF EXISTS t_sysChangeHistoryRowsToProcess;
    CREATE TEMP TABLE t_sysChangeHistoryRowsToProcess AS
    SELECT b.isTableTemporal               AS isTableTemporal,
           a.sysDictionaryTableIDAppliesTo AS sysDictionaryTableIDAppliesTo,
           a.ActionType                    AS ActionType,
           a.RowTemporalEndDate            AS RowTemporalEndDate,
           a.sysChangeHistoryID            AS sysChangeHistoryID,
           b.Name                          AS TableName,
           a.ID                            AS ID,
           a.RowIDAppliesTo                AS RowIDAppliesTo,
           a.NewData                       AS NewData,
           a.OldData                       AS OldData
    FROM sysChangeHistoryRow a
    JOIN sysDictionaryTable b
         ON b.id = a.sysDictionaryTableIDAppliesTo
    WHERE a.isprocessed = FALSE;

    FOR l_dRec IN
        WITH b AS (
                      SELECT sysDictionaryTableIDAppliesTo,
                             RowIDAppliesTO,
                             sysChangeHistoryID,
                             MAX(id) AS    MaxID,
                             -- This looks at old data where the change history id in the before image
                             -- it not equal to the current change history id
                             MIN(CASE WHEN CAST(olddata ->> 'syschangehistoryid' AS BIGINT) != sysChangeHistoryID
                                          THEN ID
                                      END) OldDataID,
                             -- TODO Retest generate with the temporal change history tests to make sure the correct results are retrieved
                             -- Change MIN to MAX because a new segment was inserted causing the old segment
                             -- to be end dated.  As a result, it picked up the new data on the ended
                             -- old segment which was the original data.
                             MAX(CASE WHEN CAST(newdata ->> 'temporalenddate' AS DATE) = rowtemporalenddate
                                          THEN ID
                                      END) NewDataID
                      FROM sysChangeHistoryRow
                      WHERE id IN (
                                      SELECT id
                                      FROM t_sysChangeHistoryRowsToProcess)
                      GROUP BY sysDictionaryTableIDAppliesTo,
                               RowIDAppliesTO,
                               sysChangeHistoryID)
        SELECT a.*,
               b.maxid     AS MaxID,
               b.olddataid AS OldDataID,
               b.newdataid AS NewDataID
        FROM t_sysChangeHistoryRowsToProcess a
        JOIN b
             ON b.sysDictionaryTableIDAppliesTo = a.sysDictionaryTableIDAppliesTo AND
                b.RowIDAppliesTO = a.RowIDAppliesTO AND
                b.sysChangeHistoryID = a.sysChangeHistoryID
        ORDER BY a.sysChangeHistoryID, a.id
    LOOP
        IF l_dRec.istabletemporal
        THEN
            -- If we have temporal data there can be many changes to the segments that the users are not
            -- interested in seeing (like changing start/end dates, adding and removing segments).
            -- All the users really need to see is the before and after image of the change
            -- To get the before image we pick up the OLDData where the change history id is not equal to
            -- the current change history id.  That will always be the before image of the record.
            -- The appropriate after image of the record is the one that has the same matching enddate
            -- in newdata as the end date on the current record. For consistency, change history column
            -- records are always connected to the last change history row record
            IF l_dRec.Maxid = l_dRec.id
            THEN
                IF l_dRec.id != l_dRec.olddataid
                THEN
                    SELECT a.oldData
                    INTO l_dRec.OldData
                    FROM sysChangeHistoryRow a
                    WHERE a.id = l_dRec.olddataid;
                END IF;
                IF l_dRec.id != l_dRec.newdataid
                THEN
                    SELECT a.newdata
                    INTO l_dRec.newdata
                    FROM sysChangeHistoryRow a
                    WHERE a.id = l_dRec.newdataid;
                END IF;
            ELSE
                l_dRec.olddata := NULL;
                l_dRec.newdata := NULL;
            END IF;
        END IF;

        -- On an insert of a change history record, the row id and table id may be null on the change history record.
        -- If so, update it.
        UPDATE syschangehistory a
        SET rowidappliesto=b.rowidappliesto, sysdictionarytableidappliesto=b.sysDictionaryTableIDAppliesTo
        FROM t_sysChangeHistoryRowsToProcess b
        WHERE a.rowidappliesto IS NULL
          AND a.id = b.sysChangeHistoryID;

        IF l_dRec.OldData IS NOT NULL OR l_dRec.NewData IS NOT NULL
        THEN
            FOR l_cRec IN
                SELECT LOWER(df.Name)                 AS Name,
                       df.id                          AS sysDictionaryColumnId,
                       df.sysDictionaryTableIDForeign AS sysDictionaryTableIDForeign,
                       df.IsChangeHistoryUsed         AS IsChangeHistoryUsed,
                       LOWER(df.purpose)              AS Purpose
                FROM sysdictionarycolumn df
                WHERE df.sysDictionaryTableID = l_dRec.sysDictionaryTableIDAppliesTo
                  AND LOWER(df.datatype) NOT IN ('bytea', 'json', 'jsonb')
                  AND LOWER(df.Name) NOT IN ('id', 'temporalstartdate', 'temporalenddate')
                    -- Dont include the multilink column the links to dictionarytableid
                  AND NOT (LOWER(df.Purpose) = 'multilink' AND LOWER(df.name) LIKE 'sysdictionarytableid%')
                  --AND df.IsChangeHistoryUsed = TRUE
                ORDER BY df.id
            LOOP
                l_ChangeCount := 0;

                l_RawDataBefore := ''; l_RawDataAfter := ''; l_TranslatedDataBefore := ''; l_TranslatedDataAfter := '';

                IF l_dRec.OldData IS NOT NULL
                THEN
                    SELECT l_dRec.OldData::json -> l_cRec.Name INTO l_RawDataBefore;
                END IF;

                IF l_dRec.NewData IS NOT NULL
                THEN
                    SELECT l_dRec.NewData::json -> l_cRec.Name INTO l_RawDataAfter;
                END IF;

                l_RawDataBefore := CASE WHEN l_RawDataBefore != 'null' THEN l_RawDataBefore
                                        ELSE ''
                                        END;
                l_RawDataAfter := CASE WHEN l_RawDataAfter != 'null' THEN l_RawDataAfter
                                       ELSE ''
                                       END;

                --	raise notice 'Var % Old % New %', l_crec.name, l_RawDataBefore, l_RawDataAfter;

                IF LOWER(l_dRec.ActionType) LIKE 'delete%' OR LOWER(l_dRec.ActionType) LIKE 'deactivate%'
                THEN
                    l_RawDataAfter := '';
                END IF;

                IF LOWER(l_dRec.ActionType) LIKE 'add%' OR LOWER(l_dRec.ActionType) LIKE 'reactivate%'
                THEN
                    l_RawDataBefore := '';
                END IF;

                l_sysDictionaryTableIDBefore := NULL;
                l_sysDictionaryTableIDAfter := NULL;
                IF l_cRec.purpose IN ('multilink', 'foreign key')
                THEN
                    IF l_RawDataBefore != ''
                    THEN
                        IF l_cRec.purpose = 'multilink'
                        THEN
                            l_string := REPLACE(l_cRec.Name, 'rowid', 'sysdictionarytableid');
                            l_sysDictionaryTableIDBefore := CAST(l_dRec.OldData::json ->> l_string
                                AS BIGINT);
                        ELSE
                            l_sysDictionaryTableIDBefore := l_cRec.sysDictionaryTableIDForeign;
                        END IF;
                        -- First try to get the translation from the cache.
                        SELECT translation
                        INTO l_TranslatedDataBefore
                        FROM sysforeignkeycache s
                        WHERE s.sysdictionarytableid = l_sysDictionaryTableIDBefore
                          AND s.rowid = l_RawDataBefore::BIGINT;
                        -- If not found, go get it from fnsysGetTranslation
                        IF l_TranslatedDataBefore IS NULL
                        THEN
                            l_TranslatedDataBefore :=
                                    fnsysGetTranslation(l_sysDictionaryTableIDBefore,
                                                        l_RawDataBefore::BIGINT);
                        END IF;
                    END IF;
                    IF l_RawDataAfter != ''
                    THEN
                        -- Still need to get the foreign key table because the dictionary table
                        -- plus the row could have changed
                        IF l_cRec.purpose = 'multilink'
                        THEN
                            l_string := REPLACE(l_cRec.Name, 'rowid', 'sysdictionarytableid');
                            l_sysDictionaryTableIDAfter := CAST(l_dRec.OldData::json ->> l_string
                                AS BIGINT);
                        ELSE
                            l_sysDictionaryTableIDAfter := l_cRec.sysDictionaryTableIDForeign;
                        END IF;
                        -- First try to get the translation from the cache.
                        SELECT translation
                        INTO l_TranslatedDataAfter
                        FROM sysforeignkeycache s
                        WHERE s.sysdictionarytableid = l_sysDictionaryTableIDAfter
                          AND s.rowid = l_RawDataAfter::BIGINT;

                        -- If not found, go get it from fnsysGetTranslation
                        IF l_TranslatedDataAfter IS NULL
                        THEN
                            l_TranslatedDataAfter :=
                                    fnsysGetTranslation(l_sysDictionaryTableIDAfter,
                                                        l_RawDataAfter::BIGINT);
                        END IF;
                    END IF;
                ELSE
                    l_TranslatedDataBefore := l_RawDataBefore;
                    l_TranslatedDataAfter := l_RawDataAfter;
                END IF;

                IF REPLACE(l_RawDataBefore, ' ', '') !=
                   REPLACE(l_RawDataAfter, ' ', '') OR
                   REPLACE(l_TranslatedDataBefore, ' ', '') !=
                   REPLACE(l_TranslatedDataAfter, ' ', '')
                THEN
                    l_ChangeCount := l_ChangeCount + 1;
                    INSERT INTO sysChangeHistoryColumn(
                        sysChangeHistoryRowID,
                        sysDictionaryColumnID,
                        sysdictionarytableidbefore,
                        sysdictionarytableidafter,
                        TranslatedDataBefore,
                        TranslatedDataAfter,
                        RawDataBefore,
                        RawDataAfter)
                    VALUES (
                               l_drec.id,
                               l_cRec.sysDictionaryColumnID,
                               l_sysDictionaryTableIDBefore,
                               l_sysDictionaryTableIDAfter,
                               l_TranslatedDataBefore,
                               l_TranslatedDataAfter,
                               l_RawDataBefore,
                               l_RawDataAfter);
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    UPDATE sysChangeHistoryRow
    SET IsProcessed= TRUE
    WHERE ID IN (
                    SELECT ID
                    FROM t_sysChangeHistoryRowsToProcess);

    /*
    -- Reset the sysCHangeHistoryid's on the affected rows.  This is so updates are not made
    -- to the records causing unintentional change history records to be generated

    SELECT STRING_AGG('update ' || tablename || ' set sysChangeHistoryID = null where id =' ||
                      RowIDAppliesTo::VARCHAR || ';
', '')
    INTO l_sql
    FROM t_sysChangeHistoryRowsToProcess;

    IF l_sql IS NOT NULL
    THEN
        EXECUTE (l_sql);
    END IF;
*/
    DROP TABLE IF EXISTS t_sysChangeHistoryRowsToProcess;

END;
$$ LANGUAGE plpgsql

/*
select e.id CHId, d.id CHRId, c.name as TableName, d.Rowidappliesto rowid, a.id CHCID,b.purpose, e.comments, d.actiontype, d.OperationType, b.name as ColumnName, a.RawDataBefore, a.TranslatedDataBefore, a.RawDataAfter, a.TranslatedDataAfter from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;
*/
