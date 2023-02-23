DROP PROCEDURE IF EXISTS S0000V0000.spsysChangeHistoryRefreshTriggers;
CREATE OR REPLACE PROCEDURE S0000V0000.spsysChangeHistoryRefreshTriggers(p_debug BOOLEAN DEFAULT FALSE)
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

This procedure recreates the triggers for change history according to the data dictionary. Anytime parameters are changed
that affect change history, this procedure should be run.  Examples of such changes are:
- turning change history on for a command
- changing the scope for change history for a table (e.g. insert, update, delete)
- changing the columns that are tracked

20210325    Blair Kjenner   Initial Code

call spsysChangeHistoryRefreshTriggers(p_debug:=false)
*/
DECLARE
    l_SQL                  VARCHAR := '';
    l_TableColumns         VARCHAR;
    l_AuditFieldComparison VARCHAR;
    l_dRec                 RECORD;
BEGIN

    CALL spsysChangeHistoryDisableTriggers();

    FOR l_dRec IN SELECT DT.isTableTemporal isTemporal,
                         DT.id              sysDictionaryTableIDAppliesTo,
                         DT.Name            TableName,
                         df.Found
                             AS             HasRecordStatus,
                         dt.ChangeHistoryScope
                  FROM sysDictionaryTable DT
                  LEFT JOIN LATERAL (SELECT CASE WHEN EXISTS(SELECT TRUE
                                                             FROM sysDictionaryColumn df
                                                             WHERE sysDictionaryTableID = dt.id
                                                               AND LOWER(df.name) = 'rowstatus'
                                                               AND df.rowstatus = 'a')
                                                     THEN TRUE
                                                 ELSE FALSE
                                                 END Found) df
                            ON TRUE
                  WHERE ChangeHistoryScope IS NOT NULL
                    AND rowstatus = 'a'
--                and dt.name='HBTransactions'

    LOOP

        SELECT STRING_AGG('a.' || a.name, ', '),
               STRING_AGG(CASE WHEN ((ischangehistoryused = TRUE OR
                                      a.name IN ('temporalstartdate', 'temporalenddate', 'rowstatus'))
                   AND a.name NOT IN ('syschangehistoryid', 'id'))
                                   THEN a.name || ', '
                               ELSE ''
                               END, '')
        INTO l_TableColumns, l_AuditFieldComparison
        FROM (
                 SELECT LOWER(name)     AS name,
                        LOWER(datatype) AS datatype,
                        ischangehistoryused,
                        datalength,
                        decimals,
                        sysdictionarytableid
                 FROM sysDictionaryColumn
                 WHERE rowstatus = 'a'
                 ORDER BY columnsequence) AS a
        WHERE a.sysDictionaryTableID = l_dRec.sysDictionaryTableIDAppliesTo;
        -- AND a.DataType NOT IN ('bytea');

        l_AuditFieldComparison := LEFT(l_AuditFieldComparison, LENGTH(l_AuditFieldComparison) - 2);

--Create the trigger, inserting INTO tracking table
        l_sql := '
' || CASE WHEN LOWER(l_dRec.ChangeHistoryScope) LIKE '%insert%' THEN '
CREATE FUNCTION fnCH' || l_dRec.TableName || '_InsCH()
RETURNS TRIGGER AS
$TEMP$

DECLARE l_count int;
        l_max int;
		l_error record;
		l_isMaxRecordsIgnored boolean;
		l_actiontype varchar;
BEGIN

-- We need to control the maximum number of changehistoryrow records created otherwise it will kill performance
select count(*) into l_count from NewRecords;
l_max := fnsysGlobal(''sys-maxChangeHistoryInBatch'')::int;
l_max := case when coalesce(l_max,0) = 0 then 100 else l_max end;

-- t_sysChangeHistoryParm allows us to commmunicate actions between a procedure that makes updates and this one
-- e.g. spsysTemporalDataUpdate
if fnIfTableExists(''t_sysChangeHistoryParm'')
then
    select actiontype into l_actiontype from t_sysChangeHistoryParm;
end if;

if coalesce(l_actiontype,'''') = ''import''
then
    return new;
end if;

if l_count > l_max
then
    -- The max records can be overridden on the sysChangeHistory record
    select IsMaxRecordsIgnored into l_IsMaxRecordsIgnored from sysChangeHistory where id=(select sysChangeHistoryID
    from NewRecords limit 1);
    l_max:=case when coalesce(L_IsMaxRecordsIgnored,false)=true then l_count else l_max end;
end if;

IF l_count <= l_max
THEN
    insert into sysChangeHistoryRow (sysDictionaryTableIDAppliesTo, RowIDAppliesTo, RowTemporalEndDate, ActionType, sysChangeHistoryID, OperationType, NewData, OldData, ChangeDate, IsProcessed)
    select ' || CAST(l_dRec.sysDictionaryTableIDAppliesTo AS VARCHAR) || '
    , a.id
    , ' || CASE WHEN l_dRec.isTemporal = TRUE THEN 'a.TemporalEndDate'
                ELSE 'null'
                END || '
    , coalesce(l_actiontype,''add'')
    , a.sysChangeHistoryID
    , ''insert''
    , (select ROW_TO_JSON(b))::jsonb
    , null::jsonb
    , now()
    , FALSE
    FROM NewRecords a
    JOIN LATERAL (SELECT ' || l_TableColumns || ') b ON TRUE
    -- A change history row record will only be created if a ChangeHistory record exists
    join sysChangeHistory c on c.id=a.sysChangeHistoryID;
ELSE
    l_Error := fnsysError(51052::text, '''', ''Table: ' || l_dRec.TableName || ', Records Inserted: '' || l_count::varchar || '' Max Records Allowed: '' || l_max::varchar);
END IF;

RETURN NEW;
End;
$TEMP$ LANGUAGE plpgsql;

CREATE TRIGGER trCH' || l_dRec.TableName || '_insCH
    AFTER INSERT
    ON ' || l_dRec.TableName || '
    REFERENCING NEW TABLE AS NewRecords
    FOR EACH STATEMENT
    EXECUTE PROCEDURE fnCH' || l_dRec.TableName || '_insCH();
'
          ELSE '
'
          END ||
                 CASE WHEN LOWER(l_dRec.ChangeHistoryScope) LIKE '%update%' THEN '
CREATE FUNCTION fnCH' || l_dRec.TableName || '_updCH()
RETURNS TRIGGER AS
$TEMP$
DECLARE l_count int;
        l_max int;
		l_error record;
		l_isMaxRecordsIgnored boolean;
		l_actiontype varchar;
BEGIN

-- We need to control the maximum number of changehistoryrow records created otherwise it will kill performance
select count(*) into l_count from NewRecords;
l_max := fnsysGlobal(''sys-maxChangeHistoryInBatch'')::int;
l_max := case when coalesce(l_max,0) = 0 then 100 else l_max end;

if l_count > l_max
then
    -- The max records can be overridden on the sysChangeHistory record
    select IsMaxRecordsIgnored into l_IsMaxRecordsIgnored from sysChangeHistory where id=(select sysChangeHistoryID
    from NewRecords limit 1);
    l_max:=case when coalesce(L_IsMaxRecordsIgnored,false)=true then l_count else l_max end;
end if;

-- t_sysChangeHistoryParm allows us to commmunicate actions between a procedure that makes updates and this one
-- e.g. spsysTemporalDataUpdate
if fnIfTableExists(''t_sysChangeHistoryParm'')
then
    select actiontype into l_actiontype from t_sysChangeHistoryParm;
end if;

-- When we are importing records we already ready have a changehistory record to work from
if coalesce(l_actiontype,'''') = ''import''
then
    return new;
end if;

IF l_count <= l_max
THEN
    insert into sysChangeHistoryRow (sysDictionaryTableIDAppliesTo, RowIDAppliesTo, RowTemporalEndDate, ActionType, sysChangeHistoryID, OperationType, NewData, OldData, ChangeDate, IsProcessed)
    -- If there are any differences between old and new the record count will be 2 for the id (as a result of the union)
    with b as (
        select id, count(0) CompareCount  from
	 (select id, ' || l_AuditFieldComparison || '
	  from NewRecords
	  union
	  select id, ' || l_AuditFieldComparison || '
	  from OldRecords)
	 a
	 group by id)
    select distinct ' || CAST(l_dRec.sysDictionaryTableIDAppliesTo AS VARCHAR) || '
    , a.id
    , ' || CASE WHEN l_dRec.isTemporal = TRUE THEN 'a.TemporalEndDate'
                ELSE 'null::date'
                END || '
    , coalesce(l_actiontype,' || CASE WHEN l_dRec.hasrecordstatus THEN 'case when a.rowstatus != c.rowstatus and a.rowStatus = ''d'' then ''deactivate''
    when a.rowstatus != c.rowstatus and a.rowStatus = ''a'' then ''reactivate''
    else ''edit'' end '
                                      ELSE '''edit'''
                                      END || ')
    , a.sysChangeHistoryID
    , ''update''
    , (select ROW_TO_JSON(e))::jsonb
    , (select ROW_TO_JSON(f))::jsonb
    , now()
    , FALSE
    from newrecords a
	join b on b.id=a.id
	join oldrecords c on c.id=a.id
    -- a change history row record will only be created if a changehistory record exists
    join syschangehistory d on d.id=a.syschangehistoryid
    join lateral (select ' || l_tablecolumns || ') e on true
    join lateral (select ' || REPLACE(l_tablecolumns, 'a.', 'c.') || ') f on true
    where b.comparecount=2;
ELSE
    l_Error := fnsysError(51052::text, '''', ''Table: ' || l_dRec.TableName || ', Records Updated: '' || l_count::varchar || '' Max Records Allowed: '' || l_max::varchar);
END IF;

RETURN NEW;
End;
$TEMP$ LANGUAGE plpgsql;

CREATE TRIGGER trCH' || l_dRec.TableName || '_updCH
    AFTER UPDATE
    ON ' || l_dRec.TableName || '
    REFERENCING OLD TABLE AS OldRecords NEW TABLE AS NewRecords
    FOR EACH STATEMENT
    EXECUTE PROCEDURE fnCH' || l_dRec.TableName || '_updCH();
'
                      END ||
                 CASE WHEN LOWER(l_dRec.ChangeHistoryScope) LIKE '%delete%' THEN '
CREATE FUNCTION fnCH' || l_dRec.TableName || '_DelCH()
RETURNS TRIGGER AS
$TEMP$
DECLARE l_count int;
        l_max int;
		l_error record;
		l_isMaxRecordsIgnored boolean;
		l_actiontype varchar;
		l_sysChangeHistoryIdForDelete BIGINT;
BEGIN

-- We need to control the maximum number of changehistoryrow records created otherwise it will kill performance
select count(*) into l_count from OldRecords;
l_max := fnsysGlobal(''sys-maxChangeHistoryInBatch'')::int;
l_max := case when coalesce(l_max,0) = 0 then 100 else l_max end;

if l_count > l_max
then
    -- The max records can be overridden on the sysChangeHistory record
    select IsMaxRecordsIgnored into l_IsMaxRecordsIgnored from sysChangeHistory where id=(select sysChangeHistoryID
    from OldRecords limit 1);
    l_max:=case when coalesce(l_IsMaxRecordsIgnored,false)=true then l_count else l_max end;
end if;

-- t_sysChangeHistoryParm allows us to commmunicate actions between a procedure that makes updates and this one
-- e.g. spsysTemporalDataUpdate
if fnIfTableExists(''t_sysChangeHistoryParm'')
then
    select actiontype, sysChangeHistoryIDForDelete into l_actiontype, l_sysChangeHistoryIdForDelete from t_sysChangeHistoryParm;
end if;

if coalesce(l_actiontype,'''') = ''import''
then
    return new;
end if;

IF l_count <= l_max
THEN
    insert into sysChangeHistoryRow (sysDictionaryTableIDAppliesTo, RowIDAppliesTo, RowTemporalEndDate, ActionType, sysChangeHistoryID, OperationType, NewData, OldData, ChangeDate, IsProcessed)
    select ' || CAST(l_dRec.sysDictionaryTableIDAppliesTo AS VARCHAR) || '
    , a.id
    , ' || CASE WHEN l_dRec.isTemporal = TRUE THEN 'a.TemporalEndDate'
                ELSE 'null'
                END || '
    , coalesce(l_actiontype,''delete'')
    , l_sysChangeHistoryIDForDelete
    , ''delete''
    , null::jsonb
    , (select ROW_TO_JSON(b))::JSONB
    , now()
    , FALSE
    from oldrecords a
    join lateral (select ' || l_tablecolumns || ') b on true
    -- a change history row record will only be created if a changehistory record exists
    -- note: it must be passed via the t_sysChangeHistoryParm table
    join syschangehistory c on c.id=l_syschangehistoryidfordelete;
ELSE
    l_Error := fnsysError(51052::text, '''', ''Table: ' || l_dRec.TableName || ', Records Deleted: '' || l_count::varchar || '' Max Records Allowed: '' || l_max::varchar);
END IF;

RETURN NEW;
End;
$TEMP$ LANGUAGE plpgsql;

CREATE TRIGGER trCH' || l_dRec.TableName || '_delCH
    AFTER DELETE
    ON ' || l_dRec.TableName || '
    REFERENCING OLD TABLE AS OldRecords
    FOR EACH STATEMENT
EXECUTE PROCEDURE fnCH' || l_dRec.TableName || '_delCH();
'
                      ELSE ''
                      END;

        IF p_debug = TRUE
        THEN
            RAISE NOTICE '%', l_SQL;
        ELSE
            EXECUTE l_sql;
        END IF;

    END LOOP;

END ;
$$ LANGUAGE plpgsql
