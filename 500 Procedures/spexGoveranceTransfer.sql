CREATE OR REPLACE PROCEDURE S0000V0000.spexGoveranceTransfer(p_exRecordGroupID BIGINT, p_id BIGINT, p_exSystemID INT,
                                                             p_sysChangeHistoryID BIGINT DEFAULT NULL,
                                                             p_Debug BOOLEAN DEFAULT FALSE)
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
This procedure transfers governance for a record to another system

CHANGE LOG
20221213 Blair Kjenner	Initial Code

PARAMETERS
p_exRecordGroupID - Record Group being transferred
p_id              - ID of record being transferred
p_exSystemID      - New govenor system
p_sysChangeHistoryID - ChangeHistoryID connected to all changes
p_Debug           - DEFAULT false

SAMPLE CALL

call spexGoveranceTransfer();

*/
DECLARE
    e_Context                TEXT;
    e_Msg                    TEXT;
    e_State                  TEXT;
    l_Error                  RECORD;
    l_Rec                    RECORD;
    l_SQL                    VARCHAR;
    l_SQLTemp                VARCHAR;
    l_max                    INT;
    l_i                      INT;
    l_sysdictionarytableid   BIGINT;
    l_exGovernanceID         BIGINT;
    l_exRecordGroupGovernor  BIGINT;
    l_exsubscriberidgovernor BIGINT;
BEGIN

    IF not exists (select from vwexsystemaws where id=p_exsystemid)
    THEN
        --System id does not exist
        RAISE SQLSTATE '51079';
    END IF;

    IF not exists (select from exrecordgroup where id=p_exrecordgroupid)
    THEN
        --Record group id does not exist
        RAISE SQLSTATE '51080';
    END IF;

    IF not exists (select from exrecordgroup where id=p_exrecordgroupid and exrecordgroupidparent is null)
    THEN
        --Record group does not reference a parent record
        RAISE SQLSTATE '51081';
    END IF;

    select sysdictionarytableid into l_sysdictionarytableid
    from exrecordgroup where id=p_exrecordgroupid;

    if fnexIsGovernor(l_sysdictionarytableid, p_id) = false
    then
        --Governance is not held by the current system
        RAISE SQLSTATE '51083';
    END IF;

    IF p_exsystemid = fnsysCurrentSystemID()
    THEN
        --Cannot transfer it to current system
        RAISE SQLSTATE '51084';
    END IF;

    p_sysChangeHistoryID := COALESCE(p_sysChangeHistoryID, fnsysChangeHistoryCreate('Governance transfer as of ' || fntime()));

    INSERT INTO exsystem (id, exsubnetserverid, name, schemaname, productionversion, testversion, subscriptionkey, rowstatus, syschangehistoryid)
    SELECT id,
           exsubnetserverid,
           name,
           schemaname,
           productionversion,
           testversion,
           subscriptionkey,
           rowstatus,
           p_syschangehistoryid
    FROM vwexsystemaws
    WHERE id = p_exSystemID
      AND id NOT IN (
                        SELECT id
                        FROM exsystem);

    INSERT INTO exgovernance (exsystemid, exrecordgroupid, rowidsubscribedto, transferdate, syschangehistoryid)
    SELECT p_exsystemid      exsystemid,
           p_exrecordgroupid exrecordgroupid,
           p_id              rowidsubscribedto,
           NOW()::TIMESTAMP  transferdate,
           p_syschangehistoryid
    RETURNING id
        INTO l_exGovernanceID;

    DROP TABLE IF EXISTS t_exRecordGroupHierarchy;

    -- When a Governance references a record group it only references the id of the parent.
    -- This query walks through the record hierarchy and creates a record for each node in the
    -- tree with the Root Parent ID and the id of the record.  For example, if we had a parent id
    -- of 100 for a record group called 'Member Data', then we would we a parent id of 100
    -- and the id of every record within the group (e.g. Member Address) would be in the output table.
    -- We also collect other data that will be useful when we process the Governance.
    CREATE TEMP TABLE t_exRecordGroupHierarchy
    AS
    WITH RECURSIVE Paths(ID,
                         ParentID,
                         sysDictionaryTableID,
                         sysDictionaryColumnID,
                         whereclause,
                         drilldowncolumn,
                         exRecordGroupIDParent,
                         sysDictionaryTableIDParent,
                         TreeLevel)
                       AS (
                              -- Start with records with no parents and recurse through child records
                              SELECT ID                              ID,
                                     ID                              ParentID,
                                     sysDictionaryTableID,
                                     sysDictionaryColumnID,
                                     whereclause,
                                     COALESCE(drilldowncolumn, 'id') drilldowncolumn,
                                     exRecordGroupIdParent,
                                     CAST(NULL AS BIGINT)            sysDictionaryTableIDParent,
                                     1                               TreeLevel
                              FROM exRecordGroup
                              WHERE id = p_exRecordGroupID
                              UNION ALL
                              SELECT c.ID                              ID,
                                     p.ParentID                        ParentID,
                                     c.sysDictionaryTableID,
                                     c.sysDictionaryColumnID,
                                     c.whereclause,
                                     COALESCE(c.drilldowncolumn, 'id') drilldowncolumn,
                                     c.exRecordGroupIdParent,
                                     p.sysDictionaryTableID            sysDictionaryTableIDParent,
                                     p.TreeLevel + 1
                              FROM exRecordGroup AS c
                              INNER JOIN Paths AS p
                                         ON p.ID = c.exRecordGroupIDParent)
    SELECT ID,
           ParentID,
           sysDictionaryTableID,
           sysDictionaryColumnID,
           whereclause,
           drilldowncolumn,
           exRecordGroupIDParent,
           sysDictionaryTableIDParent,
           TreeLevel
    FROM Paths a
    ORDER BY ParentID, ID;

    CREATE INDEX t_exRecordGroupHierarchy_index
        ON t_exRecordGroupHierarchy (ParentID, ID);

    -- In this phase of the processing we are going to build the t_Governancedetail table
    -- It contains one record for every row of data the the governor will own

    DROP TABLE IF EXISTS t_GovernanceDetail;
    CREATE TEMP TABLE t_GovernanceDetail as
    SELECT * FROM exgovernancedetail limit 0;

    INSERT INTO t_GovernanceDetail (exGovernanceID, exSystemID, exRecordGroupID, sysdictionarytableidsubscribedto, RowIDSubscribedTo, RowIDMaster)
    SELECT eg.id                   exGovernanceID,
           eg.exSystemID,
           eg.exRecordGroupID,
           rg.sysdictionarytableid sysdictionarytableidsubscribedto,
           p_id                    RowIDSubscribedTo,
           p_id                    RowIDMaster
    FROM exgovernance eg
    JOIN exrecordgroup rg
         ON rg.id = eg.exrecordgroupid
    JOIN sysdictionarytable dt
         ON dt.id = rg.sysdictionarytableid
    WHERE eg.id = l_exGovernanceID;

    SELECT MAX(treelevel) INTO l_max FROM t_exRecordGroupHierarchy;
    l_i := 2;
    LOOP
        EXIT WHEN l_i > COALESCE(l_max, 0);

        FOR l_rec IN
            SELECT DISTINCT dt.name TableName,
                            rgH.whereclause,
                            dt.id   sysdictionarytableid,
                            rgh.id  exRecordGroupID,
                            rgh.exRecordGroupIDParent,
                            rgh.drilldowncolumn,
                            dc.Name ColumnName,
                            lj.multilinkjoin
            FROM (
                     SELECT p_exRecordGroupID exrecordgroupid, p_exSystemID exSystemID, p_id RowIDSubscribedTo) sub
            JOIN      t_exRecordGroupHierarchy rgH
                      ON rgH.ParentID = sub.exrecordgroupid
            JOIN      sysdictionarycolumn dc
                      ON dc.id = rgH.sysdictionarycolumnid
            JOIN      sysdictionarytable dt
                      ON dt.id = dc.sysdictionarytableid
            LEFT JOIN LATERAL (SELECT CASE WHEN LOWER(dc.purpose) = 'multilink' THEN
                                                           'and ' ||
                                                           REPLACE(LOWER(dc.name), 'rowid', 'sysdictionarytableid') ||
                                                           '=' || rgH.sysdictionarytableidparent::VARCHAR
                                           ELSE ''
                                           END multilinkjoin) lj
                      ON TRUE
            WHERE rgH.TreeLevel = l_i
        LOOP
            l_sql := FORMAT('
insert into t_GovernanceDetail (exGovernanceID, exSystemID, exRecordGroupID, sysdictionarytableidsubscribedto, RowIDSubscribedTo, RowIDMaster)
select distinct b.exGovernanceid,
b.exSystemID,
%1$s exrecordgroupid,
%2$s sysdictionarytableidsubscribedto,
%3$s.id RowIDSubscribedTo,
%3$s.%8$s RowIDMaster
from %3$s
join t_GovernanceDetail b on b.rowidmaster = %3$s.%4$s and b.exrecordgroupid=%5$s
where true %6$s %7$s ;', l_rec.exrecordgroupid, l_rec.sysDictionaryTableID, l_rec.TableName, l_rec.ColumnName,
                            l_rec.exrecordgroupidparent, l_rec.multilinkjoin,
                            COALESCE('and ' || l_rec.whereclause, ''), l_rec.drilldowncolumn);
            IF p_debug
            THEN
                RAISE NOTICE '%', l_SQL;
            END IF;

            EXECUTE (l_SQL);

        END LOOP;
        l_i := l_i + 1;

    END LOOP;

    IF NOT EXISTS(SELECT FROM t_GovernanceDetail)
    THEN
        RETURN;
    END IF;

    insert into exgovernancedetail (exgovernanceid, exsystemid, exrecordgroupid, sysdictionarytableidsubscribedto, rowidsubscribedto, rowidmaster, syschangehistoryid)
    select exgovernanceid, exsystemid, exrecordgroupid, sysdictionarytableidsubscribedto, rowidsubscribedto, rowidmaster, p_syschangehistoryid
    from t_governancedetail;

    SELECT a.id
    INTO l_exRecordGroupGovernor
    FROM exRecordGroup a
    JOIN sysdictionarytable b
         ON b.id = a.sysdictionarytableid
    WHERE b.name ILIKE 'exgovernance';

    IF l_exRecordGroupGovernor IS NULL
    THEN
        INSERT INTO exrecordgroup (sysdictionarytableid, name, syschangehistoryid)
        SELECT id sysdictionarytableid, 'exGovernance', p_sysChangeHistoryID
        FROM sysdictionarytable a
        WHERE a.name ILIKE 'exgovernance'
        RETURNING id INTO l_exRecordGroupGovernor;

        INSERT INTO exrecordgroup (exrecordgroupidparent, sysdictionarytableid, sysdictionarycolumnid, name, syschangehistoryid)
        SELECT a.id, b.id, c.id, 'exgovernance->exgovernancedetail', a.syschangehistoryid
        FROM exrecordgroup a
        JOIN sysdictionarytable b
             ON b.name = 'exgovernancedetail'
        JOIN sysdictionarycolumn c
             ON c.sysdictionarytableid = b.id AND c.sysdictionarytableidforeign = a.sysdictionarytableid
        WHERE a.id = l_exRecordGroupGovernor;

    END IF;

    SELECT id
    INTO l_exsubscriberidgovernor
    FROM exsubscriber a
    WHERE a.exsystemid = p_exsystemid
      AND a.exsubscriberidparent IS NULL
      AND NOT EXISTS(SELECT FROM exsubscriber aa WHERE aa.exsubscriberidparent = a.id);

    IF l_exsubscriberidgovernor IS NULL
    THEN
        INSERT INTO exsubscriber (exsystemid, name, syschangehistoryid)
        SELECT p_exsystemid, name, p_syschangehistoryid
        FROM exsystem
        WHERE id = p_exsystemid
          AND NOT EXISTS(SELECT
                         FROM exsubscriber a
                         WHERE a.exsystemid = p_exsystemid
                           AND a.exsubscriberidparent IS NULL
                           AND NOT EXISTS(SELECT FROM exsubscriber aa WHERE aa.exsubscriberidparent = a.id))
        RETURNING id INTO l_exsubscriberidgovernor;

    END IF;

    IF NOT EXISTS(SELECT
                  FROM exsubscription a
                  WHERE a.exrecordgroupid = l_exRecordGroupGovernor
                    AND a.exsubscriberid = l_exsubscriberidgovernor
                    AND a.rowidsubscribedto = p_id
        )
    THEN
        INSERT INTO exsubscription (exsubscriberid, exrecordgroupid, rowidsubscribedto, effectivedate, expirydate, syschangehistoryid)
        SELECT l_exsubscriberidgovernor,
               l_exRecordGroupGovernor,
               l_exGovernanceID,
               NOW()::DATE,
               NOW()::DATE,
               p_syschangehistoryid;
    END IF;

    CALL spexpackageexport(p_syschangehistoryid);

    SELECT STRING_AGG('
select ' || id::VARCHAR || ' id,''' ||
                      name::VARCHAR || ''' name,' ||
                      COALESCE(''''||systemmodule::VARCHAR||'''', 'NULL') || ' systemmodule,' ||
                      COALESCE(syschangehistoryid::VARCHAR, 'NULL') || '::BIGINT syschangehistoryid', ' UNION')
    INTO l_sqltemp
    FROM sysdictionarytable
    WHERE id IN (   SELECT sysdictionarytableid from exrecordgroup where id in (
                    SELECT exrecordgroupid FROM exgovernancedetail WHERE syschangehistoryid=p_syschangehistoryid));

    l_sql := '
do
$temp$
begin
insert into s0000v0000.sysdictionarytable (id, name, systemmodule, syschangehistoryid)
select * from (
'|| l_sqltemp || ') a
where a.id not in (select id from s0000v0000.sysdictionarytable);
';

    SELECT STRING_AGG('
select ' || id::VARCHAR || ' id,' ||
                      COALESCE(exrecordgroupidparent::VARCHAR, 'NULL') ||'::BIGINT exrecordgroupidparent,''' ||
                      name::VARCHAR || ''' name,' ||
                      COALESCE(sysdictionarytableid::VARCHAR, 'NULL') ||'::BIGINT sysdictionarytableid,' ||
                      COALESCE(syschangehistoryid::VARCHAR, 'NULL') || '::BIGINT syschangehistoryid', ' UNION')
    INTO l_sqltemp
    FROM exrecordgroup
    WHERE id IN (
                    SELECT exrecordgroupid FROM exgovernancedetail WHERE syschangehistoryid=p_syschangehistoryid);

    l_sql := l_sql || '
insert into s0000v0000.exrecordgroup (id, exrecordgroupidparent, name, sysdictionarytableid, syschangehistoryid)
select * from (
'|| l_sqltemp || ') a
where a.id not in (select id from s0000v0000.exrecordgroup);
';

    SELECT '
insert into s0000v0000.exgovernance (id, exsystemid, exrecordgroupid, rowidsubscribedto, transferdate, syschangehistoryid)
select ' || id::VARCHAR || ',' || exsystemid::VARCHAR || ',' || exrecordgroupid::VARCHAR || ',' ||
           rowidsubscribedto::VARCHAR || ',''' || transferdate::VARCHAR || ''',' || syschangehistoryid::VARCHAR
    INTO l_sqltemp
    FROM exgovernance
    WHERE id = l_exgovernanceid;

    l_sql := l_sql || l_sqltemp || ';
insert into s0000v0000.exgovernancedetail (id, exgovernanceid, exsystemid, exrecordgroupid, sysdictionarytableidsubscribedto, rowidsubscribedto, syschangehistoryid, rowidmaster)
';

    SELECT STRING_AGG('
select ' || id::VARCHAR || ',' ||
                      exgovernanceid::VARCHAR || ',' ||
                      exsystemid::VARCHAR || ',' ||
                      exrecordgroupid::VARCHAR || ',' ||
                      sysdictionarytableidsubscribedto::VARCHAR || ',' ||
                      rowidsubscribedto::VARCHAR || ',' ||
                      syschangehistoryid::VARCHAR || ',' ||
                      rowidmaster::VARCHAR, ' union')
    INTO l_sqltemp
    FROM exgovernancedetail
    WHERE exgovernanceid = l_exgovernanceid;

    l_sql := l_sql || l_sqltemp || ';
end
$temp$ LANGUAGE plpgsql;
';

    IF p_debug
    THEN
        RAISE NOTICE '%', l_sql;
    END IF;

    PERFORM fnsysMDSExecute(l_sql);

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
