CREATE OR REPLACE PROCEDURE S0000V0000.spexPackageExport(p_sysChangeHistoryID BIGINT DEFAULT NULL, p_debug BOOLEAN DEFAULT FALSE)
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
This procedure exports data to subscriber systems based on the following process.
- check to see if any new data needs to be exported based on changes to subscriptions or record grouping.
- create export records new subscriptions plus export data for any new change history events
- create export data for foreign key references to any data that is being exported.
- update the history of what systems gets what data.  If a system has previously got
a foreign key reference record in a previous batch, it will not give it to it again.
Likewise if a record is being deleted, it will not send foreign key references for it

CHANGE LOG
20211008 Blair Kjenner	Initial Code

PARAMETERS
p_sysChangeHistoryID - All new change history records plus all exHistory records will be linked to this id.

SAMPLE CALL

call spexPackageExport(p_sysChangeHistoryID:=fnsysChangeHistoryCreate('Test exExport'));

*/
DECLARE
    e_Context           TEXT;
    e_Msg               TEXT;
    e_State             TEXT;
    l_currentsearchpath VARCHAR;
    l_Error             RECORD;
    l_exHistoryBatchID  BIGINT;
    l_i                 INT;
    l_max               INT;
    l_Rec               RECORD;
    l_message           VARCHAR;
    l_sql               VARCHAR;
    l_updatecount       INT;
BEGIN

    p_sysChangeHistoryID := coalesce(p_sysChangeHistoryID, fnsysChangeHistoryCreate('Export data as of '||now()::varchar));

    -- This condition checks for an erroneous condition where have a child entry in a record group
    -- but they have specified a rowid. The rowid for child record groups needs to be deduced
    -- based on the id of the parent record.  For example, if we are getting addresses for
    -- a contact, then the contactid used to get the address needs to come from the contact
    -- record and should not be specified as a rowid on the subscription.
    SELECT FORMAT('Record Group: %s, Subscriber: %s, RowID: %s', e2.name, e3.name, e.rowidsubscribedto)
    INTO l_message
    FROM exsubscription e
    JOIN exrecordgroup e2
         ON e.exrecordgroupid = e2.id
    JOIN exsubscriber e3
         ON e3.id = e.exsubscriberid
    WHERE e.rowidsubscribedto IS NOT NULL
      AND e2.exrecordgroupidparent IS NOT NULL
    LIMIT 1;

    IF l_message IS NOT NULL
    THEN
        -- Cannot allow to subscribe to a child record group (i.e. parent is not null) - %
        RAISE SQLSTATE '51066' USING MESSAGE = l_message;
    END IF;

    l_message := fnexWhereClauseCheck();
    IF l_message IS NOT NULL
    THEN
        RAISE SQLSTATE '51078' USING MESSAGE = l_message;
    END IF;

    -- Check for duplicate subscription records
    SELECT FORMAT('Record Group: %s, Subscriber: %s, RowID: %s', e2.name, e3.name, e.rowidsubscribedto)
    INTO l_message
    FROM exsubscription e
    JOIN exrecordgroup e2
         ON e.exrecordgroupid = e2.id
    JOIN exsubscriber e3
         ON e3.id = e.exsubscriberid
    GROUP BY FORMAT('Record Group: %s, Subscriber: %s, RowID: %s', e2.name, e3.name, e.rowidsubscribedto)
    HAVING COUNT(*) > 1
    LIMIT 1;

    IF l_message IS NOT NULL
    THEN
        -- Duplicate subscription
        RAISE SQLSTATE '51067' USING MESSAGE = l_message;
    END IF;

    DROP TABLE IF EXISTS t_exSubscriberHierarchy;

    -- When a subscription references a subscriber group it only references the id of the parent.
    -- This query walks through the subscriber hierarchy and creates a record for each node in the
    -- tree with the Root Parent ID and the id of the record.  For example, if we had a parent id
    -- of 100 for a subscriber group called 'retail outlets', then we would we a parent id of 100
    -- and the id of every retail outlet in the group in the output table.  This table is
    -- used below in a query.
    CREATE TEMP TABLE t_exSubscriberHierarchy
    AS
    WITH RECURSIVE Paths(ID,
                         ParentID)
                       AS (
                              -- Start with records with no parents and recurse through child records
                              SELECT ID ID,
                                     ID ParentID
                              FROM exSubscriber
                              WHERE exSubscriberIDParent IS NULL
                              UNION ALL
                              SELECT c.ID       ID,
                                     p.ParentID ParentID
                              FROM exSubscriber AS c
                              INNER JOIN Paths AS p
                                         ON p.ID = c.exSubscriberIDParent)
    SELECT ID, ParentID
    FROM Paths a
    WHERE NOT EXISTS(SELECT FROM exsubscriber aa WHERE aa.exsubscriberidParent = a.ID)
    ORDER BY ParentID, ID;

    CREATE INDEX t_exSubscriberHierarchy_index
        ON t_exSubscriberHierarchy (ParentID, ID);

    DROP TABLE IF EXISTS t_exRecordGroupHierarchy;

    -- When a subscription references a record group it only references the id of the parent.
    -- This query walks through the record hierarchy and creates a record for each node in the
    -- tree with the Root Parent ID and the id of the record.  For example, if we had a parent id
    -- of 100 for a record group called 'Member Data', then we would we a parent id of 100
    -- and the id of every record within the group (e.g. Member Address) would be in the output table.
    -- We also collect other data that will be useful when we process the subscription.
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
                              WHERE exRecordGroupIDParent IS NULL
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

    -- In this phase of the processing we are going to build the t_subscriptiondetail table
    -- It contains one record for every row of data for every subscriber that is going
    -- to receive it.  This is created based on subscriptions and the connections to the
    -- subscriber group and record group tables.  We need to create this table because many
    -- things could be changed like a subscriber group changed, record group changed, new
    -- record got added that needs to get distributed.

    -- First step is to create subscription row records for all record groups that have no parents.
    -- This reason we start with these is because we no the primary key.  We need to
    -- create SubscriptionRow records with this data.  This will help us set the
    -- foreign keys for child records that are levels 2 and beyond.

    DROP TABLE IF EXISTS t_SubscriptionDetail;
    CREATE TEMP TABLE t_SubscriptionDetail
    (
        id                               SERIAL NOT NULL,
        exSubscriptionID                 BIGINT,
        exSubscriberID                   BIGINT,
        exRecordGroupID                  BIGINT,
        sysdictionarytableidsubscribedto BIGINT,
        RowIDSubscribedTo                BIGINT,
        RowIDMaster                      BIGINT
    );

    DROP TABLE IF EXISTS t_SubscriptionTemp;
    CREATE TEMP TABLE t_SubscriptionTemp
    (
        exSubscriptionID                 BIGINT,
        exSubscriberID                   BIGINT,
        exRecordGroupID                  BIGINT,
        sysdictionarytableidsubscribedto BIGINT
    );

    FOR l_rec IN
        SELECT DISTINCT dt.name                            TableName,
                        rg.whereclause,
                        COALESCE(rg.drilldowncolumn, 'id') drilldowncolumn,
                        rg.sysdictionarytableid,
                        sub.rowidsubscribedto IS NULL      isrowidsubscribedtonull
        FROM exsubscription sub
        JOIN t_exSubscriberHierarchy subH
             ON subH.ParentID = sub.exSubscriberID
        JOIN exrecordgroup rg
             ON rg.id = sub.exrecordgroupid
        JOIN sysdictionarytable dt
             ON dt.id = rg.sysdictionarytableid
        WHERE rg.exrecordgroupidparent IS NULL
          AND COALESCE(sub.effectivedate, '1000-01-01'::DATE) <= NOW()::DATE
          AND COALESCE(sub.expirydate, '9999-12-31'::DATE) >= NOW()::DATE
    LOOP
        -- If the whereclause is null and the rowidsubscribed to is not null
        -- then I can pull the data directly from the subscription.
        IF l_rec.whereclause IS NULL AND l_rec.isrowidsubscribedtonull = FALSE AND l_rec.drilldowncolumn = 'id'
        THEN
            INSERT INTO t_SubscriptionDetail (exSubscriptionID, exSubscriberID, exRecordGroupID, sysdictionarytableidsubscribedto, RowIDSubscribedTo, RowIDMaster)
            SELECT sub.id,
                   subh.id,
                   rg.id,
                   l_rec.sysdictionarytableid,
                   sub.rowidsubscribedto,
                   sub.rowidsubscribedto
            FROM exsubscription sub
                -- The following join causes the select to create records for each subscriber in the hierarchy
                -- for the current subscription.  For example, if a subscription was for a subscriber group with
                -- two subscribers in it, two records would get created and assigned to the lowest level subscribers.
            JOIN t_exSubscriberHierarchy subH
                 ON subH.ParentID = sub.exSubscriberID
            JOIN exrecordgroup rg
                 ON rg.id = sub.exrecordgroupid
            JOIN sysdictionarytable dt
                 ON dt.id = rg.sysdictionarytableid
            WHERE rg.sysdictionarytableid = l_rec.sysdictionarytableid
              AND sub.rowidsubscribedto IS NOT NULL
              AND rg.whereclause IS NULL
              AND rg.exrecordgroupidparent IS NULL
              AND COALESCE(sub.effectivedate, '1000-01-01'::DATE) <= NOW()::DATE
              AND COALESCE(sub.expirydate, '9999-12-31'::DATE) >= NOW()::DATE;
        ELSE
            TRUNCATE TABLE t_SubscriptionTemp;
            INSERT INTO t_SubscriptionTemp
            SELECT sub.id  exSubscriptionID
                 , subh.id exSubscriber
                 , rg.id   exRecordGroupID
                 , rg.sysdictionarytableid
            FROM exsubscription sub
                -- The following join causes the select to create records for each subscriber in the hierarchy
                -- for the current subscription.  For example, if a subscription was for a subscriber group with
                -- two subscribers in it, two records would get created and assigned to the lowest level subscribers.
            JOIN t_exSubscriberHierarchy subH
                 ON subH.ParentID = sub.exSubscriberID
            JOIN exrecordgroup rg
                 ON rg.id = sub.exrecordgroupid
            JOIN sysdictionarytable dt
                 ON dt.id = rg.sysdictionarytableid
            WHERE rg.sysdictionarytableid = l_rec.sysdictionarytableid
              AND rg.exrecordgroupidparent IS NULL
              AND COALESCE(rg.whereclause, '') = COALESCE(l_rec.whereclause, '')
              AND sub.rowidsubscribedto IS NULL = l_rec.isrowidsubscribedtonull
              AND COALESCE(sub.effectivedate, '1000-01-01'::DATE) <= NOW()::DATE
              AND COALESCE(sub.expirydate, '9999-12-31'::DATE) >= NOW()::DATE;

            l_sql := FORMAT('
insert into t_SubscriptionDetail (exSubscriptionID, exSubscriberID, exRecordGroupID, sysdictionarytableidsubscribedto, RowIDSubscribedTo, RowIDMaster)
select distinct b.exSubscriptionID,
b.exsubscriberid,
b.exrecordgroupid,
b.sysdictionarytableidsubscribedto,
%1$s.id RowIDSubscribedTo,
%1$s.%3$s RowIDMaster
from %1$s
join t_subscriptiontemp b on TRUE
where true %2$s;
', l_rec.TableName, COALESCE('and ' || l_rec.whereclause, ''), l_rec.drilldowncolumn);

            IF p_debug
            THEN
                RAISE NOTICE '%', l_SQL;
            END IF;

            EXECUTE l_SQL;
        END IF;
    END LOOP;

    IF NOT EXISTS(SELECT FROM t_SubscriptionDetail)
    THEN
        RETURN;
    END IF;

    -- The second step is to loop through the children (grandchildren, etc) of the parent groups and use the
    -- parent ids to get the child records.  For example, if we got a parent record that was a contact in the
    -- in the first step, then we are using the id of that contact to get the child records (like address)

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
            FROM exsubscription sub
            JOIN      t_exRecordGroupHierarchy rgH
                      ON rgH.ParentID = sub.exrecordgroupid
            JOIN      t_exSubscriberHierarchy subH
                      ON subH.ParentID = sub.exSubscriberID
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
              AND COALESCE(sub.effectivedate, '1000-01-01'::DATE) <= NOW()::DATE
              AND COALESCE(sub.expirydate, '9999-12-31'::DATE) >= NOW()::DATE
        LOOP
            l_sql := FORMAT('
insert into t_SubscriptionDetail (exSubscriptionID, exSubscriberID, exRecordGroupID, sysdictionarytableidsubscribedto, RowIDSubscribedTo, RowIDMaster)
select distinct b.exsubscriptionid,
b.exSubscriberid,
%1$s exrecordgroupid,
%2$s sysdictionarytableidsubscribedto,
%3$s.id RowIDSubscribedTo,
%3$s.%8$s RowIDMaster
from %3$s
join t_SubscriptionDetail b on b.rowidmaster = %3$s.%4$s and b.exrecordgroupid=%5$s
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

    IF NOT EXISTS(SELECT FROM t_SubscriptionDetail)
    THEN
        RETURN;
    END IF;

    -- Now we have a comprehensive subscription detail list created in t_subscriptiondetail
    -- Next we will call spsysSyncTables to sync it with the actual exSubscriptionDetail
    -- data.  It will insert, update, or delete records as required based on the differences
    -- and mark the updates with the sysChangeHistoryID that was passed to the procedure.
    -- This will help us identify exactly what changed since the last time we distributed
    -- records.  The l_updatecount is updated based on p_UpdateCount.  It tells us if
    -- anything changed.

    -- Synchronizes the exsubscriptiondetail with the t_subscriptiondetail
    CALL spsysSyncTables(p_SourceTable := 't_subscriptiondetail',
                         p_DestTable := 'exsubscriptiondetail',
                         p_sysChangeHistoryID := p_syschangehistoryid,
                         p_UpdateCount := l_updatecount);

    IF l_updatecount > 0
    THEN
        -- Now we are going to create dynamic SQL to create changehistoryrow records
        -- for subscribed records that need to be exported
        FOR l_Rec IN
            SELECT DISTINCT dt.name TableName, sd.sysdictionarytableidsubscribedto, dt.istabletemporal, jl0.tablecolumns
            FROM exsubscriptiondetail SD
            JOIN sysdictionarytable dt
                 ON dt.id = sd.sysdictionarytableidsubscribedto
            JOIN LATERAL ( SELECT STRING_AGG(aa.ColumnName, ', ') TableColumns
                           FROM (
                                    SELECT LOWER(aaa.name) ColumnName
                                    FROM sysDictionaryColumn aaa
                                    WHERE aaa.sysdictionarytableid = sd.sysdictionarytableidsubscribedto
                                    ORDER BY aaa.ID) aa
                     ) AS jl0
                 ON TRUE
            WHERE sd.syschangehistoryid = p_syschangehistoryid
        LOOP
            l_SQL := FORMAT('
                insert into sysChangeHistoryRow (sysDictionaryTableIDAppliesTo, RowIDAppliesTo, RowTemporalEndDate, ActionType, sysChangeHistoryID, OperationType, NewData, OldData, ChangeDate, IsProcessed)
                select %1$s sysDictionaryTableIDAppliesTo
                , a.id
                , %2$s RowTemporalEndDate
                , ''refresh''
                , %3$s sysChangeHistoryID
                , ''export''
                , (select ROW_TO_JSON(b)) newdata
                , null::jsonb olddata
                , now()
                , FALSE
                FROM %4$s a
                JOIN LATERAL (select %5$s) b  ON TRUE
                WHERE a.id in (select aa.rowidsubscribedto
                            from exSubscriptionDetail aa
                            where aa.sysdictionarytableidsubscribedto=%1$s
                            and aa.sysChangeHistoryID=%3$s);'
                                  , l_rec.sysdictionarytableidsubscribedto --%1$s
                                  , CASE WHEN l_rec.istabletemporal THEN 'a.temporalenddate' --%2$s
                                         ELSE 'NULL'
                                         END
                                  , p_syschangehistoryid::varchar          --%3$s
                                  , l_rec.TableName                        --%4$s
                                  , l_rec.TableColumns);                   --%5$s
            IF p_debug
            THEN
                RAISE NOTICE '%', l_SQL;
            END IF;
            EXECUTE (l_sql);

        END LOOP;
    END IF;

    -- If everything worked as it should we will have a changehistoryrow record generated for
    -- every exsubscription record.  If there are records missing, then it means
    -- we have a rowidsubscribedto that does not have an associated record in the database.
    -- This could really be a warning condition because it does not have any effect
    -- on whether the export is actually successful.

    SELECT FORMAT('Record Group: %s, Subscriber: %s, RowID: %s', rg.name, sr.name, sn.rowidsubscribedto)
    INTO l_message
    FROM exsubscription sn
    JOIN exrecordgroup rg
         ON sn.exrecordgroupid = rg.id
    JOIN exsubscriber sr
         ON sr.id = sn.exsubscriberid
    WHERE sn.syschangehistoryid = p_sysChangeHistoryID
      AND sn.rowidsubscribedto IS NOT NULL
      AND NOT EXISTS(SELECT
                     FROM syschangehistoryrow aa
                     WHERE aa.sysdictionarytableidappliesto = rg.sysdictionarytableid
                       AND aa.rowidappliesto = sn.rowidsubscribedto
                       AND aa.syschangehistoryid = p_sysChangeHistoryID)
    LIMIT 1;

    IF l_message IS NOT NULL
    THEN
        -- Subscription is to a row that does not exist
        RAISE SQLSTATE '51065' USING MESSAGE = l_message;
    END IF;

    -- Next we are going to figure out what change history records we need to export.  The change
    -- history records could have been created as a result of the subscription process or it could
    -- have been changes caused by user updates

    DROP TABLE IF EXISTS t_ChangeHistoryToProcess;
    CREATE TEMP TABLE t_ChangeHistoryToProcess
    AS
    SELECT ID FROM sysChangeHistory WHERE COALESCE(isexported, FALSE) = FALSE;

    INSERT INTO exhistorybatch (syschangehistoryid)
    VALUES (p_sysChangeHistoryID)
    RETURNING ID INTO l_exHistoryBatchID;

    -- Need to make sure any change history that needs to be generated has been.
    CALL spsyschangehistorygenerate();

    -- Next we are going to insert exHistory records for everything that needs to be exported
    -- This includes new data based on subscriptions, change history data and foreign key
    -- reference data.

    DROP TABLE IF EXISTS t_exHistory;
    CREATE TEMP TABLE t_exHistory
    AS
    SELECT DISTINCT syschangehistoryrowidexported,
                    exsystemiddestination,
                    exHistoryBatchId,
                    MIN(Source)::CHAR Source,
                    exsubscriptiondetailid
    FROM (
             -- This is a list of all rows that will go to all systems based on
             -- new subscriptions
             SELECT chr.id             syschangehistoryrowidexported,
                    s.exsystemid       exsystemiddestination,
                    l_exHistoryBatchID exHistoryBatchId,
                    'r'                Source,
                    sd.id              exsubscriptiondetailid
             FROM syschangehistoryrow AS chr
             JOIN exsubscriptiondetail AS sd
                  ON sd.sysdictionarytableidsubscribedto = chr.sysdictionarytableidappliesto
                      AND sd.rowidsubscribedto = chr.rowidappliesto
                      AND sd.syschangehistoryid = chr.syschangehistoryid
             JOIN exsubscriber AS s
                  ON sd.exsubscriberid = s.id
             WHERE chr.syschangehistoryid = p_syschangehistoryid
             UNION
             -- This is a list of all rows that were added due to a change history event
             SELECT chr.id, s.exsystemid, l_exHistoryBatchID, 'u' Source, sd.id
             FROM syschangehistoryrow AS chr
             JOIN exsubscriptiondetail sd
                  ON chr.sysdictionarytableidappliesto = sd.sysdictionarytableidsubscribedto
                      AND chr.rowidappliesto = sd.rowidsubscribedto
             JOIN exsubscriber s
                  ON s.id = sd.exsubscriberid
             WHERE chr.syschangehistoryid IN (
                                                 SELECT id
                                                 FROM t_ChangeHistoryToProcess)
               AND chr.actiontype != 'refresh') a
        -- If we have previously exported a changehistoryrow to a system, we don't need to do it again
        -- on this batch.  This will mostly screen out re-exporting foreign key references that we
        -- previously exported.
    WHERE NOT EXISTS(SELECT *
                     FROM exhistory aa
                     WHERE aa.exsystemiddestination = a.exsystemiddestination
                       AND aa.syschangehistoryrowidexported = a.syschangehistoryrowidexported)
          -- It is possible that we may get a record that was updated plus a refresh.  In that case, we
          -- only want the 'r'.
    GROUP BY syschangehistoryrowidexported,
             exsystemiddestination,
             exHistoryBatchId,
             exsubscriptiondetailid;

    -- I may need to deal with foreign key references on the changehistory data
    -- like sysdictionarytableidappliesto, crmcontactiduser, etc.

    -- Next we are going to get all the data that we need to export and put it in temporary tables
    -- This includes sysChangeHistory, sysChangeHistoryRow, sysChangeHistoryColumn, exHistoryBatch and exHistory.
    -- The reason this is necessary is because we are going to switch to the subnet server and then copy these tables
    -- to it.
    DROP TABLE IF EXISTS t_sysChangeHistory;
    CREATE TEMP TABLE t_sysChangeHistory
    AS
    SELECT *
    FROM sysChangeHistory a
    WHERE (id = p_sysChangeHistoryID OR
           id IN (
                     SELECT bb.syschangehistoryid
                     FROM t_exHistory aa
                     JOIN syschangehistoryrow bb
                          ON aa.syschangehistoryrowidexported = bb.id));

    DROP TABLE IF EXISTS t_sysChangeHistoryRow;
    CREATE TEMP TABLE t_sysChangeHistoryRow
    AS
    SELECT *
    FROM sysChangeHistoryRow a
    WHERE a.id IN (
                      SELECT syschangehistoryrowidexported
                      FROM t_exHistory aa);

    -- This merely sets up the t_sysChangeHistoryColumn according to the sysChangeHistoryColumn structure
    DROP TABLE IF EXISTS t_sysChangeHistoryColumn;
    CREATE TEMP TABLE t_sysChangeHistoryColumn
    AS
    SELECT *
    FROM sysChangeHistoryColumn a
    WHERE a.syschangehistoryrowid IN (
                                         SELECT syschangehistoryrowidexported
                                         FROM t_exHistory aa);

    -- This call will take all data we have added to t_syschangehistoryrow and redact any information
    -- necessary from newdata and olddata and from t_syschangehistorycolumn
    CALL spexSubscriptionRedact();

    -- Now that we have generalized the contact data, we can go check for foreign key references
    -- that need to be satisfied.

    -- If we didnt create foreign key reference records, then we could get bad foreign key references on
    -- data that weexported.  The only way to fix this would be to force the user to create subscriptions for
    -- those records.

    -- When we supply data for foreign key references we only supply header columns as defined by
    -- the data dictionary.  It should be noted that we only check foreign key references to one level.
    -- If a foreign key reference record contains a foreign key column we are not creating that foreign
    -- key reference column.  It is certainly possible but you could end up traversing through the entire
    -- database.

    DROP TABLE IF EXISTS t_ForeignKeyRecordsToProcess;
    CREATE TEMP TABLE t_ForeignKeyRecordsToProcess
    AS
    SELECT sysdictionarytableidappliesto,
           rowidappliesto,
           sysDictionaryTableIDFK,
           istabletemporalFK,
           TableNameFK,
           RowIDFK,
           TableColumnsFK,
           GenerateChangeHistoryRow
    FROM (
             -- We need to check for foreign key references on both the before and after image of the data
             SELECT DISTINCT *
             FROM (
                      -- First we check for foreign key references on the before image
                      SELECT s3.sysdictionarytableidappliesto,
                             s3.rowidappliesto,
                             s5.id                    sysDictionaryTableIDFK,
                             s5.istabletemporal       istabletemporalFK,
                             s5.name                  TableNameFK,
                             s4.rawdatabefore::BIGINT RowIDFK
                      FROM t_syschangehistoryrow s3
                      JOIN t_syschangehistorycolumn s4
                           ON s3.id = s4.syschangehistoryrowid
                      JOIN sysdictionarytable s5
                           ON s4.sysdictionarytableidbefore = s5.id
                      -- No point creating a foreign key reference if the row we are looking at is as a result of a delete
                      WHERE s3.operationtype != 'delete'
                      UNION
                      -- Then we check for foreign key references on the after image
                      SELECT s3.sysdictionarytableidappliesto,
                             s3.rowidappliesto,
                             s5.id                   sysDictionaryTableIDFK,
                             s5.istabletemporal      istabletemporalFK,
                             s5.name                 TableNameFK,
                             s4.rawdataafter::BIGINT RowIDFK
                      FROM t_syschangehistoryrow s3
                      JOIN t_syschangehistorycolumn s4
                           ON s3.id = s4.syschangehistoryrowid
                      JOIN sysdictionarytable s5
                           ON s4.sysdictionarytableidafter = s5.id
                      WHERE s3.operationtype != 'delete') aa) a
        -- The following join creates a column list based on the header columns for the record.
    JOIN LATERAL ( SELECT STRING_AGG(aa.ColumnName, ', ') TableColumnsFK,
                          STRING_AGG(CASE WHEN aa.isheadercolumn THEN aa.ColumnName
                                          ELSE ''
                                          END, ', ')      TableHeaderColumnsFK
                   FROM (
                            SELECT LOWER(aaa.name) ColumnName, isHeaderColumn
                            FROM sysDictionaryColumn aaa
                            WHERE aaa.sysdictionarytableid = a.sysdictionarytableidFK
                              AND (aaa.isheadercolumn OR
                                   LOWER(aaa.name) IN ('id', 'rowstatus', 'temporalstartdate', 'temporalenddate'))
                            ORDER BY aaa.ID) aa
             ) AS b
         ON TRUE
             -- The following where clause stops it from including foreign key links to parent
             -- records which will always be there when we have hierarchical record groups.
             -- We also do not want to include foreign key references if the administrator
             -- has not defined any columns.
    JOIN LATERAL (SELECT NOT EXISTS(SELECT
                                    FROM syschangehistoryrow aa
                                    WHERE aa.syschangehistoryid = p_syschangehistoryid
                                      AND aa.sysdictionarytableidappliesto = a.sysdictionarytableidFK
                                      AND aa.rowidappliesto = a.rowidFK) GenerateChangeHistoryRow) c
         ON TRUE
             AND TableHeaderColumnsFK <> '';

    IF EXISTS(SELECT FROM t_ForeignKeyRecordsToProcess)
    THEN
        -- If we did find foreign key reference now we are going to create change history records with all of the foreign key
        -- reference data
        SELECT STRING_AGG(FORMAT('
            insert into sysChangeHistoryRow (sysDictionaryTableIDAppliesTo, RowIDAppliesTo, RowTemporalEndDate, ActionType, sysChangeHistoryID, OperationType, NewData, OldData, ChangeDate, IsProcessed)
            select %s sysDictionaryTableIDAppliesTo
            , a.id
            , %s RowTemporalEndDate
            , ''refresh''
            , %s sysChangeHistoryID
            , ''fkinsert''
            , (select ROW_TO_JSON(b)) newdata
            , null::jsonb olddata
            , now()
            , FALSE
            FROM %s a
            JOIN LATERAL (select %s) b  ON TRUE
            WHERE a.id=%s;
            '
                              , sysDictionaryTableIDFK
                              , CASE WHEN istabletemporalFK THEN 'a.temporalenddate'
                                     ELSE 'NULL'
                                     END
                              , p_syschangehistoryid
                              , TableNameFK
                              , TableColumnsFK
                              , RowIDFK::VARCHAR
                              ),
                          '')
        INTO l_sql
        FROM (
                 SELECT DISTINCT sysDictionaryTableIDFK, istabletemporalFK, TableNameFK, TableColumnsFK, RowIDFK
                 FROM t_ForeignKeyRecordsToProcess
                 WHERE GenerateChangeHistoryRow = TRUE) a;
        IF p_debug
        THEN
            RAISE NOTICE '%', l_SQL;
        END IF;
        EXECUTE (l_sql);

        -- Now we will call change history generate to create the change history column data for
        -- any new foreign key references that were created.

        CALL spsyschangehistorygenerate();

        INSERT INTO t_syschangehistoryrow
        SELECT *
        FROM sysChangeHistoryRow a
        WHERE syschangehistoryid = p_sysChangeHistoryID
          AND id NOT IN (
                            SELECT id
                            FROM t_syschangehistoryrow);

        INSERT INTO t_syschangehistorycolumn
        SELECT *
        FROM sysChangeHistoryColumn a
        WHERE syschangehistoryrowid IN (
                                           SELECT id
                                           FROM t_sysChangeHistoryRow)
          AND a.id NOT IN (
                              SELECT id
                              FROM t_syschangehistorycolumn);

    END IF;

    -- Add the changehistory row records just added as a result of the foreignkey
    -- references into exHistory
    -- This query went from 48s to 1s by breaking into two separate queries
    DROP TABLE IF EXISTS t_exHistorytemp;
    CREATE TEMP TABLE t_exHistorytemp AS
    SELECT DISTINCT chr.id syschangehistoryrowidexported,
                    s.exsystemid,
                    sd.id  exsubscriptiondetailid,
                    chr.sysdictionarytableidappliesto,
                    chr.rowidappliesto
    FROM syschangehistoryrow chr
    JOIN t_ForeignKeyRecordsToProcess fkp
         ON fkp.sysdictionarytableidfk = chr.sysdictionarytableidappliesto
             AND fkp.rowidfk = chr.rowidappliesto
    JOIN exsubscriptiondetail sd
         ON fkp.sysdictionarytableidappliesto = sd.sysdictionarytableidsubscribedto
             AND fkp.rowidappliesto = sd.rowidsubscribedto
    JOIN exsubscriber s
         ON s.id = sd.exsubscriberid
    WHERE chr.syschangehistoryid = p_syschangehistoryid;

    INSERT INTO t_exHistory (
        syschangehistoryrowidexported, exsystemiddestination, exHistoryBatchId, Source, exsubscriptiondetailid)
    SELECT DISTINCT *
    FROM (
             -- This is a list of all rows that were added in as a result of a foreign key refresh
             -- being created
             SELECT syschangehistoryrowidexported,
                    exsystemid         exsystemiddestination,
                    l_exHistoryBatchID exHistoryBatchId,
                    'f'                Source,
                    exsubscriptiondetailid
             FROM t_exHistorytemp a
             WHERE NOT EXISTS(SELECT
                              FROM exHistory aa
                              JOIN syschangehistoryrow bb
                                   ON bb.id = aa.syschangehistoryrowidexported
                              WHERE aa.exsystemiddestination = a.exsystemid
                                AND bb.sysdictionarytableidappliesto = a.sysdictionarytableidappliesto
                                AND bb.rowidappliesto = a.rowidappliesto)) a
        -- If we have previously exported a changehistoryrow to a system, we don't need to do it again
        -- on this batch.  This will mostly screen out re-exporting foreign key references that we
        -- previously exported.
    WHERE NOT EXISTS(SELECT *
                     FROM exhistory aa
                     WHERE aa.exsystemiddestination = a.exsystemiddestination
                       AND aa.syschangehistoryrowidexported = a.syschangehistoryrowidexported)
        -- If the record is in t_exhistory because of a refresh or update
        -- we do not need to add it for a foreign key reference.
      AND NOT EXISTS(SELECT *
                     FROM t_exhistory aa
                     WHERE aa.exsystemiddestination = a.exsystemiddestination
                       AND aa.syschangehistoryrowidexported = a.syschangehistoryrowidexported);

    INSERT INTO exhistory (exhistorybatchid, syschangehistoryrowidexported, exsystemiddestination, source)
    SELECT DISTINCT exhistorybatchid, syschangehistoryrowidexported, exsystemiddestination, source
    FROM t_exHistory;

    DROP TABLE IF EXISTS t_exHistoryBatchExport;
    CREATE TEMP TABLE t_exHistoryBatchExport
    AS
    SELECT id, createdate, distributiondate, applieddate, syschangehistoryid
    FROM exHistoryBatch a
    WHERE a.ID = l_exHistoryBatchID;

    DROP TABLE IF EXISTS t_exHistoryExport;
    CREATE TEMP TABLE t_exHistoryExport
    AS
    SELECT *
    FROM exHistory a
    WHERE a.exHistoryBatchID = l_exHistoryBatchID;

    -- It is possible that we could end up with multiple updates to the same table and row
    -- within this batch.  This could happen if a child record was inserted as a result
    -- of a change that affected the parent and then it was recognized as a new subscription
    -- record.
    DELETE
    FROM t_exHistoryExport
    WHERE id IN (
                    SELECT MIN(aa.id)
                    FROM t_exHistoryExport aa
                    JOIN syschangehistoryrow bb
                         ON bb.id = aa.syschangehistoryrowidexported
                    GROUP BY exsystemiddestination, bb.sysdictionarytableidappliesto, bb.rowidappliesto,
                             bb.rowtemporalenddate
                    HAVING COUNT(*) > 1);

--  Next we are going to switch to the subnet server

    DROP TABLE IF EXISTS t_exsystem;
    CREATE TEMP TABLE t_exsystem AS
    SELECT * FROM exsystem;

    l_currentsearchpath := CURRENT_SETTING('search_path');
    PERFORM SET_CONFIG('search_path', fnsysCurrentSubnetServerSchema() || ',' || l_currentsearchpath, TRUE);

--  Now we will copy the data onto the subnet server
    INSERT INTO sysChangeHistory (id, crmcontactiduser, syscommandid, sysdictionarytableidappliesto, rowidappliesto, rowtemporalenddate, changedate, isexported, ismaxrecordsignored, comments, syschangehistoryidundo, rowstatus)
    SELECT id,
           NULL crmcontactiduser,
           NULL syscommandid,
           sysdictionarytableidappliesto,
           rowidappliesto,
           rowtemporalenddate,
           changedate,
           isexported,
           ismaxrecordsignored,
           comments,
           syschangehistoryidundo,
           rowstatus
    FROM t_sysChangeHistory a
    WHERE NOT EXISTS(SELECT FROM syschangehistory aa WHERE aa.id = a.id);

    INSERT INTO sysChangeHistoryRow (id, syschangehistoryid, sysdictionarytableidappliesto, rowidappliesto, rowtemporalenddate, changedate, actiontype, operationtype, olddata, newdata, isprocessed)
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

    INSERT INTO sysChangeHistoryColumn (id, syschangehistoryrowid, sysdictionarycolumnid, sysdictionarytableidbefore, sysdictionarytableidafter, rawdatabefore, rawdataafter, translateddatabefore, translateddataafter)
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
    WHERE NOT EXISTS(SELECT FROM syschangehistorycolumn aa WHERE aa.id = a.id);

    INSERT INTO exHistoryBatch (id, createdate, distributiondate, applieddate, syschangehistoryid)
    SELECT id, createdate, distributiondate, applieddate, syschangehistoryid
    FROM t_exHistoryBatchExport a
    WHERE NOT EXISTS(SELECT FROM exhistorybatch aa WHERE aa.id = a.id);

    -- If the system does not exist on the subnet server then add it in
    INSERT INTO exsystem (id, exsubnetserverid, name, schemaname, productionversion, testversion, subscriptionkey, rowstatus, syschangehistoryid)
    SELECT a.*
    FROM t_exsystem a
    WHERE a.id IN (
                      SELECT DISTINCT exsystemiddestination
                      FROM t_exhistory)
      AND a.id NOT IN (
                          SELECT id
                          FROM exsystem);

    INSERT INTO exHistory (id, exHistoryBatchId, syschangehistoryrowidexported, exsystemiddestination, source)
    SELECT id, exHistoryBatchId, syschangehistoryrowidexported, exsystemiddestination, source
    FROM t_exHistoryExport a
    WHERE NOT EXISTS(SELECT FROM exhistory aa WHERE aa.id = a.id);

    PERFORM SET_CONFIG('search_path', l_currentsearchpath, TRUE);

    -- Now we can indicate the change history rows were exported

    UPDATE syschangehistory s
    SET isexported= TRUE
    WHERE s.id IN (
                      SELECT id
                      FROM t_ChangeHistoryToProcess);

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

END;
$$ LANGUAGE plpgsql

/*
--ChangeHistoryToProcess
select * from t_ChangeHistoryToProcess;
--exHistoryBatchExport
select * from t_exHistoryBatchExport;
--exHistoryBatchExport
select * from t_exHistoryBatchExport;
--exHistoryExport
select * from t_exHistoryExport;
--exHistorytemp
select * from t_exHistorytemp;
--exRecordGroupHierarchy
select * from t_exRecordGroupHierarchy;
--exSubscriberHierarchy
select * from t_exSubscriberHierarchy;
--ForeignKeyRecordsToProcess
select * from t_ForeignKeyRecordsToProcess;
--SubscriptionDetail
select * from t_SubscriptionDetail;
--SubscriptionTemp
select * from t_SubscriptionTemp;
--sysChangeHistory
select * from t_sysChangeHistory;
--sysChangeHistoryColumn
select * from t_sysChangeHistoryColumn;
--sysChangeHistoryRow
select * from t_sysChangeHistoryRow;
*/
