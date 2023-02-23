CREATE OR REPLACE FUNCTION S0000V0000.fnsysMasterDataQuery(p_table VARCHAR DEFAULT NULL
, p_queryfilter VARCHAR DEFAULT NULL, p_querydate DATE DEFAULT NULL, p_whereclause VARCHAR DEFAULT NULL
, p_foreigntable VARCHAR DEFAULT NULL, p_level INT DEFAULT 1, p_globalcondition VARCHAR DEFAULT NULL
, p_debug BOOLEAN DEFAULT FALSE)
    RETURNS VARCHAR
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
This procedure generates select statements to expose data related to a particular master record.

Parameters:

20221004    Blair Kjenner   Initial Code

p_table - (default null) filters the master tables that the query will look at.  If a query
          the p_whereclause is specified then this parameter must identify a specific table.  % symbols can be used.
p_queryfilter - (default null) - allows data to be filtered based on the master data translation.  % symbols can be used.
p_querydate - (default current date) - filters data based on the query date
p_whereclause - (default null) - allows a where clause for the master table to be specified
p_foreigntable - (default null) - allows you to filter the foreign data tables that are included in the results.  % symbols can be used.
p_level - (default 1) - allows you to drill down data multiple levels
p_globalcondition - (default null) - allows you to filter based on commonly encountered columns like rowstatus and id
p_debug - (default false) - returns results useful for debugging

A potential enhancement in the future would be to drill down past join tables.  For example, if you went from a land
interest to parcels registered against that interest, it would know it was a join table, would filter the join results based
on the land interest and then use the resulting land parcel ids to drill down to land parcels.  This would mean using
data from the t_foreignkeys plus specifically identifying join tables.  Of course, there is quite a bit of complexity
because tables can have multilinks and can be temporal.

select fnsysMasterDataQuery (p_table:='crmcontact',p_queryfilter:='aaa%');

*/
DECLARE
    l_rec      RECORD;
    l_template VARCHAR;
    l_tempsql  VARCHAR;
    l_sql      VARCHAR;
    l_i        INT;
BEGIN
    p_table := LOWER(p_table);
    p_queryfilter := fixquote(LOWER(p_queryfilter));
    p_querydate := COALESCE(p_querydate, NOW()::DATE);
    p_globalcondition := COALESCE(p_globalcondition, 'rowstatus=''a''');

    IF NOT EXISTS(SELECT FROM sysmasterdataindex)
    THEN
        CALL spsysMasterDataIndexGenerate();
    END IF;

    DROP TABLE IF EXISTS t_foreignkeyreferences;
    CREATE TEMP TABLE t_foreignkeyreferences
    (
        foreignkeyreferencesid      SERIAL,
        sysDictionaryTableIDMaster  BIGINT,
        TableNameMaster             VARCHAR,
        TableFriendlyNameMaster     VARCHAR,
        TemporalConditionMaster     TEXT,
        sysDictionaryTableIdForeign BIGINT,
        TableNameForeign            VARCHAR,
        FriendlyNameForeign         VARCHAR,
        TemporalConditionForeign    TEXT,
        ColumnNameForeign           VARCHAR,
        PurposeForeign              VARCHAR,
        RowIDForeign                BIGINT,
        RowidMaster                 BIGINT,
        datalevel                   INT
    );

    CREATE INDEX t_foreignkeyreferencesforeigntablerowid ON t_foreignkeyreferences (TableNameForeign, RowidMaster);
    CREATE INDEX t_foreignkeyreferencesforeigntablemaster ON t_foreignkeyreferences (TableNameForeign, TableNameMaster, FriendlyNameForeign);

    IF p_whereclause IS NOT NULL
        AND NOT EXISTS(SELECT FROM sysdictionarytable WHERE name = p_table)
    THEN
        RAISE EXCEPTION 'Error - where clause cannot be specified without specifying a p_table that relates to a specific table';
    END IF;

    l_template := '
    INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
    SELECT DISTINCT dd.id                            sysDictionaryTableIDMaster,
                    lower(dd.name)                   TableNameMaster,
                    dd.pluralname                    TableFriendlyNameMaster,
                    jl1.temporalcondition            TemporalConditionMaster,
                    cc.id                            sysDictionaryTableIdForeign,
                    lower(cc.name)                   TableNameForeign,
                    l_parent||COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                    jl0.temporalcondition            TemporalConditionForeign,
                    lower(bb.name)                   ColumnNameForeign,
                    lower(bb.purpose)                PurposeForeign,
                    aa.RowIdForeign                  RowIDForeign,
                    aa.RowIdMaster                   RowidMaster,
                    l_datalevel                      DataLevel
    FROM sysmasterdataindex aa
    LEFT JOIN sysdictionarycolumn bb
              ON bb.id = aa.sysdictionarycolumnidforeign
    LEFT JOIN sysdictionarytable cc
              ON cc.id = bb.sysdictionarytableid
    JOIN      sysdictionarytable dd
              ON dd.id = aa.sysdictionarytableidmaster
    JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                       THEN '' AND temporalstartdate <= '''''' || l_querydate
                         || ''''''::DATE AND temporalenddate >= ''''''
                         || l_querydate || ''''''::DATE''
                       ELSE ''''
                       END temporalcondition) JL0 ON TRUE
    JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                       THEN '' AND temporalstartdate <= '''''' || l_querydate
                         || ''''''::DATE AND temporalenddate >= ''''''
                         || l_querydate || ''''''::DATE''
                       ELSE ''''
                       END temporalcondition) JL1 ON TRUE
        l_joinstatement
    WHERE (l_table IS NULL OR dd.name ILIKE l_table)
      AND (l_queryfilter IS NULL OR aa.foreignkeytranslation ILIKE l_queryfilter)
      AND (false=l_rowidfilter OR aa.RowIdMaster in (select aaa.RowidForeign from t_foreignkeyreferences aaa where aaa.TableNameForeign=dd.name))
      AND (l_foreigntable IS NULL OR cc.name ILIKE l_foreigntable)
        l_whereclause;
    ';
    -- This SQL statement pulls data for the master records and related child records
    l_sql := REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l_template, 'l_joinstatement',
                                                                                     COALESCE('JOIN vw' || p_table || ' a on a.id=aa.RowIDMaster', ''))
                                                                         , 'l_table',
                                                                             COALESCE('''' || p_table || '''', 'null'))
                                                                 , 'l_queryfilter',
                                                                     COALESCE('''' || p_queryfilter || '''', 'null'))
                                                         , 'l_foreigntable',
                                                             COALESCE('''' || p_foreigntable || '''', 'null'))
                                                 , 'l_whereclause', COALESCE('AND ' || p_whereclause, ''))
                                         , 'l_querydate', '''' || p_querydate::VARCHAR || '''')
                                 , 'l_rowidfilter', 'false')
                         , 'l_datalevel', 1::VARCHAR)
        , 'l_parent', '''''');

    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;
    EXECUTE l_sql;

    -- created for performance reasons
    IF p_level > 1 AND fnIfTableExists('t_foreignkey') = FALSE
    THEN
        CREATE TEMP TABLE t_foreignkey AS
        SELECT DISTINCT bb.name foreigntable, RowIDMaster
        FROM sysmasterdataindex aa
        JOIN sysdictionarytable bb
             ON bb.id = aa.sysdictionarytableidmaster;
    END IF;
    -- This statement builds on the foreign key references by using the child records to traverses to grandchild records (2)
    -- and grandchild to traverse to great grandchildren (3) and beyond
    FOR l_i IN 2..p_level
    LOOP
        FOR l_rec IN SELECT DISTINCT TableNameForeign,
                                     TableNameMaster,
                                     FriendlyNameForeign,
                                     TableFriendlyNameMaster
                     FROM t_foreignkeyreferences a
                     WHERE datalevel = l_i - 1
                       AND EXISTS(SELECT
                                  FROM t_foreignkey
                                  WHERE foreigntable = a.TableNameForeign
                                    AND RowIDMaster = a.RowidForeign)
                     GROUP BY TableNameForeign, TableNameMaster, FriendlyNameForeign, TableFriendlyNameMaster
                     ORDER BY TableNameForeign
        LOOP
            l_sql := REPLACE(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l_template, 'l_joinstatement', '')
                                                                        , 'l_table', '''' || l_rec.TableNameForeign ||
                                                                                     '''')
                                                                , 'l_queryfilter', 'null')
                                                        , 'l_foreigntable', 'null')
                                                , 'l_whereclause', '')
                                        , 'l_querydate', '''' || p_querydate::VARCHAR || '''')
                                , 'l_rowidfilter', 'true')
                        , 'l_datalevel', l_i::VARCHAR)
                , 'l_parent', '''' || COALESCE(l_rec.FriendlyNameForeign, l_rec.TableNameForeign) || '->''');

            IF p_debug
            THEN
                RAISE NOTICE '%', l_SQL;
            END IF;
            EXECUTE l_sql;
        END LOOP;
    END LOOP;
/*
 -- Parameters
 select ''Master Table'' Parameter,''' || COALESCE(p_table, 'Not specified') || ''' Value union
 select ''Query filter'',''' || COALESCE(p_queryfilter, 'Not specified') || ''' union
 select ''Where clause'',''' || COALESCE(fixquote(p_whereclause), 'Not specified') || ''' union
 select ''Global Condition'',''' || COALESCE(fixquote(p_globalcondition), 'Not specified') || ''' union
 select ''Drilldown level'',''' || COALESCE(p_level, '1') || ''' union
 select ''Query date'',''' || p_querydate || ''';
*/
    l_sql := '
set search_path to '|| CURRENT_SETTING('search_path') || ';
 ';

    -- Changed our template
    l_template := '
SELECT * FROM vwl_tablename WHERE l_columnname = l_rowidl_multilinkl_temporalconditionl_globalcondition';

    -- Now that we have foreign key references we are going to build select statements
    -- for the master records.  Note if no master table name was specified
    -- there could be master records in many tables.
    FOR l_rec IN SELECT TableNameMaster, TableFriendlyNameMaster
                 FROM t_foreignkeyreferences a
                 WHERE NOT EXISTS(
                         SELECT
                         FROM t_foreignkeyreferences aa
                         WHERE aa.TableNameForeign = a.TableNameMaster)
                    OR a.datalevel = 1
                 GROUP BY TableNameMaster, TableFriendlyNameMaster
                 ORDER BY TableFriendlyNameMaster
    LOOP
        l_sql := l_sql || '
-- ' || l_rec.TableFriendlyNameMaster;
        SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l_template,
                                                                          'l_tablename', a.TableNameMaster),
                                                                  'l_columnname', a.ColumnNameForeign), 'l_rowid',
                                                          a.RowIDMaster::VARCHAR),
                                                  'l_temporalcondition', a.TemporalConditionMaster),
                                          'l_multilink', ''),
                                  'l_globalcondition', ' and ' || p_globalcondition)
                   , ' union ')
        INTO l_tempsql
        FROM (
                 SELECT DISTINCT 'id' ColumnNameForeign, RowIDMaster, TemporalConditionMaster, TableNameMaster
                 FROM t_foreignkeyreferences
                 WHERE TableNameMaster = l_rec.TableNameMaster
                   AND datalevel = 1
                 ORDER BY RowIDMaster) a;
        l_sql := l_sql || l_tempsql || ';';
    END LOOP;

    -- Now we are going to build select statements for children (1), grandchildren (2) and beyond.
    FOR l_rec IN SELECT a.TableNameForeign, b.FriendlyNameForeign
                 FROM (
                          SELECT TableNameForeign, MIN(datalevel) DataLevel
                          FROM t_foreignkeyreferences
                          WHERE TableNameForeign IS NOT NULL
                          GROUP BY TableNameForeign) A
                     -- A foreign table may be referenced many times but we will
                     -- see all references returned in the query
                     -- the following join lateral gets the lowest level
                     -- foreigntablefriendly name for the query header
                     -- e.g. see glentry in one select
                 JOIN LATERAL (SELECT FriendlyNameForeign
                               FROM t_foreignkeyreferences aa
                               WHERE aa.TableNameForeign = a.TableNameForeign
                                 AND aa.datalevel = a.datalevel
                               LIMIT 1) b
                      ON TRUE
                 ORDER BY b.FriendlyNameForeign
    LOOP
        SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l_template,
                                                                          'l_tablename', l_rec.TableNameForeign),
                                                                  'l_columnname', a.ColumnNameForeign), 'l_rowid',
                                                          a.RowIDMaster::VARCHAR),
                                                  'l_temporalcondition', a.TemporalConditionForeign),
                                          'l_multilink', CASE WHEN a.PurposeForeign = 'multilink'
                                                                  THEN ' and ' ||
                                                                       REPLACE(a.ColumnNameForeign, 'rowid', 'sysdictionarytableid') ||
                                                                       '=' || a.sysDictionaryTableIDMaster::VARCHAR
                                                              ELSE ''
                                                              END),
                                  'l_globalcondition', ' and ' || p_globalcondition)
                   , ' union ')
        INTO l_tempsql
        FROM (
                 SELECT a.ColumnNameForeign,
                        a.TemporalConditionForeign,
                        a.RowIDMaster,
                        a.PurposeForeign,
                        a.sysDictionaryTableIDMaster
                 FROM (
                          SELECT ColumnNameForeign,
                                 TemporalConditionForeign,
                                 RowIDMaster,
                                 PurposeForeign,
                                 sysDictionaryTableIDMaster,
                                 MIN(datalevel) datalevel
                          FROM t_foreignkeyreferences
                          WHERE TableNameForeign = l_rec.TableNameForeign
                          GROUP BY ColumnNameForeign, TemporalConditionForeign, RowIDMaster, PurposeForeign,
                                   sysDictionaryTableIDMaster) a
                 -- Join Lateral creates FriendlyNameForeign which is used for sorting selecting statements
                 -- so we see mttaxroll followed by mttaxroll->assessments etc
                 JOIN LATERAL (SELECT FriendlyNameForeign
                               FROM t_foreignkeyreferences aa
                               WHERE aa.ColumnNameForeign = a.ColumnNameForeign
                                 AND aa.TableNameForeign = l_rec.TableNameForeign
                                 AND aa.RowIDMaster = a.RowIDMaster
                                 AND aa.sysDictionaryTableIDMaster = a.sysDictionaryTableIDMaster
                                 AND aa.datalevel = a.datalevel
                               ORDER BY aa.datalevel
                               LIMIT 1) b
                      ON TRUE
                 ORDER BY datalevel, FriendlyNameForeign, ColumnNameForeign, RowIDMaster) a;
        l_sql := l_sql || '
-- ' || l_rec.FriendlyNameForeign || l_tempsql || ';';

    END LOOP;
    l_sql := l_sql || '
';

    RETURN l_sql;
END
$$ LANGUAGE plpgsql;
