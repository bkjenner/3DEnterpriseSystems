CREATE OR REPLACE PROCEDURE S0000V0000.spsysSyncTables(p_SourceTable VARCHAR,
                                                       p_DestTable VARCHAR,
                                                       p_sysChangeHistoryID BIGINT,
                                                       INOUT p_UpdateCount INT,
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
This procedure takes the records in a source table and updates the destination table with them.  This
involves inserting new records if they do not exist, flagging records as deleted if they are not found
in the source table and flagging them as active (if they were deactivated) and found.

The procedure only does this where there are matching columns in both the source and destination table.

CHANGE LOG
20211008 Blair Kjenner Initial Code

PARAMETERS
p_SourceTable - Name of SourceTable
p_DestTable - Name of DestTable
p_sysChangeHistoryID - Change History ID to tag on any updated records
p_UpdateCount - Count of records inserted, updated or deleted.
p_debug - DEFAULT FALSE - TRUE outputs the script without executing it. FALSE executes the script.

SAMPLE CALL

        declare l_updatecount int;
        CALL spsysSyncTables(p_SourceTable := 't_subscriptiondetail',
                             p_DestTable := 'exsubscriptiondetail',
                             p_sysChangeHistoryID := p_syschangehistoryid,
                             p_UpdateCount := l_updatecount,
                             p_debug := FALSE);
        if l_updatecount > 0
        then ...
*/
DECLARE
    l_ColumnList   VARCHAR;
    l_CompareList  VARCHAR;
    l_SQL          VARCHAR;
    l_destschema   VARCHAR;
    l_sourceschema VARCHAR;
BEGIN

    -- Need to get the actual schema in case it is a temp table
    SELECT nspname
    INTO l_sourceschema
    FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n
              ON n.oid
                  = c.relnamespace
    WHERE pg_catalog.PG_TABLE_IS_VISIBLE(c.oid)
      AND UPPER(relname) = UPPER(p_SourceTable);

    -- Need to get the actual schema in case it is a temp table
    SELECT nspname
    INTO l_destschema
    FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n
              ON n.oid
                  = c.relnamespace
    WHERE pg_catalog.PG_TABLE_IS_VISIBLE(c.oid)
      AND UPPER(relname) = UPPER(p_DestTable);

    SELECT STRING_AGG(column_name, ', '), STRING_AGG('dt.' || column_name || '=st.' || column_name, ' and ')
    INTO l_ColumnList, l_CompareList
    FROM (
             SELECT column_name
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME ILIKE p_SourceTable
               AND TABLE_SCHEMA = l_sourceschema
               AND column_name NOT IN
                   ('id', 'syschangehistoryid', 'rowstatus')
               AND column_name IN (
                                      SELECT column_name
                                      FROM INFORMATION_SCHEMA.COLUMNS
                                      WHERE TABLE_NAME ILIKE p_DestTable
                                        AND TABLE_SCHEMA = l_destschema)
             ORDER BY ordinal_position) AS a;

    DROP TABLE IF EXISTS t_updatecount;
    CREATE TEMP TABLE t_updatecount
    (
        updatecount INT
    );

    l_sql :=
            REPLACE(
                    REPLACE(
                            REPLACE(
                                    REPLACE(
                                            REPLACE(
                                                    '
        DO
        $TEMP$
        DECLARE l_updatecount int;
        BEGIN

        INSERT INTO l_desttable (l_columnlist, syschangehistoryid, rowstatus)
        SELECT l_columnlist, l_syschangehistoryid, ''a''
        FROM l_sourcetable AS st
        WHERE NOT EXISTS (SELECT
        FROM l_desttable dt
        WHERE l_comparelist);
        GET DIAGNOSTICS l_updatecount = ROW_COUNT;
        insert into t_updatecount values(l_updatecount);

        UPDATE l_desttable dt
        SET rowstatus=''d'', syschangehistoryid=l_syschangehistoryid
        WHERE NOT EXISTS (SELECT
        FROM l_sourcetable st
        WHERE l_comparelist)
        and dt.rowstatus = ''a'';
        GET DIAGNOSTICS l_updatecount = ROW_COUNT;
        update t_updatecount set updatecount=updatecount+l_updatecount;

        UPDATE l_desttable dt
        SET rowstatus=''a'', syschangehistoryid=l_syschangehistoryid
        WHERE EXISTS (SELECT
        FROM l_sourcetable st
        WHERE l_comparelist )
        and dt.rowstatus = ''d'';
        GET DIAGNOSTICS l_updatecount = ROW_COUNT;
        update t_updatecount set updatecount=updatecount+l_updatecount;

        END
        $TEMP$
        '
                                                , 'l_columnlist', l_columnlist)
                                        , 'l_comparelist', l_CompareList),
                                    'l_desttable', p_desttable)
                        , 'l_sourcetable', p_sourcetable)
                , 'l_syschangehistoryid', p_syschangehistoryid::VARCHAR);

    IF p_debug
    THEN
        RAISE NOTICE '%', l_sql;
    ELSE
        EXECUTE (l_sql);
    END IF;

    SELECT updatecount INTO p_UpdateCount FROM t_updatecount;

END ;
$$
    LANGUAGE plpgsql