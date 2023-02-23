CREATE OR REPLACE PROCEDURE S0000V0000.spsysViewCreate(
    p_SchemaName VARCHAR DEFAULT NULL, p_debug BOOLEAN DEFAULT FALSE)
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
This procedure generates views for the selected schema based on the data dictionary

CHANGE LOG
20221010 Blair Kjenner	Initial Code

SAMPLE CALL

PARAMETERS

p_schemaname - Defaults to current schema.
p_debug - Defaults to FALSE. Creates view output to console.

call spsysViewCreate('s0101v0000',TRUE)

TODO Check for duplicate labels.  A duplicate label can be caused on foreign keys if the rowid column is the same as the name column
 */
DECLARE
    l_Alphabet         VARCHAR := 'bcdefghijklmnopqrstuvwxyz';
    l_FirstField       BOOLEAN;
    l_ForeignKeyString VARCHAR;
    l_JoinCount        INT;
    l_Joins            VARCHAR;
    l_alias            CHAR;
    l_tString          VARCHAR;
    l_SelectViewsSQL   VARCHAR := '';
    l_sql              VARCHAR := '';
    l_curC             RECORD;
    l_curT             RECORD;
    l_searchpath       VARCHAR;
BEGIN

    l_searchpath := CURRENT_SETTING('search_path');
    p_schemaname := LOWER(COALESCE(p_schemaname, CURRENT_SCHEMA()));

    PERFORM SET_CONFIG('search_path', p_schemaname || ',' || CURRENT_SETTING('search_path'), TRUE);

    DROP TABLE IF EXISTS t_ColumnSort;
    CREATE TEMP TABLE t_ColumnSort
    (
        Position INT,
        Purpose  VARCHAR
    );
    INSERT INTO t_ColumnSort
    SELECT 1, 'primary key'
    UNION ALL
    SELECT 2, 'foreign key'
    UNION ALL
    SELECT 3, 'multilink'
    UNION ALL
    SELECT 4, 'data'
    UNION ALL
    SELECT 4, 'virtual field'
    UNION ALL
    SELECT 5, 'temporal'
    UNION ALL
    SELECT 6, 'audit'
    UNION ALL
    SELECT 7, 'system';

    l_SQL := '
DO
$Body$
BEGIN
';

    FOR l_curT IN SELECT LOWER(t.name)      TableName,
                         t.id               sysDictionaryTableID,
                         t.isTableTemporal  isTableTemporal,
                         EXISTS(SELECT 1
                                FROM sysdictionarycolumn aa
                                WHERE aa.sysdictionarytableid = t.id
                                  AND LOWER(aa.name) = 'rowstatus'
                                LIMIT 1) AS HasRowStatus
                  FROM sysdictionarytable t
                  ORDER BY t.name
    LOOP
        l_SelectViewsSQL := l_SelectViewsSQL || 'Select ''vw' || l_curT.TableName ||
                            '''; Select * from vw' || l_curT.TableName || ';' || CHR(13) || CHR(10);

        l_sql := l_sql || REPLACE(REPLACE('
DROP VIEW IF EXISTS l_schemaname.vwl_tablename cascade;
CREATE OR REPLACE VIEW l_schemaname.vwl_tablename
AS
SELECT
', 'l_tablename', l_curT.TableName), 'l_schemaname', p_schemaname);

        l_FirstField := TRUE;
        l_JoinCount := 0;
        l_Joins := '';
        l_ForeignKeyString := '';

        FOR l_curC IN SELECT f.name                              AS ColumnName,
                             CASE
                                 WHEN lower(f.Purpose) IN ('foreign key', 'multilink') THEN REPLACE(f.label, ' ', '')
                                 ELSE f.name
                                 END                             AS Label,
                             LOWER(f.Purpose)                    AS Purpose,
                             t2.Name                             AS ForeignTable,
                             t2.Translation                      AS Translation,
                             t2.name                             AS ForeignNormalizedName,
                             COALESCE(t2.IsTableTemporal, FALSE) AS IsTableTemporal,
                             LOWER(f.datatype)                   AS DataType
                      FROM sysdictionarycolumn f
                      LEFT JOIN sysdictionarytable t2
                                ON t2.id = f.sysDictionaryTableIdForeign
                      LEFT JOIN t_ColumnSort cs
                                ON LOWER(cs.Purpose) = LOWER(f.Purpose)
                      WHERE f.sysdictionarytableid = l_curT.sysdictionarytableid
                      ORDER BY cs.Position, f.ID
        LOOP
            IF l_curC.ForeignTable <> '' AND l_curC.translation IS NOT NULL AND l_curC.purpose != 'multilink'
            THEN
                l_JoinCount := l_JoinCount + 1;
                l_Alias := SUBSTRING(l_Alphabet, l_joincount, 1);
                l_Joins := l_Joins || LOWER('
left join ' || COALESCE(l_curC.ForeignTable, 'ForeignTableNotFound') ||
                                            ' ' ||
                                            l_Alias || ' on ' ||
                                            l_Alias || '.id' ||
                                            ' = ' || CASE
                                                WHEN POSITION('a.' IN COALESCE(l_curC.ColumnName, 'SourceNotFound')) = 0
                                                    THEN 'a.'
                                                ELSE ''
                                                END || l_curC.ColumnName ||
                                            CASE WHEN l_curC.IsTableTemporal = TRUE THEN
                                                             ' and ' || l_Alias ||
                                                             '.TemporalEndDate = ''9999-12-31'''
                                                 ELSE ''
                                                 END);
            END IF;
            --             IF l_curC.purpose != 'audit'
--             THEN
            IF l_FirstField = TRUE AND l_curC.purpose NOT IN ('foreign key', 'audit', 'multilink')
            THEN
                l_FirstField := FALSE;
                l_tString := CHR(9) || ' ';
            ELSE
                L_TSTRING := CHR(9) || ', ';
            END IF;
--             END IF;
            IF l_curC.purpose IN ('primary key', 'audit', 'data') AND l_curC.DataType = 'bigint'
            THEN
                l_tString := l_tString || 'fnsysIDView(a.' || l_curC.ColumnName || ') as ';
            ELSE
                IF POSITION('a.' IN l_curC.ColumnName) > 0
                THEN
                    l_tString := l_tString || l_curC.ColumnName || ' as ';
                ELSE
                    L_TSTRING := l_tString || 'a.' || l_curC.ColumnName || '' || ' as ';
                END IF;
            END IF;
            IF l_curC.Purpose = 'primary key'
            THEN
                l_tString := l_tString || 'UID';
            END IF;
            IF l_curC.Purpose IN ('foreign key', 'multilink', 'system', 'audit')
            THEN
                l_tString := l_tString || l_curC.ColumnName;
            END IF;
            IF l_curC.purpose IN ('data', 'virtual field', 'temporal')
            THEN
                l_tString := l_tString || l_curC.Label || '';
            END IF;
            IF l_curC.purpose = 'reference table'
            THEN
                l_tString := l_tString || l_curC.Label || 'ID';
            END IF;
            IF l_curC.purpose IN ('foreign key', 'audit', 'multilink')
            THEN
                l_ForeignKeyString := l_ForeignKeyString || l_tString || CHR(13) || CHR(10);
            ELSE
                L_SQL := l_sql || l_tString || CHR(13) || CHR(10);
            END IF;
            IF l_curC.Purpose = 'multilink' AND l_curC.ColumnName ILIKE 'rowid%'
            THEN
                IF l_FirstField = TRUE
                THEN
                    l_FirstField := FALSE;
                    l_tString := CHR(9) || ' ';
                ELSE
                    L_TSTRING := CHR(9) || ', ';
                END IF;
                l_JoinCount := l_JoinCount + 1;
                l_Alias := SUBSTRING(l_Alphabet, l_joincount, 1);
                l_Joins := l_Joins || LOWER('
left join sysforeignkeycache ' ||
                                            l_Alias || ' on ' ||
                                            l_Alias || '.rowid = ' ||
                                            CASE WHEN POSITION('a.' IN COALESCE(l_curC.ColumnName, 'SourceNotFound')) = 0
                                                     THEN 'a.'
                                                 ELSE ''
                                                 END || l_curC.ColumnName ||
                                            ' and ' || l_Alias || '.sysdictionarytableid = ' ||
                                            CASE WHEN POSITION('a.' IN COALESCE(l_curC.ColumnName, 'SourceNotFound')) = 0
                                                     THEN 'a.'
                                                 ELSE ''
                                                 END ||
                                            REPLACE(LOWER(l_curC.ColumnName), 'rowid', 'sysdictionarytableid'));
                l_tString := l_tString || 'coalesce(' || l_alias || '.translation,fnsysGetTranslation(a.' ||
                             REPLACE(LOWER(l_curC.ColumnName), 'rowid', 'sysdictionarytableid') ||
                             ', a.' ||
                             l_curC.ColumnName || ', ''' || p_schemaname || ''')) || '' ('' || fnsysIDView(a.' ||
                             LOWER(l_curC.ColumnName) ||
                             ') || '')'' as ';
                l_sql := l_sql || l_tString || l_curC.Label || CHR(13) || CHR(10);
            ELSEIF l_curC.ForeignTable <> '' AND l_curC.purpose != 'multilink'
            THEN
                IF l_FirstField = TRUE
                THEN
                    l_FirstField := FALSE;
                    l_tString := CHR(9) || ' ';
                ELSE
                    L_TSTRING := CHR(9) || ', ';
                END IF;
                IF l_curC.translation IS NULL
                THEN
                    l_tString := l_tString || ' fnsysIDView(a.' || LOWER(l_curC.ColumnName) || ') as ';
                ELSE
                    IF POSITION('x.' IN l_curC.Translation) > 0
                    THEN
                        l_tString := l_tString || '(' ||
                                     REPLACE(l_curC.Translation, 'x.', l_Alias || '.') || ')';
                    ELSE
                        L_TSTRING := l_tString || 'cast(' || l_Alias || '.' || COALESCE(l_curC.Translation, 'id') ||
                                     ' as varchar)';
                    END IF;
                    l_tString := l_tString || ' || '' ('' || fnsysIDView(a.' || LOWER(l_curC.ColumnName) ||
                                 ') || '')'' as ';
                END IF;
                IF l_curC.Label IS NOT NULL
                THEN
                    l_tString := l_tString || '' || CASE
                        WHEN LOWER(RIGHT(l_curC.Label, 2)) = 'id'
                            THEN LEFT(l_curC.Label,
                                      LENGTH(l_curC.Label) - 2)
                        ELSE l_curC.Label
                        END;
                ELSE
                    L_TSTRING := l_tString || '' || l_curC.ForeignNormalizedName;
                END IF;
                l_sql := l_sql || l_tString || CHR(13) || CHR(10);
            END IF;

        END LOOP;
        l_sql := l_sql || l_ForeignKeyString;
        l_sql := l_sql || CHR(9) || ', a.ID as ID' || CHR(13) || CHR(10);

        -- The rowstatus and systemname are added to the view and are used by the fnsysMasterDataQuery function
        l_sql := l_sql || case when l_curT.HasRowStatus then '' else chr(9) || ', ''a'' rowstatus'|| CHR(13) || CHR(10) end ;
        l_Alias := SUBSTRING(l_Alphabet, l_joincount+1, 1);
        l_sql := l_sql || chr(9) || ', ' || l_alias || '.name systemname' || CHR(13) || CHR(10);
        l_Joins := l_Joins || LOWER('
left join exsystem ' || l_alias || ' on fnsysidview(a.id,''s'') = '||l_alias||'.id');

        l_sql := l_sql || 'FROM ' || l_curT.TableName || ' as a';
        l_sql := l_sql || l_Joins || ';' || CHR(13) || CHR(10);
    END LOOP;

    l_SQL := l_SQL || '
END; $Body$' || CHR(13) || CHR(10);
    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;
    EXECUTE l_sql;

    PERFORM SET_CONFIG('search_path', l_searchpath, TRUE);

END
$$
    LANGUAGE plpgsql;