/*This file is part of the 3D Enterprise System Platform. The 3D Enterprise System Platform is free
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
This script copies data from the staging schema into the new schema
*/
SET client_min_messages TO notice;
DO
$$

    DECLARE
        l_DestinationSchema         VARCHAR := 'S0001V0000';
        l_SourceSchema              VARCHAR := '_Staging';
        l_Alphabet                  VARCHAR := 'bcdefghijklmnopqrstuvwxyz';
        l_DataJoinCount             INT     := 0;
        l_DataJoins                 VARCHAR := '';
        l_execstatements            INT     := 0;
        l_FirstColumn               INT     := 1;
        l_ColumnList                VARCHAR;
        l_TempTableStatement        VARCHAR;
        l_KeyAlias                  VARCHAR;
        l_KeyInsert                 VARCHAR := '';
        l_KeyJoinCount              INT     := 0;
        l_KeyJoins                  VARCHAR := '';
        l_NumberOfTabs              INT;
        l_PrevDestTableName         VARCHAR := '';
        l_PrevSourceTableName       VARCHAR := '';
        l_PrevSourceTablePrimaryKey VARCHAR;
        l_PrevWhereClause           VARCHAR;
        l_SelectStatement           VARCHAR;
        l_SQL                       VARCHAR := '';
        l_TableName                 VARCHAR;
        l_TempSQL                   VARCHAR := '';
        l_TempString                VARCHAR;
        l_tString1                  VARCHAR;
        l_tString2                  VARCHAR;
        l_UpdateStatement           VARCHAR;
        l_cur1                      RECORD;
        l_cur2                      RECORD;
        cur1 CURSOR
            FOR SELECT name
                FROM (
                         SELECT DISTINCT t3.name
                         FROM aadictionarycolumn f3
                         JOIN aadictionarycolumn f4
                              ON f4.id = f3.DictionaryColumnIDNew
                         JOIN aadictionarytable t3
                              ON t3.id = f4.DictionaryTableID) a
                WHERE 1 = 2;
        cur2 CURSOR
            FOR SELECT f.name                           DestColumnName,
                       f.datalength                     DestDataLength,
                       f.datatype                       DestDataType,
                       f.decimals                       DestDecimals,
                       f.DefaultValue                   DestDefaultValue,
                       f.IsNullable                     DestIsNullable,
                       f.Purpose                        DestPurpose,
                       f.DictionaryTableID              DestTableId,
                       t.name                           DestTableName,
                       COALESCE(t.IsTableTemporal, 'n') IsTableTemporal,
                       f2.name                          SourceColumnName,
                       f2.source                        SourceConversionSQL,
                       f2.datalength                    SourceDataLength,
                       f2.datatype                      SourceDataType,
                       f2.decimals                      SourceDecimals,
                       t4.name                          SourceForeignTableName,
                       t4.primarykey                    SourceForeignTablePrimaryKey,
                       f2.IsNullable                    SourceIsNullable,
                       f2.Purpose                       SourcePurpose,
                       f2.DictionaryTableID             SourceTableId,
                       t3.SourceTableName               SourceTableName,
                       t3.SourceTablePrimaryKey,
                       t4.PrimaryKey,
                       t3.SourceWhereClause
                FROM aadictionarycolumn f
                JOIN      aadictionarytable t
                          ON t.id = f.DictionaryTableID
                JOIN      (
                              SELECT DISTINCT t3.name               DestTableName,
                                              t4.name               SourceTableName,
                                              t4.id                 SourceTableID,
                                              t4.WhereClause        SourceWhereClause,
                                              t4.PrimaryKey         SourceTablePrimaryKey,
                                              t4.ConversionSequence SourceConversionSequence
                              FROM aadictionarycolumn f3
                              JOIN aadictionarycolumn f4
                                   ON f4.id = f3.DictionaryColumnIDNew
                              JOIN aadictionarytable t3
                                   ON t3.id = f4.DictionaryTableID
                              JOIN aadictionarytable t4
                                   ON t4.id = f3.DictionaryTableID) t3
                          ON t3.DestTableName = t.name
                LEFT JOIN aadictionarycolumn f2
                          ON (f2.DictionaryColumnIDNew = f.ID
                              OR (f2.DictionaryColumnIDNew = -1
                                  AND f2.NormalizedName = f.name))
                              AND f2.DictionaryTableID = t3.SourceTableId
                LEFT JOIN aadictionarytable t4
                          ON t4.id = f2.DictionaryTableIDForeign
--              The following where clause screens out invalid rows caused by dest and source tables names being the same
                WHERE f.DictionaryTableID != f2.DictionaryTableID
--                 AND t.name = 'actActivity'
--                 where t3.DestTableName like '%comdegree'
                ORDER BY t3.SourceConversionSequence, t3.SourceTableName, t.name, f.ColumnSequence;

    BEGIN

        PERFORM SET_CONFIG('search_path',
                          l_DestinationSchema || ',' || l_SourceSchema || ',' || CURRENT_SETTING('search_path'), TRUE);

        l_SQL := '

DO
$ConvertData$

declare l_TimeStamp timestamp; l_UpdateCount int; l_InsertCount int ;
BEGIN

if to_regclass(''_staging.KeyTranslation'') is null
then
    CREATE SEQUENCE _staging.KeyTranslation_id_seq;
    Create Table _staging.KeyTranslation
    (NewID serial not null
    ,OldID varchar
    ,TableName varchar
    ,Primary key (NewID));
    CREATE INDEX IXKeyTranslation ON _staging.KeyTranslation (TableName ASC,OldID ASC);
end if;

l_TimeStamp := now();
';

        OPEN cur1;
        LOOP
            FETCH cur1 INTO l_TableName;
            EXIT WHEN NOT FOUND;

            l_TempSQL := 'Truncate table ' || l_TableName || ';';
            l_sql := l_sql || l_TempSQL || CHR(13) || CHR(10);

        END LOOP;
        CLOSE cur1;


        OPEN cur2;
        FETCH cur2 INTO l_cur2;

        WHILE l_cur2.DestColumnName IS NOT NULL
        LOOP

            IF l_PrevSourceTableName <> l_cur2.SourceTableName OR l_PrevDestTableName <> l_cur2.DestTableName
            THEN
                --                     IF l_cur2.IsTableTemporal <> 'Y'
--                     THEN
--                         l_InsertStatement := 'Identity_insert ' || l_cur2.DestTableName || ' On' || CHR(13) || CHR(10);
--                     ELSE
                l_ColumnList := '';
--                     END IF;
                l_SelectStatement := 'Select ';
                l_UpdateStatement := 'UPDATE ' || l_DestinationSchema || '.' || l_cur2.DestTableName || ' A SET ';
            END IF;
            IF l_FirstColumn = 1
            THEN
                l_FirstColumn := 0;
                l_SelectStatement := l_SelectStatement || CHR(9) || ' ';
            ELSE
                l_ColumnList := l_ColumnList || ', ';
                l_SelectStatement := l_SelectStatement || CHR(9) || CHR(9) || ', ';
                IF l_cur2.SourceColumnName IS NOT NULL AND l_cur2.DestColumnName <> 'ID'
                THEN
                    IF RIGHT(l_UpdateStatement, 4) = 'SET '
                    THEN
                        l_UpdateStatement := l_UpdateStatement || CHR(9);
                    ELSE
                        l_UpdateStatement := l_UpdateStatement || CHR(9) || CHR(9) || ', ';
                    END IF;
                END IF;
            END IF;
            IF l_cur2.SourceConversionSQL IS NOT NULL AND
               NOT (l_cur2.DestPurpose = 'Multilink' AND l_cur2.SourceForeignTableName IS NOT NULL)
            THEN
                l_cur2.SourceColumnName := l_cur2.SourceConversionSQL;
            END IF;
            IF l_cur2.DestColumnName = 'RowStatus'
            THEN
                l_cur2.SourceColumnName := 'coalesce(lower(xx.' || l_cur2.SourceColumnName || '),''a'')';
            END IF;
            IF l_cur2.SourceForeignTableName <> ''
            THEN
                l_DataJoinCount := l_DataJoinCount + 1;
                l_tString1 := CASE
                    WHEN POSITION('xx' IN l_cur2.SourceForeignTablePrimaryKey) > 0 THEN REPLACE(
                            l_cur2.SourceForeignTablePrimaryKey, 'xx',
                            SUBSTRING(l_Alphabet, l_DataJoinCount, 1) ||
                            SUBSTRING(l_Alphabet, l_DataJoinCount, 1))
                    ELSE
                                    SUBSTRING(l_Alphabet, l_DataJoinCount, 1) ||
                                    SUBSTRING(l_Alphabet, l_DataJoinCount, 1) || '.' ||
                                    l_cur2.SourceForeignTablePrimaryKey
                    END;
                l_tString2 := CASE
                    WHEN POSITION('xx' IN l_cur2.SourceColumnName) > 0
                        THEN REPLACE(l_cur2.SourceColumnName, 'xx', 'a')
                    ELSE 'a.' || l_cur2.SourceColumnName
                    END;
                --              I had an issue once of a primary key being duplicated in a foreign key table
--              which caused all the rows to duplicate
                l_DataJoins := l_DataJoins || 'LEFT JOIN LATERAL ( Select * from ' ||
                               COALESCE(l_cur2.SourceForeignTableName, 'ForeignTableNotFound') || ' ' ||
                               SUBSTRING(l_Alphabet, l_DataJoincount, 1) ||
                               SUBSTRING(l_Alphabet, l_DataJoincount, 1) ||
                               ' where ' || l_tString1 || ' = ' || l_tString2 || ' LIMIT 1) ' ||
                               SUBSTRING(l_Alphabet, l_DataJoincount, 1) || ' ON TRUE' || CHR(13) || CHR(10);
            END IF;
            IF (l_cur2.DestPurpose IN ('Reftable', 'Primary key', 'Foreign key')
                OR (l_cur2.DestPurpose = 'Multilink' AND l_cur2.SourceForeignTableName IS NOT NULL))
                AND
               l_cur2.SourceColumnName IS NOT NULL
            THEN
                l_tString2 := COALESCE(CASE
                                           WHEN POSITION('xx' IN l_cur2.SourceColumnName) > 0
                                               THEN REPLACE(l_cur2.SourceColumnName, 'xx', 'a')
                                           ELSE 'a.' || l_cur2.SourceColumnName
                                           END,
                                       'Error - SourceColumnName is null');
                l_KeyJoinCount := l_KeyJoinCount + 1;
                l_TableName := COALESCE(CASE
                                            WHEN l_cur2.SourceForeignTableName IS NOT NULL
                                                THEN l_cur2.SourceForeignTableName
                                            ELSE l_cur2.SourceTableName
                                            END,
                                        'Error - ' || COALESCE(l_cur2.DestPurpose, 'DestPurpose is null') ||
                                        ' Tablename is null');
                l_KeyAlias := SUBSTRING(l_Alphabet, l_KeyJoincount, 1) || SUBSTRING(l_Alphabet, l_KeyJoincount, 1);
                l_KeyInsert := l_KeyInsert || 'Insert into ' || l_SourceSchema || '.' ||
                               'KeyTranslation (OldId, TableName) ' || CHR(13) ||
                               CHR(10);
                l_KeyInsert := l_KeyInsert || 'Select cast(' || l_tString2 || ' as varchar (1000)),''' ||
                               l_TableName || ''' from ' || l_SourceSchema || '.' || l_cur2.SourceTableName || ' a' ||
                               CHR(13) || CHR(10);
                l_KeyInsert := l_KeyInsert ||
                               'where not exists (select 1 from ' || l_SourceSchema || '.' ||
                               'KeyTranslation b where b.oldid = cast(' ||
                               l_tString2 || ' as varchar) and b.tablename=''' || l_TableName ||
                               ''' LIMIT 1) and ' ||
                               l_tString2 || ' is not null group by '|| l_tString2 ||' order by '|| l_tString2 ||';' || CHR(13) || CHR(10) || CHR(13) || CHR(10);
                l_KeyJoins := l_KeyJoins || 'left join ' || l_SourceSchema || '.' || 'KeyTranslation ' || l_KeyAlias ||
                              ' on ' || l_KeyAlias ||
                              '.OldId = cast(' || l_tString2 || ' as varchar)' || ' and ' || l_KeyAlias ||
                              '.tablename=''' || l_TableName || '''' || CHR(13) || CHR(10);
                l_cur2.SourceColumnName := l_KeyAlias || '.Newid + 10000000000000';
            ELSE
                l_cur2.SourceColumnName := CASE
                    WHEN POSITION('xx' IN l_cur2.SourceColumnName) > 0
                        THEN REPLACE(l_cur2.SourceColumnName, 'xx', 'a')
                    ELSE 'a.' || l_cur2.SourceColumnName
                    END;
            END IF;
            IF l_cur2.SourceColumnName IS NOT NULL AND l_cur2.DestColumnName <> 'ID'
            THEN
                l_Updatestatement := l_UpdateStatement || l_cur2.DestColumnName || ' = b.' ||
                                     l_cur2.DestColumnName || CHR(13) || CHR(10);
            END IF;

            l_ColumnList := l_ColumnList || l_cur2.DestColumnName;
            IF l_cur2.SourceColumnName IS NOT NULL AND l_cur2.DestDefaultValue IS NOT NULL
            THEN
                l_TempString := 'coalesce(' || l_cur2.SourceColumnName;
                l_TempString := l_TempString || ',' || CASE
                    WHEN l_cur2.DestDefaultValue IS NOT NULL
                        THEN l_cur2.DestDefaultValue || ')'
                    ELSE 'Null)'
                    END;
            ELSE
                IF l_cur2.SourceColumnName IS NOT NULL AND l_cur2.DestDefaultValue IS NULL
                THEN
                    l_TempString := l_cur2.SourceColumnName;
                ELSE
                    l_TempString :=
                            CASE
                                WHEN l_cur2.DestDefaultValue IS NOT NULL THEN l_cur2.DestDefaultValue
                                ELSE 'Null'
                                END;
                END IF;
            END IF;
            l_NumberOfTabs := 10 - ROUND((LENGTH(l_TempString) + 2) / 4, 0);
            IF l_NumberOfTabs < 1
            THEN
                l_NumberOfTabs := 1;
            END IF;
            l_SelectStatement := l_SelectStatement || l_TempString || LEFT(
                                                            CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) ||
                                                            CHR(9) ||
                                                            CHR(9) ||
                                                            CHR(9) || CHR(9) || CHR(9), l_NumberOfTabs) || '-- ' ||
                                 l_cur2.DestColumnName || CHR(13) || CHR(10);
            l_PrevSourceTableName := l_cur2.SourceTableName;
            l_PrevSourceTablePrimaryKey := l_cur2.SourceTablePrimaryKey;
            l_PrevSourceTableName := l_cur2.SourceTableName;
            l_PrevDestTableName := l_cur2.DestTableName;
            l_PrevWhereClause := l_cur2.SourceWhereClause;

            FETCH cur2 INTO l_cur2;

            IF (l_PrevSourceTableName != COALESCE(l_cur2.SourceTableName, '') OR
                l_PrevDestTableName != COALESCE(l_cur2.DestTableName, ''))
            THEN

                l_TempSQL := l_KeyInsert;
                --              A select 0 into the temp table using the destination table ensures the data types between
--              the temp and new table match
                l_TempSQL := l_TempSQL || 'DROP TABLE IF EXISTS t_' || l_PrevSourceTableName || ';' || CHR(13) ||
                             CHR(10);
                l_TEMPSQL := l_TempSQL || 'CREATE TABLE t_' || l_PrevSourceTableName || ' AS ' || CHR(13) || CHR(10);
                l_TEMPSQL := l_TempSQL || 'SELECT ' || l_ColumnList || ' FROM ' || l_DestinationSchema || '.' ||
                             l_PrevDestTableName || ' LIMIT 0 ;' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || 'Insert into t_' || l_PrevSourceTableName || ' (' || l_ColumnList || ')' ||
                             CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || COALESCE(l_SelectStatement, 'Error - Select Statement was null') ||
                             ' From ' || l_SourceSchema || '.' ||
                             l_PrevSourceTableName || ' a' || CHR(13) || CHR(10);
                --              The following statement includes joins to all the tables.  The only reason I could think
--              that would be necessary is to make data from related tables available for creating the new table
--              I dont need that for BMS
--                 l_TempSQL := l_TempSQL || COALESCE(l_DataJoins, 'Error - DataJoins was null');
                l_TempSQL := l_TempSQL || COALESCE(l_KeyJoins, 'Error - Keyjoins was null') || ';' || CHR(13) ||
                             CHR(10);
                l_SQL := l_SQL || l_TempSQL;

                l_tString1 := CASE
                    WHEN POSITION('xx' IN l_PrevSourceTablePrimaryKey) > 0
                        THEN REPLACE(l_PrevSourceTablePrimaryKey, 'xx', 'a')
                    ELSE 'a.' || l_PrevSourceTablePrimaryKey
                    END;
                l_TempSQL := 'if (select count(*) from ' || l_PrevDestTableName || ' ) > 0 THEN ' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || CHR(9) || COALESCE(l_UpdateStatement, 'Error - UpdateStatement was null');
                l_TempSQL := l_TempSQL || CHR(9) || 'FROM t_' || l_PrevSourceTableName || ' b' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || CHR(9) || 'WHERE a.ID=b.ID;' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || CHR(9) || 'GET DIAGNOSTICS l_UpdateCount = ROW_COUNT;' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || 'ELSE' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || CHR(9) || 'Insert into ' || l_DestinationSchema || '.' || l_PrevDestTableName;
                l_TempSQL := l_TempSQL || CHR(9) || ' (' || l_ColumnList || ')' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || CHR(9) || 'SELECT ' ||
                             COALESCE(l_ColumnList, 'Error - Insert Statement was null') || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || CHR(9) || ' FROM t_' || l_PrevSourceTableName || ' a ;' || CHR(13) || CHR(10);


                --              I eliminated the where key because the primary key should never be null
--                 l_TempSQL := l_TempSQL || CHR(9) || 'where ' || l_tString1 || ' is not null;' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || CHR(9) || 'GET DIAGNOSTICS l_InsertCount = ROW_COUNT;' || CHR(13) ||
                             CHR(10) ||
                             CHR(13) || CHR(10);
                IF COALESCE(l_PrevWhereClause, '') <> ''
                THEN
                    l_TempSQL := l_TempSQL || CHR(9) || 'WHERE ' || REPLACE(l_PrevWhereClause, 'xx', 'a');
                END IF;
                l_TempSQL := l_TempSQL || 'END IF' || ';' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || 'RAISE NOTICE ''%'',  ''' || l_PrevSourceTableName || '''|| CHR(9)||''' ||
                             l_PrevDestTableName ||
                             ''' || CHR(9) || ''UpdateCount''||CHR(9)||cast(coalesce(l_UpdateCount,0) as varchar)|| CHR(9)|| ''InsertCount''||CHR(9)||cast(coalesce(l_insertcount,0) as varchar)' ||
                             ';' || CHR(13) || CHR(10);
                l_TempSQL := l_TempSQL || 'l_TimeStamp:=now(); l_insertcount:=0; l_updatecount:=0;' || CHR(13) ||
                             CHR(10);
                l_TempSQL := l_TempSQL || 'DROP TABLE IF EXISTS t_' || l_PrevSourceTableName || ';' || CHR(13) ||
                             CHR(10);
                l_FirstColumn := 1;
                l_DataJoinCount := 0;
                l_DataJoins := '';
                l_KeyJoins := '';
                l_KeyInsert := '';
                l_KeyJoinCount := 0;
                l_SQL := l_SQL || l_TempSQL;
            END IF;
        END LOOP;
        IF l_execstatements = 1
        THEN
            L_SQL := l_SQL || 'EXEC (l_TempSQL);' || CHR(13) || CHR(10);
        END IF;
        CLOSE cur2;
        L_SQL := L_SQL || 'END;' || CHR(13) || CHR(10) || '$ConvertData$' || CHR(13) || CHR(10);
        --RAISE NOTICE '%', l_SQL;
        EXECUTE l_SQL;

        SET SEARCH_PATH TO DEFAULT;
    END ;

$$ LANGUAGE plpgsql;
