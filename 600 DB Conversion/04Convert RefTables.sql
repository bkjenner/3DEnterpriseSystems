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
This script loads data from the reference table (representing a list of tables)
  into the new schema
*/

DO
$$

    DECLARE
        l_DestinationSchema   VARCHAR      := 'S0001V0000';
        l_SourceSchema        varchar := '_Staging';
        l_Alphabet            varchar  := 'bcdefghijklmnopqrstuvwxyz';
        l_FirstColumn         BOOLEAN      := TRUE;
        l_PrevDestTableName   varchar := '';
        l_PrevSourceTableName varchar;
        l_InsertStatement     VARCHAR      := '';
        l_SelectStatement     VARCHAR;
        l_KeyJoinCount        INT          := 0;
        l_KeyJoins            VARCHAR      := '';
        l_KeyInsert           VARCHAR      := '';
        l_KeyAlias            varchar;
        l_NumberOfTabs        INT;
        l_SQL                 VARCHAR      := '';
        l_cur1                RECORD;
        l_cur2                RECORD;
        Cur1 CURSOR
            FOR SELECT f.name                      DestColumnName,
                       f.datalength                DestDataLength,
                       f.datatype                  DestDataType,
                       f.decimals                  DestDecimals,
                       f.DefaultValue              DestDefaultValue,
                       f.IsNullable                DestIsNullable,
                       f.Purpose                   DestPurpose,
                       f.DictionaryTableID         DestTableId,
                       t.name                      DestTableName,
                       t.IsTableTemporal           IsTableTemporal,
                       f2.name                     SourceColumnName,
                       SUBSTRING(t.source, 4, 999) SourceTableName
                FROM aadictionarycolumn f
                JOIN      aadictionarytable t
                          ON t.id = f.DictionaryTableID
                LEFT JOIN LATERAL (SELECT *
                                   FROM aadictionarycolumn sf
                                   WHERE sf.TableName LIKE '%ReferenceFields%'
                                     AND sf.name = CASE
                                                       WHEN f.source = 'Val1'         THEN 'Description'
                                                       WHEN f.source = 'Val2'         THEN 'Val2'
                                                       WHEN f.name = 'RowStatus'      THEN 'RecordStatus'
                                                       WHEN f.name = 'DisplaySequence' THEN 'Seqno'
                                                                                      ELSE f.name
                                                       END
                                   LIMIT 1) f2
                          ON TRUE
                    --where t.Source like 'SI-BILLING STATUS%'
                WHERE t.source LIKE 'SI-%'
                  AND f.name <> 'syschangehistoryid'
                ORDER BY t.name, f.ColumnSequence;
        cur2 CURSOR
            FOR SELECT f.name                             DestColumnName,
                       f.datalength                       DestDataLength,
                       f.datatype                         DestDataType,
                       f.decimals                         DestDecimals,
                       f.DefaultValue                     DestDefaultValue,
                       f.IsNullable                       DestIsNullable,
                       f.Purpose                          DestPurpose,
                       f.DictionaryTableID                DestTableId,
                       t.name                             DestTableName,
                       t.IsTableTemporal                  IsTableTemporal,
                       f2.SourceColumnName,
                       TRIM(SUBSTRING(t.source, 21, 999)) SourceTableName
                FROM aadictionarycolumn f
                JOIN aadictionarytable t
                     ON t.id = f.DictionaryTableID
                JOIN LATERAL (SELECT CASE
                                         WHEN f.name = 'DisplaySequence' THEN 'Sequence'
                                         WHEN f.name = 'Name'           THEN 'Description'
                                         WHEN f.name = 'DisplaySequence' THEN 'Sequence'
                                         WHEN f.name = 'RowStatus'      THEN 'RecordStatus'
                                                                        ELSE f.name
                                         END SourceColumnName) f2
                     ON TRUE
                    --where t.Source like 'SI-ATTRIBUTE TYPES%'
                WHERE t.source LIKE 'aareferencefields%'
                  AND f.name <> 'syschangehistoryid'
                ORDER BY t.name, f.ColumnSequence;

    BEGIN

        PERFORM SET_CONFIG('search_path',
                          l_DestinationSchema || ',' || l_SourceSchema || ',' || CURRENT_SETTING('search_path'), TRUE);

        INSERT INTO KeyTranslation (OldId, TableName)
        SELECT DISTINCT a.id, 'ReferenceFields'
        --SELECT DISTINCT a.id, b.description
        FROM referencefields a
        JOIN referencetables b
             ON b.id = a.referencetableid
        WHERE NOT EXISTS(SELECT 1
                         FROM KeyTranslation aa
                         WHERE aa.oldid = a.id::VARCHAR
                           AND aa.tablename = 'ReferenceFields'
                           -- AND aa.tablename = b.description
                         LIMIT 1);

        INSERT INTO KeyTranslation (OldId, TableName)
        SELECT DISTINCT a.id, a.type
        FROM aareferencefields a
        WHERE ID IS NOT NULL
          AND NOT EXISTS(SELECT 1
                         FROM KeyTranslation aa
                         WHERE aa.oldid = a.id::VARCHAR
                           AND aa.tablename = a.type
                         LIMIT 1);

        OPEN Cur1;
        FETCH cur1 INTO l_cur1;

        WHILE l_cur1.DestColumnName IS NOT NULL
        LOOP

            IF l_PrevDestTableName <> l_cur1.DestTableName
            THEN
                l_InsertStatement := CHR(13) || CHR(10) || 'Insert into ' || l_DestinationSchema || '.' ||
                                     l_cur1.DestTableName || ' (';
                l_SelectStatement := 'Select ';
            END IF;

            IF l_FirstColumn = TRUE
            THEN
                l_FirstColumn := FALSE;
                l_SelectStatement := l_SelectStatement || CHR(9) || ' ';
            ELSE
                l_InsertStatement := l_InsertStatement || ', ';
                l_SelectStatement := l_SelectStatement || CHR(9) || CHR(9) || ', ';
            END IF;

            IF l_Cur1.DestPurpose IN ('Primary key')
            THEN
                l_KeyJoinCount := l_KeyJoinCount + 1;
                l_KeyAlias := SUBSTRING(l_Alphabet, l_KeyJoincount, 1) || SUBSTRING(l_Alphabet, l_KeyJoincount, 1);
                l_KeyJoins := l_KeyJoins || 'join KeyTranslation ' || l_KeyAlias || ' on ' || l_KeyAlias ||
                              '.OldId = CAST(a.' || l_Cur1.SourceColumnName || ' as varchar)' ||
                              ' and ' || l_KeyAlias || '.TableName = ''ReferenceFields''' || CHR(13) || CHR(10);
                l_Cur1.SourceColumnName := l_KeyAlias || '.Newid + 10000000000000';
            ELSE
                l_Cur1.SourceColumnName := COALESCE('a.' || l_Cur1.SourceColumnName, 'null');
            END IF;

            l_InsertStatement := l_InsertStatement || l_Cur1.DestColumnName;

            l_NumberOfTabs := 10 - ROUND((LENGTH(l_Cur1.SourceColumnName) + 2) / 4, 0);
            IF l_NumberOfTabs < 1
            THEN
                l_NumberOfTabs := 1;
            END IF;

            l_SelectStatement := l_SelectStatement || l_Cur1.SourceColumnName || LEFT(
                                        CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) ||
                                        CHR(9) || CHR(9) || CHR(9), l_NumberOfTabs) || '-- ' || l_Cur1.DestColumnName ||
                                 CHR(13) || CHR(10);

            l_PrevDestTableName := l_Cur1.DestTableName;
            l_PrevSourceTableName := l_Cur1.SourceTableName;
            FETCH cur1 INTO l_cur1;

            IF (l_PrevSourceTableName != COALESCE(l_cur1.SourceTableName, '') OR
                l_PrevDestTableName != COALESCE(l_cur1.DestTableName, ''))
            THEN
                l_sql := l_sql || l_KeyInsert;
                l_sql := l_sql || CHR(13) || CHR(10) || 'Truncate table ' || l_DestinationSchema || '.' ||
                         l_PrevDestTableName || ';' || CHR(13) || CHR(10);
                l_sql := l_sql || COALESCE(l_InsertStatement, ' NULL l_InsertStatement') || ')' || CHR(13) || CHR(10);
                l_sql := l_sql || COALESCE(l_SelectStatement, ' NULL l_SelecttStatement') ||
                         'From referencefields a' || CHR(13) || CHR(10);
                l_sql := l_sql || l_KeyJoins;
                l_sql := l_sql || 'join referencetables z on z.id=a.referencetableid' || CHR(13) || CHR(10);
                l_sql := l_sql || 'where z.shortcode=''' || l_PrevSourceTableName || ''';' || CHR(13) || CHR(10);
                l_FirstColumn := TRUE;
                l_KeyJoins := '';
                l_KeyInsert := '';
                l_KeyJoinCount := 0;

            END IF;
        END LOOP;

        CLOSE Cur1;

        OPEN cur2;

        FETCH cur2 INTO l_cur2;

        WHILE l_cur2.DestColumnName IS NOT NULL
        LOOP

            IF l_PrevDestTableName <> l_cur2.DestTableName
            THEN
                l_InsertStatement := CHR(13) || CHR(10) || 'Insert into ' || l_DestinationSchema || '.' ||
                                     l_cur2.DestTableName || ' (';
                l_SelectStatement := 'Select ';
            END IF;

            IF l_FirstColumn = TRUE
            THEN
                l_FirstColumn := FALSE;
                l_SelectStatement := l_SelectStatement || CHR(9) || ' ';
            ELSE
                l_InsertStatement := l_InsertStatement || ', ';
                l_SelectStatement := l_SelectStatement || CHR(9) || CHR(9) || ', ';
            END IF;

            --             IF l_cur2.DestPurpose IN ('Primary key')
--             THEN
--                 l_KeyJoinCount := l_KeyJoinCount + 1;
--                 l_KeyAlias := SUBSTRING(l_Alphabet, l_KeyJoincount, 1) || SUBSTRING(l_Alphabet, l_KeyJoincount, 1);
--                 l_KeyJoins := l_KeyJoins || 'join KeyTranslation ' || l_KeyAlias || ' on ' || l_KeyAlias ||
--                               '.OldId = CAST(a.' || l_Cur2.SourceColumnName || ' as varchar)' ||
--                               ' and ' || l_KeyAlias || '.TableName = ''aaReferenceFields''' || CHR(13) || CHR(10);
--                 l_cur2.SourceColumnName := l_KeyAlias || '.Newid';
--             ELSE
            IF l_cur2.DestPurpose IN ('Primary key')
            THEN
                l_cur2.SourceColumnName := 'fnsysIDCreate(p_Systemid:=0, p_Rowid:=a.' || l_cur2.SourceColumnName || ')';
            ELSE
                l_cur2.SourceColumnName := COALESCE('a.' || l_cur2.SourceColumnName, 'null');
            END IF;
--             END IF;

            l_InsertStatement := l_InsertStatement || l_cur2.DestColumnName;

            l_NumberOfTabs := 10 - ROUND((LENGTH(l_cur2.SourceColumnName) + 2) / 4, 0);
            IF l_NumberOfTabs < 1
            THEN
                l_NumberOfTabs := 1;
            END IF;

            l_SelectStatement := l_SelectStatement || l_cur2.SourceColumnName || LEFT(
                                        CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) || CHR(9) ||
                                        CHR(9) || CHR(9) || CHR(9), l_NumberOfTabs) || '-- ' || l_cur2.DestColumnName ||
                                 CHR(13) || CHR(10);

            l_PrevDestTableName := l_cur2.DestTableName;
            l_PrevSourceTableName := l_cur2.SourceTableName;

            FETCH cur2 INTO l_cur2;

            IF (l_PrevSourceTableName != COALESCE(l_cur2.SourceTableName, '') OR
                l_PrevDestTableName != COALESCE(l_cur2.DestTableName, ''))
            THEN
                l_sql := l_sql || l_KeyInsert;
                l_sql := l_sql || CHR(13) || CHR(10) || 'Truncate table ' || l_DestinationSchema || '.' ||
                         l_PrevDestTableName || ';' || CHR(13) || CHR(10);
                l_sql := l_sql || l_InsertStatement || ')' || CHR(13) || CHR(10);
                l_sql := l_sql || COALESCE(l_SelectStatement, '') || 'From aaReferenceFields a' || CHR(13) ||
                         CHR(10);
--                l_sql := l_sql || l_KeyJoins;
                l_sql := l_sql || 'where a.type=''' || l_PrevSourceTableName || ''';' || CHR(13) || CHR(10);
                l_FirstColumn := TRUE;
                l_KeyJoins := '';
                l_KeyInsert := '';
                l_KeyJoinCount := 0;
            END IF;
        END LOOP;

        --RAISE NOTICE '%', l_sql;
        EXECUTE l_sql;

        CLOSE cur2;

        SET SEARCH_PATH TO DEFAULT;

    END;

$$ LANGUAGE plpgsql;

/*
Select bb.Newid ID
, a.Description Description
, a.SeqNo DisplaySequence
, a.RowStatus RowStatus
From bms.referencefields a
join KeyTranslation bb on bb.OldId = a.ID
join bms.referencetables z on z.id=a.referencetableid
where z.shortcode='BILLING STATUS'

select * from NEWBMS.actBillingStatus


*/