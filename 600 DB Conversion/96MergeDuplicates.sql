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
This script merges duplicate records
*/
DO
$ManualConv$
    DECLARE
        l_ColumnName varchar; l_execstatements BOOLEAN = FALSE; l_ForeignTableName varchar; l_PrevTableName varchar= ''; l_pSystem varchar; l_MergeKey varchar; l_SQL VARCHAR= ''; l_TableId INT; l_TableName varchar; l_TempString VARCHAR; l_IsTemporal CHAR(1);
        curMergeTables CURSOR FOR
            SELECT t.ID,
                   t.Name,
                   t.MergeKey,
                   c.name,
                   c.tablename,
                   t.isTableTemporal
            FROM aadictionarytable t
            LEFT JOIN aadictionarycolumn c
                      ON c.DictionaryTableIDForeign = t.ID
            WHERE t.SystemName = l_pSystem
              AND t.MergeKey IS NOT NULL
            ORDER BY t.name,
                     c.tablename;
    BEGIN
        /*

        - fix max string length
        - add in audit logging
        - why no record in crmcontact

        */

        l_pSystem := 'NEWBMS';
        l_TempString := '
    DO
$$

declare l_MergeCount int; l_TemporalStartDate timestamp;
BEGIN

DROP TABLE IF EXISTS MergeDuplicate

Create Table MergeDuplicate
(NewID integer not null
,OldID integer not null
,MergeKey varchar
,TableName varchar
,Primary key (TableName, OldID))

l_TemporalStartDate:=now()

';
        l_SQL := l_tempstring;
        perform set_config('search_path', '_Staging,'||current_setting('search_path'), false);

        OPEN curMergeTables;
        FETCH NEXT FROM curMergeTables INTO l_TableId, l_TableName, l_MergeKey, l_ColumnName, l_ForeignTableName, l_isTemporal;
        WHILE l_tableID IS NOT NULL
        LOOP
            IF l_PrevTableName <> l_TableName
            THEN
                l_MergeKey := REPLACE(l_MergeKey, 'xx.', 'a.');
                l_TempString := 'while 1=1' || CHR(13) || CHR(10);
                l_TempString := l_TempString || 'Begin' || CHR(13) || CHR(10);
                l_TempString :=
                                    l_TempString || CHR(9) ||
                                    'insert into MergeDuplicate (Mergekey, NewID, OldID, TableName)' ||
                                    CHR(13) || CHR(10);
                l_TempString :=
                                                    l_TempString || CHR(9) || 'select ' || l_MergeKey ||
                                                    ', min(a.id) NewID, max(a.id) OldID, ''' ||
                                                    l_TableName || ''' from ' || l_tablename || ' a' || CHR(13) ||
                                                    CHR(10);
                IF l_IsTemporal = 'Y'
                THEN
                    l_TempString := l_TempString || CHR(9) ||
                                    'join (select b.id, max(b.temporalstartdate) TemporalStartDate from ' ||
                                    l_TableName ||
                                    ' b group by b.id) b on b.id=a.id and b.TemporalStartDate=a.TemporalStartDate' ||
                                    CHR(13) || CHR(10);
                END IF;
                l_TempString := l_TempString || CHR(9) || 'group by ' || l_MergeKey || ' having count(*) > 1' ||
                                CHR(13) ||
                                CHR(10);
                l_TempString := l_TempString || CHR(9) || 'if l_l_rowcount=0' || CHR(13) || CHR(10);
                l_TempString := l_TempString || CHR(9) || CHR(9) || 'break' || CHR(13) || CHR(10) || CHR(13) ||
                                CHR(10);
                l_sql := l_sql || l_tempstring;
            END IF;
            IF l_ForeignTableName IS NOT NULL
            THEN
                l_TempString := CHR(9) || 'update a a.' || l_columnname || '=b.NewID' || CHR(13) || CHR(10);
                l_TempString := l_TempString || CHR(9) || 'from ' || l_ForeignTableName || ' a' || CHR(13) ||
                                CHR(10);
                l_TempString := l_TempString || CHR(9) || 'join MergeDuplicate b on b.oldid=a.' || l_ColumnName ||
                                ' and b.tablename=''' || l_TableName || '''' || CHR(13) || CHR(10) || CHR(13) ||
                                CHR(10);
                l_sql := l_sql || l_tempstring;
            END IF;
            l_PrevTableName := l_TableName;
            FETCH NEXT FROM curMergeTables INTO l_TableId, l_TableName, l_MergeKey, l_ColumnName, l_ForeignTableName, l_isTemporal;
            IF (l_PrevTableName <> l_TableName
                OR l_l_FETCH_STATUS <> 0)
            THEN
                l_TempString := CHR(9) || 'delete a ' || CHR(13) || CHR(10);
                l_TempString := l_TempString || CHR(9) || 'from ' || l_PrevTableName || ' a ' || CHR(13) || CHR(10);
                l_TempString :=
                                                    l_TempString || CHR(9) || 'Join MergeDuplicate b on b.oldid=a.id' ||
                                                    ' and b.tablename=''' ||
                                                    l_PrevTableName || '''' || CHR(13) || CHR(13) || CHR(10) ||
                                                    CHR(10);
                l_TempString := l_TempString || CHR(9) || 'l_MergeCount:=l_l_rowcount' || CHR(13) || CHR(10) ||
                                CHR(13) ||
                                CHR(10);
                l_TempString := l_TempString || CHR(9) || 'Print ''' || l_PrevTableName ||
                                ''' || CHR(9) || cast(datediff(second, l_TemporalStartDate, getdate()) as varchar) || CHR(9) || ''MergeCount''||CHR(9)||cast(l_MergeCount as varchar)' ||
                                CHR(13) || CHR(10);
                l_TempString := l_TempString || CHR(9) || 'l_TemporalStartDate=getdate()' || CHR(13) || CHR(10);
                l_TempString := l_TempString || 'end' || CHR(13) || CHR(10) || CHR(13) || CHR(10);
                l_sql := l_sql || l_tempstring;
            END IF;
        END LOOP;
        IF l_execstatements = TRUE
        THEN
            EXECUTE l_SQL;
        END IF;
        CLOSE curMergeTables;

		RAISE NOTICE '%', l_SQL;
    END;
$ManualConv$