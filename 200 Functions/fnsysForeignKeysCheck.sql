DROP FUNCTION IF EXISTS S0000V0000.fnsysForeignKeysCheck;
CREATE OR REPLACE FUNCTION S0000V0000.fnsysForeignKeysCheck()
    RETURNS TABLE
            (
                TableName        VARCHAR,
                ColumnName       VARCHAR,
                ID               BIGINT,
                ForeignTableName VARCHAR,
                ForeignKeyID     BIGINT
            )
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
This function checks all foreign keys in the database and returns any bad references that are found
set search_path to default;

select * from fnsysForeignKeysCheck() order by foreigntablename, foreignkeyid

*/
DECLARE
    l_sql    TEXT;
    l_sqlall TEXT;

BEGIN

    DROP TABLE IF EXISTS t_BadForeignKeys;

    CREATE TEMP TABLE t_BadForeignKeys
    (
        TableName        VARCHAR,
        ColumnName       VARCHAR,
        ID               BIGINT,
        ForeignTableName VARCHAR,
        ForeignKeyID     BIGINT
    );

    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE('
insert into t_BadForeignKeys (tablename, columnname, id, foreigntablename, foreignkeyid)
select ''l_tablename'' tablename, ''l_columnname'' columnname, a.id, ''l_foreigntablename'',  a.l_columnname
from l_tablename a
left join l_foreigntablename b on b.id = a.l_columnname
where b.id is null and a.l_columnname is not null;
'
                                                  , 'l_tablename', c.tablename)
                                          , 'l_dictionarycolumnid', c.dictionarycolumnid)
                                  , 'l_columnname', c.columnname)
                          , 'l_foreigntablename', c.foreigntablename)
               , '')
    INTO l_sql
    FROM (
             SELECT LOWER(cc.name) tablename,
                    aa.id::VARCHAR dictionarycolumnid,
                    LOWER(aa.name) columnname,
                    LOWER(bb.name) foreigntablename
             FROM sysdictionarycolumn aa
             JOIN sysdictionarytable bb
                  ON aa.sysDictionaryTableIDForeign = bb.ID
             JOIN sysdictionarytable cc
                  ON cc.id = aa.sysdictionarytableid
             ORDER BY cc.name, aa.name
    ) C;
    l_sqlall := l_sql;
    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE('
insert into t_BadForeignKeys (tablename, columnname, id, foreigntablename, foreignkeyid)
select ''l_tablename'' tablename, ''l_rowcolumnname'' columnname, a.id, c.name, a.l_rowcolumnname
from l_tablename a
left join sysforeignkeycache b on b.rowid = a.l_rowcolumnname and b.sysdictionarytableid=a.l_tablecolumnname
join sysdictionarytable c on c.id=a.l_tablecolumnname
where b.id is null
and a.l_rowcolumnname is not null
-- If there is no translation then it will not be in the sysforeignkeycache
and c.translation is not null;
'
                                                  , 'l_tablename', c.tablename)
                                          , 'l_dictionarycolumnid', c.dictionarycolumnid)
                                  , 'l_rowcolumnname', c.rowcolumnname)
                          , 'l_tablecolumnname', c.tablecolumnname)
               , '')
    INTO l_sql
    FROM (
             SELECT LOWER(cc.name)                                                  tablename,
                    aa.id::VARCHAR                                                  dictionarycolumnid,
                    LOWER(aa.name)                                                  rowcolumnname,
                    LOWER(REPLACE(LOWER(aa.name), 'rowid', 'sysdictionarytableid')) tablecolumnname
             FROM sysdictionarycolumn aa
             JOIN sysdictionarytable cc
                  ON cc.id = aa.sysdictionarytableid
             WHERE LOWER(aa.purpose) = 'multilink'
               AND LOWER(aa.name) LIKE 'rowid%'
             ORDER BY cc.name, aa.name
    ) C;

    l_sqlall := l_sqlall || coalesce(l_sql,'');

    --RAISE NOTICE 'sql %', l_sqlall;

    EXECUTE l_sql;

    RETURN QUERY SELECT * FROM t_BadForeignKeys;

END;
$$ LANGUAGE plpgsql

/*

select * from t_BadForeignKeys a
order by a.foreignkeyid

update actactivity a set rowidperformedfor=null, sysdictionarytableidperformedfor=null
where a.id in (select id from t_badforeignkeys aa where lower(aa.foreigntablename)='crmcontact')

*/
