CREATE OR REPLACE FUNCTION s0000v0000.fnsyscolumndefinitioncreate(p_firstchar varchar, p_columnname varchar, p_purpose varchar, p_schema varchar, p_tablename varchar, p_systemid integer, p_datatype varchar, p_datalength integer, p_decimals integer, p_defaultvalue varchar, p_isalter boolean DEFAULT false) RETURNS varchar
    LANGUAGE plpgsql
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

This function is used by spsysSchemaUpdate.  It forms column defintion statements.

20220902    Blair Kjenner   Initial Code

select fnsyscolumndefinitioncreate(p_firstchar := l_firstchar,
                                 p_columnname := l_curC.columnname,
                                 p_purpose := l_curC.purpose,
                                 p_schema := l_curT.Schema,
                                 p_tablename := l_curT.TableName,
                                 p_systemid := l_systemid,
                                 p_datatype := l_curC.datatype,
                                 p_datalength := l_curC.datalength,
                                 p_decimals := l_curC.decimals,
                                 p_defaultvalue := l_curC.defaultvalue)

*/
DECLARE
    l_columnstatement VARCHAR;
BEGIN
    l_columnstatement := p_firstchar || ' ' || p_columnname;
    IF p_purpose = 'primary key'
    THEN
        -- a direct reference to the schema is required so we can add to table from another schema (e.g. insert into s0101v0000.lpparcel)
        l_columnstatement := l_columnstatement || case when p_isalter then ' type ' else '' end || ' bigint not null default fnsysidcreate(p_table:='''
                              || p_tablename ||
                              ''',p_systemid:=' || p_systemid || ', p_schema:='''
                              || p_schema || ''') ';
    ELSE
        IF p_datatype IN ('varchar', 'varchar2', 'nvarchar', 'char', 'nchar', 'varbinary')
        THEN
            l_columnstatement := l_columnstatement || case when p_isalter then ' type ' else ' ' end || p_datatype;
            if coalesce(p_datalength,0) > 1
            then
                l_columnstatement := l_columnstatement || '(' || p_datalength || ')';
            end if;
        ELSE
            IF p_datatype = 'decimal'
            THEN
                l_columnstatement := l_columnstatement || case when p_isalter then ' type ' else '' end || ' decimal(' || COALESCE(p_datalength, 10) || ',' ||
                                      COALESCE(p_decimals, 0) || ')';
            ELSE
                l_columnstatement := l_columnstatement || case when p_isalter then ' type ' else ' ' end || CASE
                    WHEN p_datatype = 'datetime' THEN 'timestamp'
                    ELSE p_datatype
                    END;
            END IF;
        END IF;
    END IF;

    IF LOWER(p_columnname) = 'temporalstartdate'
    THEN
        l_columnstatement := l_columnstatement || ' default date ''1000-01-01''';
    ELSEIF LOWER(p_columnname) = 'rowstatus'
    THEN
        l_columnstatement := l_columnstatement || ' default ''a''::bpchar';
    ELSEIF LOWER(p_columnname) = 'temporalenddate'
    THEN
        l_columnstatement := l_columnstatement || ' default date ''9999-12-31''';
    ELSEIF LOWER(p_columnname) != 'id' AND p_DefaultValue IS NOT NULL
    THEN
        l_columnstatement := l_columnstatement || ' default ' || p_defaultvalue;
    END IF;
    RETURN l_columnstatement;
END;
$$;