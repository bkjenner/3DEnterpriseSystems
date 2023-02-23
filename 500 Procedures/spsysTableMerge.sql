CREATE OR REPLACE PROCEDURE S0000V0000.spsysTableMerge(p_schemadestination VARCHAR,
                                                       p_schemasource VARCHAR,
                                                       p_tablename VARCHAR,
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

CHANGE LOG
20220902 Blair Kjenner	Initial Code

PARAMETERS

p_debug BOOLEAN             - DEFAULT FALSE - causes SQL script to be output

SAMPLE CALL

call spsysTableMerge('s0111v0000','s0103v0000','actactivity',true);

TODO  The merge should only be taking records that were produced by the system we are exporting from
This could cause an issue if I am exporting from a system that has added a column that the governing
system doesnt have.

*/
DECLARE
    e_Context           TEXT;
    e_Msg               TEXT;
    e_State             TEXT;
    l_Error             RECORD;
    l_SQL               VARCHAR := '';
    l_istabletemporal   BOOLEAN;
    l_ColumnList        VARCHAR;
    l_UpdateList        VARCHAR;
    l_CompareList       VARCHAR;
    l_currentsearchpath VARCHAR;
    l_recordsupdated    INT;
BEGIN
    l_currentsearchpath := CURRENT_SETTING('search_path');

    p_schemadestination := LOWER(p_schemadestination);
    p_schemasource := LOWER(p_schemasource);
    p_tablename := LOWER(p_tablename);

    SELECT STRING_AGG('a.' || a.Column_name, ', ')                   ColumnList,
           STRING_AGG(a.Column_name || '=b.' || a.Column_name, ', ') UpdateList
    INTO l_ColumnList, l_UpdateList
    FROM INFORMATION_SCHEMA.COLUMNS a
    JOIN INFORMATION_SCHEMA.COLUMNS b
         ON b.table_name = a.table_name AND b.table_schema = p_schemasource AND a.column_name = b.column_name
    WHERE a.TABLE_NAME = p_tablename
      AND a.TABLE_SCHEMA = p_schemadestination
      AND a.COLUMN_NAME NOT IN
          ('id', 'syschangehistoryid');

    SELECT STRING_AGG('a.' || a.Column_name, ', ') CompareList
    INTO l_CompareList
    FROM INFORMATION_SCHEMA.COLUMNS a
    JOIN INFORMATION_SCHEMA.COLUMNS b
         ON b.table_name = a.table_name AND b.table_schema = p_schemasource AND a.column_name = b.column_name
    WHERE a.TABLE_NAME = p_tablename
      AND a.TABLE_SCHEMA = p_schemadestination
      AND a.COLUMN_NAME NOT IN
          ('id', 'syschangehistoryid')
      AND a.data_type NOT IN ('json', 'jsonb');

    l_istabletemporal := EXISTS(SELECT
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE table_name = p_tablename
                                  AND column_name = 'temporalenddate');

    l_SQL := '
DO
$SUB$
BEGIN

    -- This creates a temp table with the records that will be
    -- exported.  It does not export foreign key reference
    -- records because they contain limited data
    drop table if exists t_recordstocheck;
    create temp table t_recordstocheck as
    select a.id, l_columnlist
    from l_schemasource.l_tablename a
    where not exists(select
                 from l_schemasource.syschangehistoryrow aa
                 join l_schemasource.sysdictionarytable b on b.name = ''l_tablename''
                 where aa.sysdictionarytableidappliesto = b.id
                   and aa.rowidappliesto = a.id
                   and aa.operationtype = ''fkinsert'');

    drop table if exists t_recordstoupdate;
    create temp table t_recordstoupdate
    as
    select a.id l_temporalcolumn from
        (select a.id, l_comparelist
        from l_schemadestination.l_tablename a
        join t_recordstocheck b on b.id=a.id l_temporalcondition
        union all
        select a.id, l_comparelist
        from t_recordstocheck a) a
        group by a.id, l_comparelist
        having count(*)=1;

    delete from t_recordstocheck a
    where not exists (select from t_recordstoupdate b where b.id=a.id l_temporalcondition);

    update l_schemadestination.l_tablename a set l_updatelist
    from t_recordstocheck b
    where a.id=b.id l_temporalcondition;

    insert into l_schemadestination.l_tablename (id, l_columnlist)
    select a.id, l_a.columnlist from t_recordstocheck a
    left join l_schemadestination.l_tablename b on b.id=a.id l_temporalcondition
    where b.id is null;

END;
$SUB$ LANGUAGE  plpgsql;
';
    -- Now we are going to substitute all our 'l_' variables with data that we collected
    l_SQL :=
            REPLACE(
                REPLACE(
                    REPLACE(
                            REPLACE(
                                    REPLACE(
                                            REPLACE(
                                                    REPLACE(
                                                            REPLACE(
                                                                    REPLACE(
                                                                            REPLACE(l_SQL,
                                                                                    'l_tablename', p_tablename)
                                                                        , 'l_a.columnlist', l_columnlist)
                                                                , 'l_comparelist', l_comparelist)
                                                        , 'l_columnlist', REPLACE(l_columnlist, 'a.', ''))
                                                , 'l_updatelist', l_updatelist)
                                        , 'l_schemadestination', p_schemadestination)
                                , 'l_schemasource', p_schemasource)
                        , 'l_temporalcondition', CASE WHEN l_istabletemporal THEN
                                                          ' AND a.temporalenddate=b.temporalenddate'
                                                      ELSE ''
                                                      END)
                , 'l_temporalcolumn', CASE WHEN l_istabletemporal THEN ', a.temporalenddate'
                                           ELSE ''
                                           END)
    , 'l_temporal', l_istabletemporal::varchar);

    IF p_DEBUG = TRUE
    THEN
       RAISE NOTICE '%', l_sql;
    END IF;
    IF L_SQL IS NOT NULL
    THEN
        EXECUTE (l_SQL);
    END IF;
    SELECT COUNT(*) INTO l_recordsupdated FROM t_recordstoupdate;
    IF l_recordsupdated > 0
    THEN
        RAISE NOTICE '% records updated in %.%',l_recordsupdated, p_schemasource, p_tablename;
    END IF;
    --RAISE NOTICE '%', l_sql;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_State = RETURNED_SQLSTATE,
            e_Msg = MESSAGE_TEXT,
            e_Context = PG_EXCEPTION_CONTEXT;
        l_Error := fnsysError(e_State, e_Msg, e_Context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', l_Error.Message;
        ELSE
            IF l_error.IsExceptionRaised = TRUE
            THEN
                RAISE EXCEPTION '%', l_Error.Message;
            ELSE
                RAISE NOTICE '%', l_Error.Message ;
            END IF;
        END IF;
END ;
$$
    LANGUAGE plpgsql