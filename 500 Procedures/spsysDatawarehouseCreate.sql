CREATE OR REPLACE PROCEDURE S0000V0000.spsysDatawarehouseCreate(p_schemalist VARCHAR,
                                                                INOUT p_schemadatawarehouse VARCHAR DEFAULT NULL)
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
This procedure will create or update a data warehouse. It is passed a list of
schemas to aggregate.  If the p_schemadatawarehouse is null, the procedure
will be assume it is creating a data warehouse otherwise it is assummed
it is updating the data warehouse schema that was passed.

if create then
 - call create data warehouse schema
 - create new data dictionary by starting with the last data dictionary
   and continually overlaying until the first.  It is done in this order
   so the earlier data dictionaries override the later ones
 else (merge)
 - copy dictionarytable/dictionarycolumn into the merged database
 - the update schema to align with the data dictionary
end if
 - call a merge table utility for each table in system 2 to copy data from system 2 into the
   merged database where the id does not exist
 - create the tables in the data warehouse according to the data dictionaries
 - for each schema that is passed (in reverse order)
        merge the tables from the source schemas into the dw schema

CHANGE LOG
20220902 Blair Kjenner	Initial Code

PARAMETERS
p_schemalist          - This list needs to be in the order of the main schema to secondary
                        schemas to tertiary schemas
p_schemadatawarehouse - Schema name of the datawarehouse.  If it is null, this assumed
                        to be a creation of a datawarehouse and not an update

SAMPLE CALL
DO
$test$
    DECLARE
        p_schemadatawarehouse VARCHAR;
    BEGIN
        -- The first call p_schemadatawarehouse is null so it creates the DW.  p_schemadatawarehouse is an INOUT parameter
-- so it will assign it to l_schemadatawarehouse and use it in the second call as an update.
        CALL spsysDatawarehouseCreate(p_schemalist := 's0001v0000, s0005v0000, s0006v0000, s0007v0000',
                                      p_schemadatawarehouse := l_schemadatawarehouse);
        CALL spsysDatawarehouseCreate(p_schemalist := 's0001v0000, s0005v0000, s0006v0000, s0007v0000',
                                      p_schemadatawarehouse := l_schemadatawarehouse);
    END;
$test$ LANGUAGE plpgsql;
*/
DECLARE
--         p_schemalist            VARCHAR := 's0001v0000, s0005v0000, s0006v0000, s0007v0000';
--         p_schemadatawarehouse   VARCHAR := 's0009v0000';
    e_Context               TEXT;
    e_Msg                   TEXT;
    e_State                 TEXT;
    l_Error                 RECORD;
    l_SQL                   VARCHAR := '';
    l_TableSQL              VARCHAR;
    l_sRec                  RECORD;
    l_datawarehousesystemid INT;
    l_datawarehousename     VARCHAR;
    l_currentsearchpath     VARCHAR;
    l_schemadatawarehouse   VARCHAR;
BEGIN
    l_currentsearchpath := CURRENT_SETTING('search_path');
    DROP TABLE IF EXISTS t_schemas;
    CREATE TEMP TABLE t_schemas
    (
        id         SERIAL,
        schemaname VARCHAR
    );

    INSERT INTO t_schemas (schemaname)
    SELECT UNNEST(STRING_TO_ARRAY(LOWER(REPLACE(p_schemalist, ' ', '')), ','));

    IF p_schemadatawarehouse IS NOT NULL
    THEN
        l_schemadatawarehouse := p_schemadatawarehouse;
        SELECT STRING_AGG('
        call spsysTableMerge(''' || l_schemadatawarehouse || ''',''' || schemaname || ''',''sysDictionaryTable'');
        call spsysTableMerge(''' || l_schemadatawarehouse || ''',''' || schemaname || ''',''sysDictionaryColumn'');
        ', '')
        INTO l_sql
        FROM (
                 SELECT schemaname
                 FROM t_schemas
                 ORDER BY id DESC) a;

        PERFORM SET_CONFIG('search_path', p_schemadatawarehouse || ',' || l_currentsearchpath, TRUE);

        EXECUTE (l_sql);

    ELSE
        SELECT id INTO l_datawarehousesystemid FROM vwexSystemNextValAWS;
        l_datawarehousename := 'DW s' || RIGHT('0000' || l_datawarehousesystemid::VARCHAR, 4) || 'v0000';
        l_schemadatawarehouse := SUBSTRING(l_datawarehousename, 4, 999);

        p_schemadatawarehouse := l_schemadatawarehouse;
        PERFORM fnsysMDSGetNextSystemID(p_name := l_datawarehousename, p_IsSubnetServer := FALSE,
                                        p_exSystemID := l_datawarehousesystemid);

        l_sql := 'CREATE SCHEMA ' || l_schemadatawarehouse || ';';

        EXECUTE (l_sql);

        PERFORM SET_CONFIG('search_path', p_schemadatawarehouse || ',' || l_currentsearchpath, TRUE);

        l_sql := REPLACE('do $sub$ begin

        create table sysdictionarycolumn
        as
        select * from l_schemalast.sysdictionarycolumn;

        create table sysdictionarytable
        as
        select * from l_schemalast.sysdictionarytable;

        ', 'l_schemalast', (
                                                        SELECT schemaname
                                                        FROM t_schemas
                                                        ORDER BY id DESC
                                                        LIMIT 1));

        -- Merge the rest of the dictionarytable and dictionarycolumn tables from the
        -- schemas into the DW schema
        SELECT STRING_AGG('
        call spsysTableMerge(''' || l_schemadatawarehouse || ''',''' || schemaname || ''',''sysDictionaryTable'');
        call spsysTableMerge(''' || l_schemadatawarehouse || ''',''' || schemaname || ''',''sysDictionaryColumn'');
        ', '')
        INTO l_tablesql
        FROM (
                 SELECT schemaname
                 FROM t_schemas
                 WHERE id NOT IN (
                                     SELECT MAX(id)
                                     FROM t_schemas)
                 ORDER BY id DESC) a;

        l_sql := l_sql || l_tableSQL || ' end; $sub$';
--raise notice '%', l_sql;
        EXECUTE (l_sql);

    END IF;

    -- This will create a DW schema that is a aggregation of all tables
    -- and columns from all schemas
    CALL spsysschemaupdate(l_schemadatawarehouse);

    FOR l_sRec IN
        SELECT schemaname
        FROM t_schemas
        ORDER BY id DESC
    LOOP

        SELECT STRING_AGG('
        call spsysTableMerge(''' || l_schemadatawarehouse || ''',''' || l_sRec.schemaname || ''',''' ||
                          table_name || ''');
        ', '')
        INTO l_tablesql
        FROM (
                 SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = l_sRec.schemaname
                   AND table_type = 'BASE TABLE'
                   AND (table_name LIKE 'syschangehistory%' OR table_name NOT LIKE 'sys%')
                   AND (table_name NOT LIKE ('ex%') OR table_name = 'exsystem')
                   AND table_name != 'glsetup'
                 ORDER BY table_name) a;

        IF l_tablesql IS NOT NULL
        THEN
            EXECUTE (l_tablesql);
        END IF;

    END LOOP;

    SELECT STRING_AGG('
        call spsysTemporalDataNormalize(''' || name || ''');
        ', '')
    INTO l_sql
    FROM sysdictionarytable
    WHERE istabletemporal;

    IF l_sql IS NOT NULL
    THEN
        EXECUTE (l_sql);
    END IF;

    PERFORM SET_CONFIG('search_path', l_currentsearchpath, TRUE);

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
END;
$$ LANGUAGE plpgsql;
