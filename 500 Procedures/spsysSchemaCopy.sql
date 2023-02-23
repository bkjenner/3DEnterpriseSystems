CREATE OR REPLACE PROCEDURE S0000V0000.spsysSchemaCopy(
    p_SourceSchema TEXT,
    p_DestSchema TEXT,
    p_SourcePrefix VARCHAR DEFAULT NULL,
    p_DestPrefix VARCHAR DEFAULT NULL,
    p_Update BOOLEAN DEFAULT FALSE,
    p_Debug BOOLEAN DEFAULT FALSE,
    p_SystemName VARCHAR DEFAULT NULL)
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
This stored procedure creates a new schema from an existing one.  Schemas are made up of two components - System ID and Release ID.
The System ID is baked into each table.  When a new system is created, it will be assigned a new id.  Any IDs it creates
will be prefixed with that ID.

The release ID allows the same System ID to have many releases.  For example we could have

s0001v010 - Which is our current production version
s0001v011 - Which is our UAT version
s0001v012 - Which is our Dev version

The version number allows us to update insert new values into our system and lookup tables that we can transfer from
one release to the next without colliding on ids.

CHANGE LOG
20211010    Blair Kjenner   Initial Code

PARAMETERS
p_SourceSchema - Name of schema that is going to be copied
p_DestSchema - Name of new schema
p_SourcePrefix - DEFAULT NULL - This wraps the source table name with a character.  (Used for creating template tables)
p_DestPrefix - DEFAULT NULL - This wraps the dest table name with a character (Used for creating template tables).
p_debug - DEFAULT FALSE - TRUE outputs the script without executing it. FALSE executes the script.

SAMPLE CALL

call spsysSchemaCopy ('S0001V0000', 'S0003V0000');

 */
DECLARE
    l_sql               TEXT;
    l_sqlAll            TEXT;
    l_SystemIDDest      VARCHAR;
    l_SystemIDSource    VARCHAR;
    l_ReleaseID         VARCHAR;
    l_CurrentSearchPath VARCHAR;
BEGIN


    p_SourceSchema := LOWER(p_SourceSchema);
    p_DestSchema := LOWER(p_DestSchema);
    l_SystemIDSource := fnsysCurrentSystemID(p_SourceSchema);
    l_SystemIDDest := fnsysCurrentSystemID(p_DestSchema);
    l_ReleaseID := fnsysCurrentReleaseID(p_DestSchema);
    p_SourcePrefix := LOWER(COALESCE(p_SourcePrefix, ''));
    p_DestPrefix := LOWER(COALESCE(p_DestPrefix, ''));

    IF p_SourceSchema = p_DestSchema
    THEN
        RAISE WARNING 'Source and Dest must be different %', p_SourceSchema;
        RETURN;
    END IF;

    IF (isnumeric(l_SystemIDDest) IS FALSE OR isnumeric(l_ReleaseID) IS FALSE)
    THEN
        RAISE WARNING 'Could not deduce new system ( % ) and/or release ( % )', l_SystemIDDest, l_ReleaseID;
        RETURN;
    END IF;

    IF NOT EXISTS(SELECT FROM pg_namespace WHERE nspname = p_SourceSchema)
    THEN
        RAISE WARNING 'source schema % does not exist!', p_SourceSchema;
        RETURN;
    END IF;

    IF EXISTS(SELECT FROM pg_namespace WHERE nspname = p_DestSchema) AND p_Update = FALSE
    THEN
        RAISE WARNING 'dest schema % already exists!', p_DestSchema;
        RETURN;
    END IF;

    IF p_Debug
    THEN
        l_SQLAll := REPLACE(REPLACE('
    CREATE SCHEMA IF NOT EXISTS ' || p_DestSchema || ';
    SELECT SET_CONFIG(''search_path'', ''l_destschema, l_sourceschema ,'' || CURRENT_SETTING(''search_path''),
                       FALSE);
    ', 'l_destschema', p_DestSchema), 'l_sourceschema', p_SourceSchema);
        IF p_SystemName IS NOT NULL
        THEN
            l_SQLAll := l_SQLALL || 'COMMENT ON SCHEMA ' || p_DestSchema || ' IS ''' || p_SystemName || ''';';
        END IF;
    ELSE
        EXECUTE ('CREATE SCHEMA IF NOT EXISTS ' || p_DestSchema);
        IF p_SystemName IS NOT NULL
        THEN
            EXECUTE ('COMMENT ON SCHEMA ' || p_DestSchema || ' IS ''' || p_SystemName || '''');
        END IF;
    END IF;

    l_CurrentSearchPath := CURRENT_SETTING('search_path');

    PERFORM SET_CONFIG('search_path', p_DestSchema || ',' || p_SourceSchema || ',' || CURRENT_SETTING('search_path'),
                       FALSE);

    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('
    DROP TABLE IF EXISTS l_destschema.l_desttable CASCADE;
    DROP SEQUENCE IF EXISTS l_destschema.l_desttable_id_seq;
    CREATE SEQUENCE l_destschema.l_desttable_id_seq;
    CREATE TABLE l_destschema.l_desttable (LIKE l_sourceschema.l_sourcetable INCLUDING ALL);
    INSERT INTO l_destschema.l_desttable
    SELECT * from l_sourceschema.l_sourcetable;
    SELECT SETVAL(''l_desttable_id_seq'', p_startingid, FALSE);
    ', 'l_desttable', p_destprefix || s.name), 'l_sourcetable', table_name)
                                          , 'l_sourceschema', p_SourceSchema), 'l_destschema', p_destSchema),
                              'p_startingid',
                              lj.startingid::VARCHAR), '')
    INTO l_sql
    FROM information_schema.tables A
    JOIN sysdictionarytable s
         ON LOWER(p_SourcePrefix || s.name) = LOWER(a.table_name)
    JOIN LATERAL (SELECT (CASE WHEN LOWER(s.tabletype) IN ('lookup', 'system') THEN l_ReleaseID::INT * 10000
                               ELSE 0
                               END) + 1 Startingid) lj
         ON TRUE
    WHERE LOWER(table_type) = 'base table'
      AND table_schema = p_SourceSchema
      AND table_name LIKE p_SourcePrefix || '%';

    IF l_SQL IS NULL
    THEN
        RAISE WARNING 'Source tables not found for % %', p_SourceSchema, CASE WHEN p_SourcePrefix = '' THEN ''
                                                                              ELSE ' with prefix ' || p_SourcePrefix
                                                                              END;
        RETURN;
    END IF;

    l_SQL := '
SET session_replication_role = replica;
' || l_SQL || '
SET session_replication_role = origin;
';
    IF p_Debug
    THEN
        l_SQLAll := l_SQLAll || COALESCE(l_SQL, 'NULL');
    ELSE
        EXECUTE (l_SQL);
    END IF;

    --     add FK constraint  (Oct 26 2021. With the prefix being used it worked way better just to regenerate them.)
--     SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE('
--     ALTER TABLE l_tablename ADD CONSTRAINT l_constraintname l_constraintdef;
--     ', 'l_destschema', p_destSchema), 'l_tablename', p_destprefix || replace(mt.relname,p_SourcePrefix,'')), 'l_constraintname', ct.conname),
--                               'l_constraintdef', jl.constraintdef), '')
--     INTO l_SQL
--     FROM pg_constraint ct
--     JOIN pg_class mt -- Main Table
--          ON mt.oid = ct.conrelid
--     JOIN pg_class ft -- Foreign Table
--          ON ft.oid = ct.confrelid
--     JOIN LATERAL (SELECT LEFTFIND(lower(PG_GET_CONSTRAINTDEF(ct.oid)),'references')||'references '||p_DestPrefix||replace(ft.relname,p_sourceprefix,'')||'('||RIGHTFIND(lower(PG_GET_CONSTRAINTDEF(ct.oid)),'(') ConstraintDef) jl
--          ON TRUE
--     WHERE ct.connamespace = (
--                                 SELECT nss.oid
--                                 FROM pg_namespace nss
--                                 WHERE nspname = p_SourceSchema)
--       AND mt.relname LIKE p_SourcePrefix || '%'
--       AND mt.relkind = 'r'
--       AND ct.contype = 'f';
--
--     IF COALESCE(l_SQL, '') != ''
--     THEN
--         IF p_Debug
--         THEN
--             l_SQLAll := l_SQLAll || COALESCE(l_SQL, 'NULL');
--         ELSE
--             RAISE NOTICE '%', l_sql;
--             EXECUTE (l_SQL);
--         END IF;
--     END IF;

    --  add column default
    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE('
    ALTER TABLE l_tablename ALTER COLUMN l_columnname SET DEFAULT l_columndefault;
    ', 'l_tablename', p_DestPrefix || SUBSTRING(TABLE_NAME, LENGTH(p_SourcePrefix) + 1, 999)), 'l_columnname',
                                      column_name), 'l_columndefault',
                              lj.columndefault), '')
    INTO l_SQL
    FROM information_schema.COLUMNS A
    JOIN LATERAL (SELECT CASE WHEN column_default LIKE '%fnsysidcreate%' THEN REPLACE(
                REPLACE(column_default, ' ' || l_SystemIDSource || ',', ' ' || l_SystemIDDest || ','), p_SourceSchema,
                p_DestSchema)
                              ELSE column_default
                              END ColumnDefault) LJ
         ON TRUE
    WHERE table_schema = p_SourceSchema
      AND column_default IS NOT NULL
      AND table_name LIKE p_SourcePrefix || '%';

    IF COALESCE(l_SQL, '') != ''
    THEN
        IF p_Debug
        THEN
            l_SQLAll := l_SQLAll || COALESCE(l_SQL, 'NULL');
        ELSE
            EXECUTE (l_SQL);
        END IF;
    END IF;

    -- add Table Triggers
    WITH T AS (
                  SELECT trg.tgname  AS triggername,
                         tbl.relname AS tablename,

                         CASE
                             WHEN trg.tgenabled = 'O' THEN 'ENABLED'
                             ELSE 'DISABLED'
                             END     AS status,
                         CASE trg.tgtype::INTEGER & 1
                             WHEN 1 THEN 'ROW'::TEXT
                             ELSE 'STATEMENT'::TEXT
                             END     AS triggerlevel,
                         CASE trg.tgtype::INTEGER & 66
                             WHEN 2 THEN 'BEFORE'
                             WHEN 64 THEN 'INSTEAD OF'
                             ELSE 'AFTER'
                             END     AS actiontiming,
                         CASE trg.tgtype::INTEGER & CAST(60 AS int2)
                             WHEN 16 THEN 'UPDATE'
                             WHEN 8 THEN 'DELETE'
                             WHEN 4 THEN 'INSERT'
                             WHEN 20 THEN 'INSERT OR UPDATE'
                             WHEN 28 THEN 'INSERT OR UPDATE OR DELETE'
                             WHEN 24 THEN 'UPDATE OR DELETE'
                             WHEN 12 THEN 'INSERT OR DELETE'
                             WHEN 32 THEN 'TRUNCATE'
                             END     AS triggerevent,
                         'EXECUTE PROCEDURE ' || (
                                                     SELECT nspname
                                                     FROM pg_namespace
                                                     WHERE oid = pc.pronamespace)
                             || '.' || proname || '('
                             ||
                         REGEXP_REPLACE(REPLACE(TRIM(TRAILING '\000' FROM ENCODE(tgargs, 'escape')), '\000', ','),
                                        '{(.+)}',
                                        '''{\1}''', 'g')
                             || ')'  AS actionstatement

                  FROM pg_trigger trg
                  JOIN pg_class tbl
                       ON trg.tgrelid = tbl.oid
                  JOIN pg_proc pc
                       ON pc.oid = trg.tgfoid
                  WHERE trg.tgname NOT LIKE 'RI_ConstraintTrigger%'
                    AND trg.tgname NOT LIKE 'pg_sync_pg%'
                    AND tbl.relnamespace = (
                                               SELECT oid
                                               FROM pg_namespace
                                               WHERE nspname = p_SourceSchema)
                    AND tbl.relname LIKE p_SourcePrefix || '%')
    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('
    CREATE TRIGGER l_triggername l_actiontiming l_triggerevent ON l_tablename FOR EACH l_triggerlevel l_actionstatement;
    ', 'l_triggername', triggername), 'l_actiontiming', actiontiming), 'l_triggerevent', triggerevent), 'l_tablename',
                                              p_DestPrefix || SUBSTRING(tablename, LENGTH(p_SourcePrefix) + 1, 999)),
                                      'l_triggerlevel',
                                      triggerlevel), 'l_actionstatement', actionstatement), '')
    INTO l_sql
    FROM t;

    IF COALESCE(l_SQL, '') != ''
    THEN
        IF p_Debug
        THEN
            l_SQLAll := l_SQLAll || COALESCE(l_SQL, 'NULL');
        ELSE
            EXECUTE (l_SQL);
        END IF;
    END IF;

    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE('
CREATE OR REPLACE VIEW l_tablename AS l_viewdefinition
', 'l_tablename', p_DestPrefix || table_name), 'l_viewdefinition', view_definition), 'c_', ''), '')
    INTO l_sql
    FROM information_schema.views
    WHERE table_schema = p_SourceSchema;

    IF COALESCE(l_SQL, '') != ''
    THEN
        IF p_Debug
        THEN
            l_SQLAll := l_SQLAll || l_SQL;
            RAISE NOTICE '%',l_SQLAll;
        ELSE
            EXECUTE (l_SQL);
        END IF;
    END IF;

    IF p_DestPrefix != ''
    THEN
        CALL spsysforeignkeyconstraintgenerate();
    END IF;

    PERFORM SET_CONFIG('search_path', l_CurrentSearchPath, FALSE);

END

$$
    LANGUAGE plpgsql