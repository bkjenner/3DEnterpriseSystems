CREATE OR REPLACE PROCEDURE S0000V0000.spsysSchemaUpdate(
    p_schemaname VARCHAR DEFAULT NULL, p_dropsuperfluoustables BOOLEAN DEFAULT FALSE)
AS
$BODY$
/*
OVERVIEW
This stored procedure modifiies the database to match the data dictionary.

CHANGE LOG
20220921    Blair Kjenner   Initial Code

PARAMETERS
p_schemaname - Name of schema that is going to be updated.  Defaults to current schema
p_dropsuperfluoustables - Will drop superfluous tables if they exist if set to true

SAMPLE CALL

call spsysSchemaUpdate ();

*/
DECLARE
    l_systemid   INT;
    l_searchpath VARCHAR;
    l_string     VARCHAR;
    l_SQL        VARCHAR := '';
    l_curC       RECORD;
    l_curT       RECORD;
    l_firstchar  VARCHAR;
BEGIN

    l_searchpath := CURRENT_SETTING('search_path');
    p_schemaname := LOWER(COALESCE(p_schemaname, CURRENT_SCHEMA()));
    l_systemid := fnsysCurrentSystemID(p_schemaname);

    PERFORM SET_CONFIG('search_path', p_schemaname || ',' || CURRENT_SETTING('search_path'), TRUE);

    DROP TABLE IF EXISTS t_databasecolumns;
    CREATE TEMP TABLE t_databasecolumns
    AS
    SELECT c.relname::VARCHAR    AS tablename,
           a.attname::VARCHAR    AS columnname,
           a.attnum::INT         AS columnsequence,
           JL4.columndefault     AS defaultvalue,
           CASE WHEN a.attnotnull OR t.typtype = 'd'::CHAR AND t.typnotnull THEN FALSE
                ELSE TRUE
                END::BOOLEAN     AS isnullable,
           jl2.datatype::VARCHAR AS datatype,
           jl7.datalength::INT   AS datalength,
           jl0.decimals::INT     AS decimals
    FROM pg_attribute a
    LEFT JOIN pg_attrdef ad
              ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
    JOIN      (pg_class c
            JOIN pg_namespace nc
        ON c.relnamespace = nc.oid)
              ON a.attrelid = c.oid
    JOIN      (pg_type t
            JOIN pg_namespace nt
        ON t.typnamespace = nt.oid)
              ON a.atttypid = t.oid
    LEFT JOIN (pg_type bt
            JOIN pg_namespace nbt
        ON bt.typnamespace = nbt.oid)
              ON t.typtype = 'd'::CHAR AND t.typbasetype = bt.oid
    JOIN      LATERAL (SELECT information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.*, t.*),
                                                                   information_schema._pg_truetypmod(a.*, t.*))::INT AS decimals) JL0
              ON TRUE
    JOIN      LATERAL (SELECT CASE
                                  WHEN t.typtype = 'd'::CHAR THEN
                                      CASE
                                          WHEN bt.typelem <> 0::oid AND bt.typlen = '-1'::INTEGER THEN 'array'
                                          WHEN nbt.nspname = 'pg_catalog'::name
                                              THEN FORMAT_TYPE(t.typbasetype, NULL::INTEGER)
                                          ELSE 'user-defined'
                                          END
                                  ELSE
                                      CASE
                                          WHEN t.typelem <> 0::oid AND t.typlen = '-1'::INTEGER THEN 'array'
                                          WHEN COALESCE(jl0.decimals, 0) > 0 THEN 'decimal'
                                          WHEN nt.nspname = 'pg_catalog'::name
                                              THEN FORMAT_TYPE(a.atttypid, NULL::INTEGER)
                                          ELSE 'user-defined'
                                          END
                                  END::VARCHAR AS datatype
                  ) jl1
              ON TRUE
    JOIN      LATERAL (SELECT CASE WHEN jl1.datatype = 'character' THEN 'char'
                                   WHEN jl1.datatype = 'character varying' THEN 'varchar'
                                   WHEN jl1.datatype = 'double precision' THEN 'float'
                                   WHEN jl1.datatype LIKE 'timestamp%' THEN 'datetime'
                                   WHEN COALESCE(jl0.decimals, 0) > 0 THEN 'decimal'
                                   ELSE jl1.datatype
                                   END datatype
                  ) jl2
              ON TRUE
    JOIN      LATERAL (SELECT REPLACE(REPLACE(CASE
                                                  WHEN a.attgenerated = ''::CHAR
                                                      THEN PG_GET_EXPR(ad.adbin, ad.adrelid)
                                                  ELSE NULL::TEXT
                                                  END::information_schema.character_data, ' => ', ':='),
                                      's0000v0000.', '') AS columndefault) JL3
              ON TRUE
    JOIN      LATERAL (SELECT CASE WHEN JL3.columndefault LIKE 'fnsysidcreate%'
                                       THEN REPLACE(jl3.columndefault, '::character varying', '')
                                   WHEN JL3.columndefault LIKE '%without time zone'
                                       THEN REPLACE(jl3.columndefault, ' without time zone', '')
                                   ELSE jl3.columndefault
                                   END columndefault ) JL4
              ON TRUE
    JOIN      LATERAL (SELECT information_schema._pg_char_max_length(information_schema._pg_truetypid(a.*, t.*),
                                                                     information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS charlength) JL5
              ON TRUE
    JOIN      LATERAL (SELECT information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.*, t.*),
                                                                       information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS decimallength) JL6
              ON TRUE
    JOIN      LATERAL (SELECT CASE WHEN JL2.datatype IN ('char', 'varchar', 'nvarchar', 'nchar') THEN jl5.charlength
                                   WHEN JL2.datatype = 'decimal' THEN jl6.decimallength
                                   END datalength) jl7
              ON TRUE
    WHERE NOT PG_IS_OTHER_TEMP_SCHEMA(nc.oid)
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND c.relkind = 'r'
      AND (PG_HAS_ROLE(c.relowner, 'USAGE'::TEXT) OR
           HAS_COLUMN_PRIVILEGE(c.oid, a.attnum, 'SELECT, INSERT, UPDATE, REFERENCES'::TEXT))
      AND nc.nspname = p_schemaname;

    DROP TABLE IF EXISTS t_dictionarycolumns;
    CREATE TEMP TABLE t_dictionarycolumns AS
    SELECT f.id,
           LOWER(f.name)                      columnname,
           LOWER(COALESCE(f.purpose, 'null')) purpose,
           t.id                               sysdictionarytableid,
           LOWER(COALESCE(t.name, 'null'))    tablename,
           f.datatype                         datatype,
           f.datalength                       datalength,
           f.decimals                         decimals,
           COALESCE(t.istabletemporal, FALSE) istabletemporal,
           LOWER(f.defaultvalue)              defaultvalue
    FROM sysdictionarycolumn f
    LEFT JOIN sysdictionarytable t
              ON t.id = f.sysDictionaryTableID
    LEFT JOIN sysdictionarytable t2
              ON t2.id = f.sysDictionaryTableIDForeign;

    FOR l_curT IN SELECT LOWER(t.name)     TableName,
                         t.id              sysDictionaryTableID,
                         t.isTableTemporal isTableTemporal
                  FROM sysdictionarytable t
                  WHERE LOWER(t.name) NOT IN (
                                                 SELECT table_name
                                                 FROM information_schema.tables
                                                 WHERE table_schema = LOWER(p_schemaname)
                                                   AND table_type = 'BASE TABLE')
                  ORDER BY t.name
    LOOP
        l_sql := l_sql || REPLACE(REPLACE('
create sequence l_schema.l_tablename_id_seq;
create table l_schema.l_tablename
', 'l_tablename', l_curT.TableName)
            , 'l_schema', p_schemaname);
        l_firstchar := '(';
        FOR l_curc IN SELECT columnname,
                             purpose,
                             datatype,
                             datalength,
                             decimals,
                             defaultvalue
                      FROM t_dictionarycolumns
                      WHERE sysdictionarytableid = l_curt.sysdictionarytableid
                      ORDER BY id
        LOOP

            l_sql := l_sql || CHR(9) ||
                     fnsyscolumndefinitioncreate(p_firstchar := l_firstchar,
                                                 p_columnname := l_curC.columnname,
                                                 p_purpose := l_curC.purpose,
                                                 p_schema := p_schemaname,
                                                 p_tablename := l_curT.TableName,
                                                 p_systemid := l_systemid,
                                                 p_datatype := l_curC.datatype,
                                                 p_datalength := l_curC.datalength,
                                                 p_decimals := l_curC.decimals,
                                                 p_defaultvalue := l_curC.defaultvalue) ||
                     CHR(13) || CHR(10);
            l_firstchar := ',';
        END LOOP;
        IF l_curT.isTableTemporal
        THEN
            l_sql := l_sql || CHR(9) || ', primary key (id, temporalenddate));' ;
        ELSE
            l_sql := l_sql || CHR(9) || ', primary key (id));' ;
        END IF;
        l_sql := l_sql || CHR(13) || CHR(10) || CHR(13) || CHR(10);
    END LOOP;

    IF p_dropsuperfluoustables
    THEN
        -- drop superfluous tables
        SELECT STRING_AGG(REPLACE('drop table if exists l_tablename cascade;
drop sequence if exists l_tablename_id_seq cascade;

', 'l_tablename', a.table_name), '')
        INTO l_string
        FROM information_schema.tables a
        WHERE a.table_schema = LOWER(p_schemaname)
          AND a.table_type = 'BASE TABLE'
          AND a.table_name NOT IN (
                                      SELECT LOWER(aa.name)
                                      FROM sysdictionarytable aa);

        l_SQL := COALESCE(l_sql, '') || COALESCE(l_string, '');
    END IF;

    -- drop superfluous columns.  Note if a column changes datatype we are forced
    -- to drop and re-add it.  This can be enhanced in the future because some
    -- conversions are okay (like int to varchar)
    SELECT STRING_AGG('alter table ' || a.tablename || ' drop column ' || a.columnname || ' cascade;
', '')
    INTO l_string
    FROM t_databasecolumns a
    LEFT JOIN t_dictionarycolumns b
              ON a.tablename = b.tablename AND a.columnname = b.columnname AND a.datatype = b.datatype
    WHERE b.columnname IS NULL
      AND a.tablename IN (
                             SELECT DISTINCT tablename
                             FROM t_dictionarycolumns aa);

    l_SQL := COALESCE(l_sql, '') || COALESCE(l_string, '');

    -- Add missing columns
    SELECT STRING_AGG(fnsyscolumndefinitioncreate(
                              p_firstchar := 'alter table ' || a.tablename || ' add ',
                              p_columnname := a.columnname,
                              p_purpose := a.purpose,
                              p_schema := p_schemaname,
                              p_tablename := l_curT.TableName,
                              p_systemid := l_systemid,
                              p_datatype := a.datatype,
                              p_datalength := a.datalength,
                              p_decimals := a.decimals,
                              p_defaultvalue := a.defaultvalue) || ';
', '')
    INTO l_string
    FROM t_dictionarycolumns a
    LEFT JOIN t_databasecolumns b
              ON a.tablename = b.tablename AND a.columnname = b.columnname AND a.datatype = b.datatype
    WHERE b.columnname IS NULL
      AND a.tablename IN (
                             SELECT DISTINCT tablename
                             FROM t_databasecolumns aa);

    l_SQL := COALESCE(l_sql, '') || COALESCE(l_string, '');

    -- alter columns that have changed

    SELECT STRING_AGG(fnsyscolumndefinitioncreate(p_firstchar := 'alter table ' || a.tablename || ' alter column ',
                                                  p_columnname := a.columnname,
                                                  p_purpose := a.purpose,
                                                  p_schema := p_schemaname,
                                                  p_tablename := a.tablename,
                                                  p_systemid := l_systemid,
                                                  p_datatype := a.datatype,
                                                  p_datalength := a.datalength,
                                                  p_decimals := a.decimals,
                                                  p_defaultvalue := a.defaultvalue,
                                                  p_isalter := TRUE) || ';
', '')
    INTO l_string
    FROM t_dictionarycolumns a
    JOIN t_databasecolumns b
         ON a.tablename = b.tablename AND a.columnname = b.columnname AND a.datatype = b.datatype
    JOIN sysdictionarycolumn c
         ON c.id = a.id
    WHERE a.decimals != b.decimals
       OR a.defaultvalue != b.defaultvalue
       OR a.datalength != b.datalength;

    l_SQL := COALESCE(l_sql, '') || COALESCE(l_string, '');

    IF COALESCE(l_sql, '') != ''
    THEN
        EXECUTE l_SQL;
    END IF;

    PERFORM SET_CONFIG('search_path', l_searchpath, TRUE);

END ;
$BODY$ LANGUAGE plpgsql;

