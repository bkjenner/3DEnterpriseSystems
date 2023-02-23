CREATE OR REPLACE PROCEDURE S0000V0000.spsysMasterDataIndexGenerate(p_debug BOOLEAN DEFAULT FALSE)
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
This procedure generates the master data index (sysmasterdataindex) which is used by fnsysMasterDataQuery

2022-11-11    Blair Kjenner   Initial Code

call spsysMasterDataIndexGenerate();

*/
DECLARE
    l_sql      VARCHAR;
    l_template VARCHAR;
BEGIN

    DROP TABLE IF EXISTS t_foreignkeys;
    CREATE TEMP TABLE t_foreignkeys AS
    SELECT b.id               sysdictionarytableidforeign,
           a.id               sysdictionarycolumnidforeign,
           LOWER(a.name)      columnnameforeign,
           LOWER(b.name)      tablenameforeign,
           c.id               sysdictionarytableidmaster,
           LOWER(c.name)      tablenamemaster,
           LOWER(b.tabletype) tabletypeforeign,
           LOWER(c.tabletype) tabletypemaster,
           b.istabletemporal  istemporalforeign,
           NULL::VARCHAR      multilinktablecolumn
    FROM sysdictionarycolumn a
    JOIN sysdictionarytable b ON b.id = a.sysdictionarytableid
    JOIN sysdictionarytable c
         ON c.id = a.sysdictionarytableidforeign
    WHERE a.purpose ILIKE 'foreign key'
      AND LOWER(a.name) != 'syschangehistoryrowid'
      AND c.translation IS NOT NULL;

    l_template := '

insert into t_foreignkeys (sysdictionarytableidforeign, sysdictionarycolumnidforeign, columnnameforeign, tablenameforeign, sysdictionarytableidmaster, tablenamemaster, tabletypeforeign, tabletypemaster, istemporalforeign, multilinktablecolumn)
select distinct l_sysdictionarytableidforeign,
l_sysdictionarycolumnidforeign,
''l_columnnameforeign'' ,
''l_tablenameforeign'',
l_sysdictionarytableidcolumn,
lower(b.name) tablenamemaster,
''l_tabletypeforeign'' ,
lower(b.tabletype) tabletypemaster,
l_istabletemporal,
''l_sysdictionarytableidcolumn'' multilinktablecolumn
from l_tablenameforeign a
join sysdictionarytable b on b.id=a.l_sysdictionarytableidcolumn
where b.translation is not null;
';

    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l_template
                                                                          , 'l_sysdictionarytableidforeign',
                                                                              b.id::VARCHAR),
                                                                      'l_sysdictionarycolumnidforeign',
                                                                      a.id::VARCHAR),
                                                              'l_columnnameforeign', LOWER(a.name)),
                                                      'l_tablenameforeign', LOWER(b.name)),
                                              'l_sysdictionarytableidcolumn', COALESCE(LOWER(c.name),
                                                                                       'ERROR - Column name ' ||
                                                                                       REPLACE(LOWER(a.name), 'rowid', 'sysdictionarytableid') ||
                                                                                       ' not found')),
                                      'l_tabletypeforeign', LOWER(b.tabletype)),
                              'l_istabletemporal', b.istabletemporal::VARCHAR)
               , '')
    INTO l_SQL
    FROM sysdictionarycolumn a
    JOIN      sysdictionarytable b
              ON b.id = a.sysdictionarytableid
    LEFT JOIN sysdictionarycolumn c
              ON c.sysdictionarytableid = a.sysdictionarytableid
                  AND LOWER(c.name) = REPLACE(LOWER(a.name), 'rowid', 'sysdictionarytableid')
    WHERE a.purpose ILIKE 'multilink'
      AND b.tabletype NOT ILIKE 'system'
      AND a.sysdictionarytableidforeign IS NULL;

    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;
    EXECUTE (l_SQL);

    TRUNCATE TABLE sysmasterdataindex;

    CALL spsysForeignKeyCacheRefresh();

    l_template := '

INSERT INTO sysmasterdataindex (sysdictionarytableidmaster, rowidmaster, sysdictionarycolumnidforeign, sysdictionarytableidforeign, rowidforeign, foreignkeytranslation)
SELECT l_sysdictionarytableidmaster
     , a.l_columnnameforeign
     , l_sysdictionarycolumnidforeign
     , l_sysdictionarytableidforeign
     , a.id
     , b.translation
FROM l_tablenameforeign a
JOIN sysforeignkeycache b on b.rowid=a.l_columnnameforeign and b.sysdictionarytableid=l_sysdictionarytableidmaster
WHERE a.l_columnnameforeign IS NOT NULL l_temporalcondition l_multilinkcondition;';

    SELECT STRING_AGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l_template
                                                                          , 'l_sysdictionarytableidmaster',
                                                                              a.sysdictionarytableidmaster::VARCHAR),
                                                                      'l_columnnameforeign', a.columnnameforeign),
                                                              'l_sysdictionarycolumnidforeign',
                                                              a.sysdictionarycolumnidforeign::VARCHAR),
                                                      'l_tablenameforeign', a.tablenameforeign),
                                              'l_sysdictionarytableidforeign', a.sysdictionarytableidforeign::VARCHAR),
                                      'l_multilinkcondition', CASE WHEN a.multilinktablecolumn IS NOT NULL THEN
                                                                                   ' and a.' ||
                                                                                   a.multilinktablecolumn ||
                                                                                   ' = ' ||
                                                                                   a.sysdictionarytableidmaster
                                                                   ELSE ''
                                                                   END),
                              'l_temporalcondition', JL0.temporalcondition)
               , '')
    INTO l_SQL
    FROM t_foreignkeys a
    JOIN LATERAL (SELECT CASE WHEN a.istemporalforeign
                                  THEN ' and a.temporalstartdate <= now()::date and a.temporalenddate >= now()::date'
                              ELSE ''
                              END temporalcondition) JL0
         ON TRUE;

    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;

    EXECUTE (l_SQL);

    INSERT INTO sysmasterdataindex (sysdictionarytableidmaster, rowidmaster, foreignkeytranslation)
    SELECT sysdictionarytableid, RowId, translation
    FROM sysforeignkeycache;

    IF NOT EXISTS(SELECT
                  FROM pg_catalog.pg_class c
                  LEFT JOIN pg_catalog.pg_namespace n
                            ON n.oid
                                = c.relnamespace
                  WHERE LOWER(n.nspname) = CURRENT_SCHEMA()
                    AND LOWER(relname) = 'sysmasterdataindexindex')
    THEN
        CREATE INDEX sysmasterdataindexindex ON sysmasterdataindex (sysdictionarytableidmaster, rowidmaster, sysdictionarycolumnidforeign);
    END IF;

END
$$ LANGUAGE plpgsql;
