CREATE OR REPLACE PROCEDURE S0000V0000.spsysForeignKeyCacheRefresh(p_debug BOOLEAN DEFAULT FALSE)
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
This procedure refreshes the sysforeignkeycache with new data from the master records.

CHANGE LOG
20211006 Blair Kjenner	Initial Code

PARAMETERS
p_debug - DEFAULT FALSE - TRUE outputs the script without executing it. FALSE executes the script.

SAMPLE CALL
call spsysForeignKeyCacheRefresh (p_debug:=false);

*/
DECLARE
    l_SQL TEXT ;
BEGIN

    TRUNCATE TABLE sysforeignkeycache;
    SELECT STRING_AGG(FORMAT('
INSERT INTO sysforeignkeycache (
sysdictionarytableid, rowid, translation)

SELECT %s  AS          sysdictionarytableid,
       ID AS          ROWID,
       coalesce(b.foreignkeytranslation,''null'')
FROM %s AS x
join lateral (select (%s) foreignkeytranslation) b ON TRUE
%s
;
'
                          , x.id
                          , x.name
                          , jl2.translation
                          ,case when istabletemporal then 'and temporalenddate=''9999-12-31''' else '' end), '')
    INTO l_SQL
    FROM sysdictionarytable AS x
    JOIN LATERAL (SELECT CASE WHEN x.translation LIKE '%x.%' THEN x.translation
                              ELSE 'x.' || x.translation
                              END translation) jl2
         ON TRUE
    WHERE x.translation IS NOT NULL;

    IF p_debug = TRUE
    THEN
        RAISE NOTICE '%', l_SQL;
    ELSE
        EXECUTE (l_SQL);
    END IF;

END;

$$ LANGUAGE plpgsql
