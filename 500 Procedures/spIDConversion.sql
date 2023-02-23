CREATE OR REPLACE PROCEDURE S0000V0000.spIDConversion(p_tablename VARCHAR, p_order VARCHAR DEFAULT 'ID',
                                                      p_increment INT DEFAULT 10)
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


This converts ID's from high values to low values starting at 1

20210607    Blair Kjenner   Initial Code

 call IDConversion('glBatchType', 'name')

 */
DECLARE
    l_columnnames TEXT;
    l_sql         TEXT;
    l_convertIDs  TEXT;
BEGIN
    SELECT STRING_AGG(column_name::TEXT, ',')
    INTO l_columnnames
    FROM (
             SELECT column_name
             FROM INFORMATION_SCHEMA.COLUMNS a
             WHERE TABLE_NAME ILIKE p_tablename
               AND TABLE_SCHEMA = CURRENT_SCHEMA()
             ORDER BY a.ordinal_position) a;

    l_SQL := '
SET session_replication_role = replica;

DROP TABLE IF EXISTS aaIDConversion;
CREATE TEMP TABLE aaIDConversion
AS
SELECT *, (RANK() OVER ( ORDER BY ' || p_order || ' ))*' || p_increment || ' NewID FROM ' || p_tablename || ';

DELETE FROM ' || p_tablename || ' WHERE ID IN (SELECT ID FROM aaIDConversion);

INSERT INTO ' || p_tablename || ' (' || l_columnnames || ')
SELECT NewID' || SUBSTRING(l_columnnames, 3, 999) || ' FROM aaIDConversion;

';
    SELECT STRING_AGG('Update ' || c.name || ' a set ' || a.name || '=b.newid from aaIDConversion b where a.' ||
                      a.name ||
                      '=b.id;'::TEXT, CHR(13) || CHR(10))
    INTO l_convertIDs
    FROM sysdictionarycolumn A
    JOIN sysdictionarytable b
         ON b.id = a.sysdictionarytableidforeign
    JOIN sysdictionarytable c
         ON c.id = a.sysdictionarytableid
    WHERE LOWER(b.name) ILIKE p_tablename;

    l_SQL := l_SQL || COALESCE(l_convertIDs, '') || ';

SET session_replication_role = origin;

';
    --RAISE NOTICE '%', l_columnnames;
    --RAISE NOTICE '%', l_SQL;

    EXECUTE l_SQL;
END;
$$
    LANGUAGE plpgsql;

