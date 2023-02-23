CREATE OR REPLACE PROCEDURE s0000v0000.spsysNotNullConstraintGenerate(p_DropNotNullConstraint BOOLEAN DEFAULT FALSE,
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
 OVERVIEW
This procedure sets up Not Null constraints based on the dictionary

CHANGE LOG
20211125 Blair Kjenner	Initial Code

SAMPLE CALL

PARAMETERS

p_DropNotNullConstraint - DEFAULT FALSE - If true Not Null Constraints are dropped

call spsysNotNullConstraintGenerate()

 */
DECLARE
    l_sql VARCHAR := '';
    l_rec RECORD;
BEGIN

    FOR l_rec IN SELECT f.name AS ColumnName,
                        t.Name AS TableName
                 FROM sysdictionarycolumn f
                 LEFT JOIN sysdictionarytable t
                           ON t.id = f.sysDictionaryTableId
                 WHERE f.isnullable = FALSE
                 ORDER BY t.name
    LOOP
        l_SQL := l_SQL || 'ALTER TABLE ' || l_rec.TableName
                     || ' ALTER COLUMN ' || l_rec.ColumnName
                     || CASE WHEN p_DropNotNullConstraint THEN ' DROP'
                             ELSE ' SET'
                             END
                     || ' NOT NULL;' || CHR(13) || CHR(10);
    END LOOP;
    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;
    EXECUTE (l_SQL);
END
$$ LANGUAGE plpgsql
