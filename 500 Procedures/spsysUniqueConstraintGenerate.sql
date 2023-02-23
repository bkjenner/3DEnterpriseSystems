CREATE OR REPLACE PROCEDURE s0000v0000.spsysUniqueConstraintGenerate(p_DropUniqueConstraint BOOLEAN DEFAULT FALSE,
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
This procedure sets up Unique constraints based on the dictionary

CHANGE LOG
20211125 Blair Kjenner	Initial Code

SAMPLE CALL

PARAMETERS

p_DropUniqueConstraint - DEFAULT FALSE - If true Unique Constraints are dropped

call spsysUniqueConstraintGenerate()

 */
DECLARE
    l_sql VARCHAR := '';
    l_rec RECORD;
BEGIN

    FOR l_rec IN SELECT LOWER(a.name) AS       TableName,
                        LOWER(b.UniqueColumns) UniqueColumns
                 FROM sysdictionarytable a
                 JOIN LATERAL ( SELECT STRING_AGG(aa.Name, ', ') UniqueColumns
                                FROM sysDictionaryColumn aa
                                WHERE aa.sysdictionarytableid = a.id
                                  AND aa.isincludedinuniqueconstraint
                          ) AS b
                      ON TRUE
                 WHERE b.UniqueColumns IS NOT NULL
                 ORDER BY a.name
    LOOP
        l_SQL := l_SQL || FORMAT('
ALTER TABLE %1$s DROP constraint IF EXISTS %1$s_unique_constraint;
', l_rec.TableName);
        IF p_DropUniqueConstraint = FALSE
        THEN
            l_SQL := l_sql || FORMAT('
ALTER TABLE %1$s ADD CONSTRAINT %1$s_unique_constraint UNIQUE (%2$s);
', l_rec.TableName, l_rec.uniquecolumns);
        END IF;
    END LOOP;
    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;
    EXECUTE (l_SQL);
END
$$ LANGUAGE plpgsql;
