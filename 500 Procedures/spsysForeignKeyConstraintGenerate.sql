CREATE OR REPLACE PROCEDURE s0000v0000.spsysForeignKeyConstraintGenerate(p_DropForeignKeyConstraint BOOLEAN DEFAULT FALSE,
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
This procedure sets up Foreign Key constraints based on the dictionary.  It also sets up triggers
for checking foreign key constraints on temporal parent records.  Temporal needs to be accommodated
differently because the unique constraint for a temporal table is ID and temporalenddate.  If the ID
exists, we will always be able to satisfy the join

This procedure could be updated to check if a parent record is deleted thereby causing foreign key
reference issues for the child records.

CHANGE LOG
20210916 Blair Kjenner	Initial Code

SAMPLE CALL

PARAMETERS

p_DropForeignKeyConstraint - DEFAULT FALSE - If true Foreign Key Constraints are dropped

call spsysForeignKeyConstraintGenerate()

 */
DECLARE
    l_sql      VARCHAR := '';
    l_rec      RECORD;
    l_template VARCHAR;
BEGIN

    FOR l_rec IN SELECT f.name             AS ColumnName,
                        t.Name             AS TableName,
                        t2.Name            AS ForeignTableName,
                        t2.istabletemporal AS IsTableTemporal
                 FROM sysdictionarycolumn f
                 LEFT JOIN sysdictionarytable t
                           ON t.id = f.sysDictionaryTableId
                 LEFT JOIN sysdictionarytable t2
                           ON t2.id = f.sysDictionaryTableIdForeign
                 WHERE LOWER(f.Purpose) = 'foreign key'
                     -- There are circular references and if I attempt to maintain all references in all
                     -- schemas it means every schema has to know about every subnet server schema since the subnet server
                     -- references the subnet server schema.
                   AND LOWER(t.name) NOT IN ('exsystem', 'exSubnetServer')
                   AND t2.istabletemporal = FALSE
                 ORDER BY t.name, t2.name
    LOOP
        l_SQL := l_SQL || 'ALTER TABLE ' || l_rec.TableName || ' DROP CONSTRAINT IF EXISTS fk_' ||
                 l_rec.TableName || '_' || l_rec.ColumnName || ';' || CHR(13) || CHR(10);
        IF p_DropForeignKeyConstraint = FALSE
        THEN
            l_SQL := l_SQL || 'ALTER TABLE ' || l_rec.TableName || ' ADD CONSTRAINT fk_' ||
                     l_rec.TableName || '_' || l_rec.ColumnName || ' FOREIGN KEY (' || l_rec.ColumnName
                         || ') REFERENCES ' || l_rec.ForeignTableName || ' (ID);' ||
                     CHR(13) || CHR(10);
        END IF;

    END LOOP;
    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;
    EXECUTE (l_SQL);

    -- Add FK Triggers for temporal columns
    SELECT STRING_AGG('Drop FUNCTION if exists ' || ROUTINE_name || ' CASCADE; ', '')
    INTO l_SQL
    FROM information_schema.routines r
    WHERE (ROUTINE_name LIKE 'fnfk%insfk'
        OR ROUTINE_name LIKE 'fnfk%updfk')
      AND specific_schema = CURRENT_SCHEMA();

    IF p_debug
    THEN
        RAISE NOTICE '%', l_SQL;
    END IF;

    IF l_sql IS NOT NULL
    THEN
        EXECUTE l_sql;
    END IF;

    IF p_DropForeignKeyConstraint = FALSE
    THEN
        l_template := '
DO $SUB$
DECLARE l_ID bigint;
BEGIN
   SELECT aa.columnname INTO l_ID FROM tablename aa
   LEFT JOIN foreigntable bb ON bb.id = aa.columnname
   WHERE bb.id IS NULL
      AND aa.columnname IS NOT NULL
   LIMIT 1;
   IF l_id IS NOT NULL
   THEN
       RAISE EXCEPTION ''Bad foreign key reference in tablename for columnname %'', fnsysIDView(l_id) ;
   END IF;
END;
$SUB$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnFKtablename_columnname_InsFK()
    RETURNS TRIGGER AS
$SUB$
DECLARE
    l_ID BIGINT;
BEGIN
    IF EXISTS(SELECT FROM newrecords aa WHERE aa.columnname IS NOT NULL)
    THEN
       SELECT aa.columnname INTO l_ID FROM newrecords aa
       LEFT JOIN foreigntable bb ON bb.id = aa.columnname
       WHERE bb.id IS NULL
          AND aa.columnname IS NOT NULL
       LIMIT 1;
       IF l_id IS NOT NULL
       THEN
           RAISE EXCEPTION ''Bad foreign key reference on columnname %'', fnsysIDView(l_id) ;
       END IF;
    END IF;
    RETURN NEW;
END;
$SUB$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trFKtablename_columnname_insFK
    AFTER INSERT
    ON tablename
    REFERENCING NEW TABLE AS NewRecords
    FOR EACH STATEMENT
EXECUTE PROCEDURE fnFKtablename_columnname_insFK();

CREATE OR REPLACE FUNCTION fnFKtablename_columnname_updFK()
    RETURNS TRIGGER AS
$SUB$
DECLARE
    l_ID BIGINT;
BEGIN
    IF EXISTS(SELECT FROM newrecords aa
              JOIN oldrecords bb ON bb.id = aa.id
              WHERE COALESCE(aa.columnname, -1) != COALESCE(bb.columnname, -1))
    THEN
        SELECT aa.columnname INTO l_ID FROM newrecords aa
        LEFT JOIN foreigntable bb ON bb.id = aa.columnname
        WHERE bb.id IS NULL
          AND aa.columnname IS NOT NULL
        LIMIT 1;
        IF l_id IS NOT NULL
        THEN
            RAISE EXCEPTION ''Bad foreign key reference on columnname %'', fnsysIDView(l_id) ;
        END IF;
    END IF;
    RETURN NEW;
END;
$SUB$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trFKtablename_columnname_updFK
    AFTER UPDATE
    ON tablename
    REFERENCING OLD TABLE AS OldRecords NEW TABLE AS NewRecords
    FOR EACH STATEMENT
EXECUTE PROCEDURE fnFKtablename_columnname_updFK();
';

        SELECT STRING_AGG(
                       REPLACE(REPLACE(REPLACE(l_template, 'tablename', t.name), 'columnname', f.name), 'foreigntable',
                               t2.name), '')
        INTO l_sql
            --select *
        FROM sysdictionarycolumn f
        LEFT JOIN sysdictionarytable t
                  ON t.id = f.sysDictionaryTableId
        LEFT JOIN sysdictionarytable t2
                  ON t2.id = f.sysDictionaryTableIdForeign
        WHERE LOWER(f.Purpose) = 'foreign key'
          AND t2.istabletemporal = TRUE;

        IF p_debug
        THEN
            RAISE NOTICE '%', l_SQL;
        END IF;

        IF l_sql IS NOT NULL
        THEN
            EXECUTE l_sql;
        END IF;

    END IF;

END;

$$ LANGUAGE plpgsql
