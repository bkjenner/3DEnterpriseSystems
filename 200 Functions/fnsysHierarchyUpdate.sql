-- Update BottomUpLevel
CREATE OR REPLACE FUNCTION S0000V0000.fnsysHierarchyUpdate(p_Table varchar, p_sortkey varchar)
    RETURNS BOOLEAN
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
This procedure sets up BottomUpLevel, TopDownLevel, DisplaySequence for hierarchical structures.  It calls glIntegrityCheck to
check for errors.  If errors are detected, it returns false otherwise it returns true.

20210326    Blair Kjenner   Initial Code

select fnsysHierarchyUpdate('glaccount', 'referencenumber')

*/
DECLARE
    l_SQL varchar;

BEGIN

    DROP TABLE IF EXISTS t_errors;
    CREATE TEMP TABLE t_errors
    AS
    SELECT * from fnglIntegrityCheck(p_table);

    IF (
           SELECT COUNT(*)
           FROM t_errors) > 0
    THEN
        RETURN FALSE;
    ELSE
        L_SQL := case when isColumnFound(p_table,'bottomuplevel') then '
WITH RECURSIVE Paths(BottomUpLevel,
                     %table%IDRoot,
                     %table%IDParent,
                     %table%ID)
                   AS (
                          SELECT 1,
                                 a.ID %table%IDRoot,
                                 a.%table%IDParent,
                                 a.ID %table%ID
                          FROM %table% a
                          LEFT JOIN %table% b
                                    ON b.%table%IDParent = a.id and b.rowstatus=''a''
                          WHERE b.id IS NULL
                          AND a.rowstatus=''a''
                          UNION ALL
                          SELECT p.BottomUpLevel + 1,
                                 p.%table%IDRoot,
                                 c.%table%IDParent,
                                 C.ID
                          FROM %table% AS c
                          INNER JOIN Paths AS p
                                     ON p.%table%IDParent = c.id
                          WHERE c.rowstatus=''a'')
UPDATE %table% a
SET BottomUpLevel = p.BottomUpLevel
FROM Paths p
     where p.%table%ID = a.ID;
' else '' end || '
-- Update TopDownLevel
WITH RECURSIVE Paths(TopDownLevel,
                     FullPath,
                     %table%ID,
                     %table%IDParent)
                   AS (
                          SELECT 1      AS TopDownLevel,
                                 coalesce(cast(a.%sortkey% as varchar),CAST(a.ID AS varchar))::TEXT AS FullPath,
                                 a.ID   AS %table%ID,
                                 a.%table%IDParent
                          FROM %table% a
                          WHERE %table%IDParent IS NULL
                          AND a.rowstatus=''a''
                          UNION ALL
                          SELECT p.TopDownLevel + 1                                                                       AS TopDownLevel,
                                 p.FullPath || ''.'' || coalesce(cast(c.%sortkey% as varchar),CAST(c.ID AS varchar)) AS FullPath,
                                 c.ID AS %table%ID,
                                 c.%table%IDParent
                          FROM %table% AS c
                          INNER JOIN Paths AS p
                                     ON p.%table%ID = c.%table%IDParent
                          WHERE c.rowstatus=''a'')

UPDATE %table% a
SET TopDownLevel   = b.TopDownLevel,
    DisplaySequence = b.DisplaySequence
FROM
    (
        SELECT p.%table%ID,
               ROW_NUMBER() OVER (
                   ORDER BY p.fullpath) AS DisplaySequence,
               p.FullPath,
               p.TopDownLevel
        FROM Paths p
    ) b
    WHERE b.%table%ID = a.id;
';
        l_SQL := replace(replace(l_SQL,'%table%', p_table),'%sortkey%',p_sortkey);
        --RAISE NOTICE '%', p_sortkey;
        EXECUTE l_sql;
        RETURN TRUE;

    END IF;


END;
$$ LANGUAGE plpgsql