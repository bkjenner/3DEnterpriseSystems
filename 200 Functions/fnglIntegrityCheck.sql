DROP FUNCTION IF EXISTS S0000V0000.fnglIntegrityCheck (p_table varchar) CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnglIntegrityCheck(p_table varchar DEFAULT 'all')
    RETURNS TABLE
            (
                TABLENAME         varchar,
                ERRORMESSAGE      varchar,
                ERRORMESSAGESTATE varchar
            )
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
This function checks the integrity of the financial system

20210317 Blair Kjenner	Initial Code

if exists(select fnglIntegrityCheck ('glaccount'))

TO DO
- recursive checks
- transaction balancing
- billing account glaccount and glentry glaccount match
- setup fields are filled in (fiscal year start, etc)

*/
DECLARE
    row       RECORD;
    l_message TEXT;
    l_return  RECORD;
BEGIN
    DROP TABLE IF EXISTS l_errormessage;
    CREATE TEMP TABLE l_errormessage
    (
        tablename         varchar,
        errormessage      varchar,
        errormessagestate varchar
    );

    IF LOWER(p_table) = 'glaccount' OR COALESCE(p_table, 'all') ilike 'all'
    THEN
        IF (
               SELECT COUNT(*)
               FROM glaccount
               WHERE glaccounttypeid = 10) <> 1 --Balance Account
        THEN
            INSERT INTO l_errormessage
            SELECT 'glaccount', description, state
            FROM sysmessage
            WHERE state = '51028';
        END IF;

        IF (
               SELECT COUNT(*)
               FROM glaccount
               WHERE glaccounttypeid = 50) <> 1 --Net Income Account
        THEN
            INSERT INTO l_errormessage
            SELECT 'glaccount', description, state
            FROM sysmessage
            WHERE state = '51029';
        END IF;

        IF (
               SELECT COUNT(*) FROM glaccount WHERE glaccounttypeid = 10 AND glaccountidparent IS NULL) <>
           1 --Balance Account
        THEN
            INSERT INTO l_errormessage
            SELECT 'glaccount', description, state
            FROM sysmessage
            WHERE state = '51030';
        END IF;

        DROP TABLE IF EXISTS t_glAccountHierarchy;

        CREATE TEMP TABLE t_glAccountHierarchy
        (
            RootID            BIGINT,
            glAccountIDParent BIGINT,
            ID                BIGINT,
            IsRecursive       BOOLEAN
        );

        CREATE INDEX glaccounthierarchy_rootid_id_index
            ON t_glAccountHierarchy (RootID, ID);

        INSERT INTO t_glAccountHierarchy (RootID, glAccountIDParent, ID, IsRecursive)
        WITH RECURSIVE Paths(RootID,
                             glAccountIDParent,
                             ID,
                             IDArray,
                             IsRecursive)
                           AS (
                                  -- 						   	  The beginning select gets all glaccount records that have no children (i.e. the
                                  -- 					          bottom of the hierarchy).  It also gets the Net Income account because it is
                                  -- 					          the only glaccount that can be posted to that is not at the bottom of the hierarchy.
                                  SELECT DISTINCT a.ID         RootID,
                                                  a.glAccountIDParent,
                                                  a.ID         ID,
                                                  --                                            To check for recursion we add the ids to an array and
                                                  --                                            then check to see if we encounter then again.
                                                  ARRAY [a.id] IDArray,
                                                  FALSE        IsRecursive
                                  FROM glAccount a
                                  LEFT JOIN glAccount b
                                            ON b.glAccountIDParent = a.id
                                  WHERE a.glaccounttypeid = 50 --Net Income
                                     OR b.id IS NULL
                                  UNION ALL
                                  -- 							  This loops from the bottom up to get every parent of every record.
                                  SELECT p.RootID,
                                         C.glAccountIDParent,
                                         C.ID,
                                         p.IDArray || c.ID,
                                         c.glAccountIDParent = ANY (p.IDArray) IsRecursive
                                  FROM glAccount AS C
                                  JOIN Paths AS p
                                       ON p.glAccountIDParent = C.id
                                  WHERE p.IsRecursive = FALSE
            )

        SELECT RootID, glAccountIDParent, ID, IsRecursive
        FROM Paths;

        IF EXISTS(SELECT 0 FROM t_glAccountHierarchy WHERE IsRecursive = TRUE LIMIT 1)
        THEN
            INSERT INTO l_errormessage
            SELECT 'glaccount', description, state
            FROM sysmessage
            WHERE state = '51032';
        END IF;

    END IF;

    IF LOWER(p_table) = 'glcostcentre' OR COALESCE(p_table, 'all') ILIKE 'all'
    THEN

        IF (
               SELECT COUNT(*)
               FROM glcostcentre
               WHERE glcostcentreidparent IS NULL
               AND RowStatus='a') <> 1
        THEN
            INSERT INTO l_errormessage
            SELECT 'glcostcentre', description, state
            FROM sysmessage
            WHERE state = '51031';
        END IF;

        DROP TABLE IF EXISTS t_glCostCentreHierarchy;

        CREATE TEMP TABLE t_glCostCentreHierarchy
        (
            RootID               BIGINT,
            glCostCentreIDParent BIGINT,
            ID                   BIGINT,
            IsRecursive          BOOLEAN
        );

        INSERT INTO t_glCostCentreHierarchy (RootID, glCostCentreIDParent, ID, IsRecursive)
        WITH RECURSIVE Paths(RootID,
                             glCostCentreIDParent,
                             ID,
                             IDArray,
                             IsRecursive)
                           AS (
                                  -- 						   	  The beginning select gets all glcostcentre records that have no child (i.e. the
                                  -- 					          bottom of the hierarchy).
                                  SELECT a.ID         RootID,
                                         a.glCostCentreIDParent,
                                         a.ID         ID,
                                         ARRAY [a.id] IDArray,
                                         FALSE        IsRecursive
                                  FROM glCostCentre a
                                  LEFT JOIN glCostCentre b
                                            ON b.glCostCentreIDParent = a.id
                                  WHERE b.ID IS NULL
                                  UNION ALL
                                  SELECT p.RootID,
                                         C.glCostCentreIDParent,
                                         C.ID,
                                         p.IDArray || c.ID,
                                         c.glCostCentreIDParent = ANY (p.IDArray) IsRecursive
                                  FROM glCostCentre AS C
                                  JOIN Paths AS p
                                       ON p.glCostCentreIDParent = C.id
                                  WHERE p.IsRecursive = FALSE
            )
        SELECT RootID,
               glCostCentreIDParent,
               ID,
               IsRecursive
        FROM Paths;

        IF EXISTS(SELECT 0 FROM t_glCostCentreHierarchy WHERE IsRecursive = TRUE LIMIT 1)
        THEN
            SELECT 'glcostcentre', description, state FROM sysmessage WHERE state = '51033';
        END IF;

    END IF;
    RETURN QUERY SELECT * FROM l_errormessage;
END;

$$ LANGUAGE plpgsql;
