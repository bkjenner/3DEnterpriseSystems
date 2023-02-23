-- This function is still under development
DROP FUNCTION IF EXISTS S0000V0000.fnGLBiggestChange CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnGLBiggestChange(p_ReportingDate DATE = null,
                                                    p_GlAccountID INT = null,
                                                    p_GLCostCentreID INT = null,
                                                    p_CostCentreLevel INT = 1,
                                                    p_AccountLevel INT = 4,
                                                    p_Rows INT = 100,
                                                    p_ValueType CHAR(1) = 'f',
                                                    p_ComparisonType CHAR(1) = 'p',
                                                    p_Period varchar = 'm'
)
    RETURNS
        TABLE
        (
            SUMMARYDATE    DATE,
            GLACCOUNTID    BIGINT,
            GLACCOUNT      varchar,
            GLCOSTCENTREID BIGINT,
            GLCOSTCENTRE   varchar,
            AMOUNT         DECIMAL(19, 2)
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
This function allows users to identify the biggest change that occurred for a given period.

p_ReportingDate    - Query date used for the date of the financial information to be displayed,
p_GLAccountID      - If passed, the query results will traverse down from p_GLAccountID.  If it is not passed,
                     it will default to the Income Statement account.
p_GLCostCentreID   - If passed, glAccountBalance information will be selected based on p_GLCostCentreID.  If null is
                     passed then the GLCostCentre defaults to the highest level (root) cost centre.
p_CostCentreLevel  - Number of levels down to be checked for the cost centre hierarchy
p_AccountLevel     - Number of levels down to be checked for the account hierarchy
p_Rows             - Number of rows to be returned
p_ValueType        - Allowable values are 'f' fiscal period amount or 'r' Reporting Period Amount
p_ComparisonType   - 'p' Previous Period or 'b' Budget
p_Period           - 'm' Month, 'y' Year

20210325    Blair Kjenner   Initial Code

select * from fnGLBiggestChange(p_ReportingDate := date '2020-11-30', p_GlAccountID := 35170, p_GLCostCentreID := 35141, 
p_CostCentreLevel := 1, p_AccountLevel := 4, p_Rows := 100, p_ValueType := 'f', p_ComparisonType := 'p', p_Period := 'm')

*/
DECLARE
    l_prevYear             DATE;
    l_TopGLCostCentreID    BIGINT;
    l_Variable1            varchar;
    l_Variable2            varchar;
    l_SQL                  varchar;
    l_FirstAccountLevel    INT;
    l_FirstCostCentreLevel INT;
    l_periodStart          DATE;
    l_FiscalEndDate        DATE;
    l_TotalChange          DECIMAL(19, 2);
BEGIN
    p_ValueType := LOWER(p_ValueType);
    p_ComparisonType := LOWER(p_ComparisonType);

    SELECT a.fiscalenddate INTO l_fiscalenddate FROM glsetup a LIMIT 1;
    IF p_ReportingDate IS NULL
    THEN
        p_ReportingDate:=l_fiscalenddate;
    END IF;

    p_ReportingDate := p_ReportingDate - DAY(p_ReportingDate) + 1; --BOM

    l_periodStart := CASE
                         WHEN p_Period = 'm'
                             THEN p_ReportingDate
                             ELSE fyearstart(p_ReportingDate, l_fiscalenddate)
                         END;

    DROP TABLE IF EXISTS t_GLAccounts;
    CREATE TEMP TABLE t_GLAccounts
    (
        id           INT,
        topdownlevel INT
    );
    l_FirstAccountLevel := (
                               SELECT a.topdownlevel
                               FROM GLAccount a
                               WHERE a.ID = p_GLAccountID
                               ORDER BY a.displaysequence);
    p_AccountLevel := p_AccountLevel + l_FirstAccountLevel - 1;
    l_FirstCostCentreLevel := (
                                  SELECT a.topdownlevel
                                  FROM GLCostCentre a
                                  WHERE a.ID = p_GLCostCentreID
                                  ORDER BY a.displaysequence);
    p_CostCentreLevel := p_CostCentreLevel + l_FirstCostCentreLevel - 1;
    WITH RECURSIVE Accounts(ID,
                            topdownlevel,
                            lowestlevel)
                       AS (
                              SELECT a.id,
                                     a.topdownlevel,
                                     CASE
                                         WHEN a.glAccountIDParent IS NOT NULL
                                             THEN 1
                                             ELSE 0
                                         END
                              FROM GLAccount a
                              WHERE a.id = p_GLAccountID
                              UNION ALL
                              SELECT C.ID,
                                     C.topdownlevel,
                                     CASE
                                         WHEN C.glAccountIDParent IS NOT NULL
                                             THEN 1
                                             ELSE 0
                                         END
                              FROM GLAccount AS C
                              INNER JOIN Accounts AS p
                                         ON p.ID = C.GLAccountIDParent)
    INSERT
    INTO t_GLAccounts(ID)
    SELECT ID
    FROM Accounts
    WHERE (p_AccountLevel IS NULL
        AND lowestlevel = 1)
       OR topdownlevel = p_AccountLevel;
    l_TopGLCostCentreID := (
                               SELECT id
                               FROM GLCostCentre
                               WHERE GLCostCentreIDparent IS NULL
                                 AND rowstatus = 'a'
                               LIMIT 1);
    DROP TABLE IF EXISTS t_GLCostCentre;
    CREATE TEMP TABLE t_GLCostCentre
    (
        id BIGINT
    );
    IF p_GLCostCentreID <> l_TopGLCostCentreID
    THEN
        WITH GLCostCentre(ID,
                          TopDownLevel,
                          LowestLevel)
                 AS (
                        SELECT a.id,
                               a.TopDownLevel,
                               CASE
                                   WHEN a.glCostCentreIDParent IS NOT NULL
                                       THEN 1
                                       ELSE 0
                                   END
                        FROM GLCostCentre a
                        WHERE a.id = p_GLCostCentreID
                        UNION ALL
                        SELECT C.ID,
                               C.TopDownLevel,
                               CASE
                                   WHEN glCostCentreID IS NOT NULL
                                       THEN 1
                                       ELSE 0
                                   END
                        FROM GLCostCentre AS C
                        INNER JOIN GLCostCentre AS p
                                   ON p.ID = C.GLCostCentreIDParent)
        INSERT
        INTO t_GLCostCentre(ID)
        SELECT ID
        FROM GLCostCentre
        WHERE (p_CostCentreLevel IS NULL
            AND lowestlevel = 1)
           OR topdownlevel = p_CostCentreLevel;
    END IF;
    l_Variable1 := (
                       SELECT CASE
                                  WHEN p_ValueType = 'f'
                                      THEN 'GLAccountAmount'
                                  WHEN p_ValueType = 'r'
                                      THEN 'GLReportingPeriodAmount'
                                  END);
    l_Variable2 := (
                       SELECT CASE
                                  WHEN p_ComparisonType = 'm'
                                      AND p_ValueType = 'f'
                                      THEN 'd.GLAccountAmount'
                                  WHEN p_ComparisonType = 'm'
                                      AND p_ValueType = 'r'
                                      THEN 'd.GLReportingPeriodAmount'
                                  WHEN p_ComparisonType = 'b'
                                      AND p_ValueType IN ('f')
                                      THEN 'a.GLBudgetAmount'
                                  WHEN p_ComparisonType = 'b'
                                      AND p_ValueType IN ('r')
                                      THEN 'a.GLBudgetAmount'
                                  END);
    l_SQL := 'Select
p_TotalChange=COALESCE(a.' + l_Variable1 + ',0)-COALESCE(' + l_Variable2 + ',0)
from glAccountBalance a ';
    IF p_ComparisonType = 'p'
    THEN
        l_SQL := l_SQL + '
left join glAccountBalance d on d.GLAccountID=a.GLAccountID
and d.GLCostCentreID=a.GLCostCentreID
and d.summarydate = cast(cast(left(a.summarydate,4) as int)-1 as varchar)+right(a.summarydate,2) ';
    END IF;
    l_SQL := l_SQL + '
where a.summarydate=''' + p_ReportingDate + '''
and a.GLCostCentreID=' + CAST(p_GLCostCentreID AS varchar) + '
and a.glaccountid=' + CAST(p_GLAccountID AS varchar);
END;
$$ LANGUAGE plpgsql
/*
RAISE NOTICE ‘%’,  l_SQL;
EXEC sp_executesql
l_sql,
p_TotalChange money OUTPUT ',
p_TotalChange = l_TotalChange OUTPUT;
l_SQL := '
SELECT ' + CASE
WHEN p_Rows IS NULL
THEN ''
ELSE '                                                                                               top
' + CAST(p_Rows AS varchar)
END + '
a.summarydate
, a.GLAccountID
, b.Description                                                                             Account
, a.GLCostCentreID
, C.Description                                                                             costCentre
, COALESCE(e.normalizationfactor * a.' + l_Variable1 + ', 0)                             AS ActualAmount
, COALESCE(e.normalizationfactor * ' + l_Variable2 + ', 0)                               AS ComparisonAmount
, f.Difference
, CAST(f.Difference / ' + CAST(l_TotalChange AS varchar) + ' * 100 AS DECIMAL(8, 5)) AS PercentageOfTotal
FROM glAccountBalance a
JOIN      GLAccount b
ON b.ID = a.GLAccountID
LEFT JOIN GLAccountType e
ON e.id = b.GLAccountTypeid
JOIN      GLCostCentre C
ON C.ID = a.GLCOSTCENTREID ';
IF p_ComparisonType = 'p'
l_SQL := l_SQL + '
LEFT JOIN glAccountBalance D
ON D.
GLAccountID = a.GLAccountID
      AND D.GLCostCentreID = a.GLCostCentreID
      AND
  D.summarydate = CAST(CAST(LEFT(a.summarydate, 4) AS INT) - 1 AS varchar) + RIGHT(a.summarydate, 2)
  ';
 l_SQL := l_SQL + ' JOIN LATERAL (SELECT COALESCE(a.' + l_Variable1 + ',0)-COALESCE(' + l_Variable2 + ',0) Difference) f
WHERE a.summarydate >=''' + l_periodStart + '''
AND a.summarydate <=''' + p_ReportingDate + '''
AND a.glaccountid IN (SELECT ID FROM t_GLAccounts)
AND ((' + CAST(p_GLCostCentreID AS varchar) + '=' + CAST(l_TopGLCostCentreID AS varchar) + '
AND ((' + CAST(p_CostCentreLevel AS varchar) + ' IS NULL
AND C.glCostCentreID IS NOT NULL
AND C.glCostCentreID <> ''1'')
OR C.topdownlevel=' + CAST(p_CostCentreLevel AS varchar) + '))
OR
(a.GLCostCentreID IN (SELECT ID FROM t_GLCostCentre)))
ORDER BY abs(f.Difference) DESC';
RAISE NOTICE ‘%’,  l_SQL;
qq-exec (l_SQL)
SELECT TOP 100 a.summarydate,
a.GLAccountID,
b.Description Account,
a.GLCostCentreID,
c.Description costCentre,
COALESCE(e.normalizationfactor * a.FPActualAmount, 0) AS ActualAmount,
COALESCE(e.normalizationfactor * d.FPActualAmount, 0) AS ComparisonAmount,
f.Difference,
CAST(f.Difference / -126250631.78 * 100 AS DECIMAL(8, 5)) AS PercentageOfTotal
FROM glAccountBalance a
JOIN GLAccount b ON b.ID = a.GLAccountID
LEFT JOIN GLAccountType e ON e.id = b.GLAccountTypeid
JOIN GLCostCentre c ON c.ID = a.GLCostCentreID
LEFT JOIN glAccountBalance d ON d.GLAccountID = a.GLAccountID
AND d.GLCostCentreID = a.GLCostCentreID
AND d.summarydate = CAST(CAST(LEFT(a.summarydate, 4) AS INT) - 1 AS varchar) + RIGHT(a.summarydate, 2)
JOIN LATERAL
(
SELECT COALESCE(a.FPActualAmount, 0) - COALESCE(d.FPActualAmount, 0) Difference
) f
WHERE a.summarydate >= '201212'
AND a.summarydate <= '201212'
AND a.glaccountid IN
(
SELECT ID
FROM t_GLAccounts
)
AND ((50124 = 50124
AND ((1 IS NULL
AND c.glCostCentreID IS NOT NULL
AND c.glCostCentreID <> '1')
OR c.topdownlevel = 1))
OR (a.GLCostCentreID IN
(
SELECT ID
FROM t_GLCostCentre
)))
ORDER BY ABS(f.Difference) DESC;
End IF;
CALL spBiggestChange ()
*/



