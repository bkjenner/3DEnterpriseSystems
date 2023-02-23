DROP FUNCTION IF EXISTS S0000V0000.fnGLMissingPeriod CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnGLMissingPeriod(p_StartMonth DATE = NULL,
                                                    p_EndMonth DATE = NULL,
                                                    p_GlAccountID BIGINT = NULL,
                                                    p_GLCostCentreID INT = NULL,
                                                    p_Rows INT = 100,
                                                    p_ValueType CHAR(1) = 'f',
                                                    p_MinimumAmount DECIMAL(19, 2) = NULL
)
    RETURNS
        TABLE
        (
            GLACCOUNTID    BIGINT,
            GLACCOUNT      varchar,
            GLCOSTCENTREID BIGINT,
            GLCOSTCENTRE   varchar,
            PERIOD1DATE    DATE,
            PERIOD1AMOUNT  DECIMAL(19, 2),
            PERIOD2DATE    DATE,
            PERIOD2AMOUNT  DECIMAL(19, 2)
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
This function checks for situations where we see a charge in several months followed by a missed charge followed
by more charges.  The intent is to identify a missed expense or revenue.

p_StartMonth       - Starting month to check.  Defaults to beginning of fiscal year.
p_EndMonth         - Ending month to check.  Defaults to end of fiscal year.
p_GLAccountID      - If passed, the query results will traverse down from p_GLAccountID.  If it is not passed,
                     it will default to the Income Statement account.
p_GLCostCentreID   - If passed, glAccountBalance information will be selected based on p_GLCostCentreID.  If null is
                     passed then the GLCostCentre defaults to the highest level (root) cost centre.
p_Rows             - Number of rows to return.  Defaults to 100.
p_ValueType        - Allowable values are 'f' fiscal period amount or 'r' Reporting Period Amount
p_MinimumAmount    - Minimum amount to check for.

20210326    Blair Kjenner   Initial Code

select * from fnGLMissingPeriod (p_StartMonth:= date '1998-01-01', p_EndMonth:= date '2020-12-31', p_GLCostCentreID:=0,p_Rows:=100,p_GlAccountID:=2400,p_ValueType:='f', p_MinimumAmount:=1)

*/
DECLARE
    l_Variable1       varchar;
    l_SQL             varchar;
    l_StartMonth      DATE;
    l_EndMonth        DATE;
    l_fiscalstartdate DATE;
    l_fiscalenddate   DATE;
BEGIN
    p_ValueType := LOWER(p_ValueType);

    SELECT a.fiscalstartdate, a.fiscalenddate INTO l_fiscalstartdate, l_fiscalenddate FROM glsetup a LIMIT 1;
    l_StartMonth := COALESCE(p_StartMonth, l_fiscalstartdate);
    l_EndMonth := COALESCE(p_EndMonth, l_fiscalenddate);
    l_StartMonth := l_StartMonth - DAY(l_StartMonth) + 1; --BOM
    l_EndMonth := l_EndMonth - DAY(l_EndMonth) + 1; --BOM

    IF p_GLAccountID IS NULL
    THEN
        SELECT a.id
        INTO p_GLAccountID
        FROM glAccount a
        LEFT JOIN GLAccountType b
                  ON b.id = a.glaccounttypeid
        WHERE b.id = 50 --Net Income
        LIMIT 1;
    END IF;

    DROP TABLE IF EXISTS t_GLAccounts;
    CREATE TEMP TABLE T_GLACCOUNTS
    (
        id BIGINT
    );
    WITH RECURSIVE Accounts (ID,
                             bottomuplevel)
                       AS (
                              SELECT a.id,
                                     a.bottomuplevel
                              FROM GLAccount a
                              WHERE a.id = p_GLAccountID
                              UNION ALL
                              SELECT c.ID,
                                     C.bottomuplevel
                              FROM GLAccount AS C
                              INNER JOIN Accounts AS p
                                         ON p.ID = C.GLAccountIDParent)
    INSERT
    INTO t_GLAccounts(ID)
    SELECT ID
    FROM Accounts
    WHERE bottomuplevel = 1;

    IF P_GLCOSTCENTREID IS NOT NULL
    THEN
        DROP TABLE IF EXISTS t_GLCostCentre;
        CREATE TEMP TABLE t_GLCostCentre
        (
            id BIGINT
        );
        WITH RECURSIVE CostCentre(ID,
                                  BottomUpLevel)
                           AS (
                                  SELECT a.id,
                                         a.BottomUpLevel
                                  FROM GLCostCentre a
                                  WHERE a.id = p_GLCostCentreID
                                  UNION ALL
                                  SELECT C.ID,
                                         C.BottomUpLevel
                                  FROM GLCostCentre AS C
                                  INNER JOIN GLCostCentre AS p
                                             ON p.ID = C.GLCostCentreIDParent)
        INSERT
        INTO t_GLCostCentre(ID)
        SELECT ID
        FROM CostCentre
        WHERE BottomUpLevel = 1;
    END IF;
    l_Variable1 := CASE
        WHEN p_ValueType IN ('f')
            THEN 'GLAccountAmount'
        WHEN p_ValueType IN ('r')
            THEN 'GLReportingPeriodAmount'
        END;
    l_SQL := 'select
a.GLAccountID
, b.Description Account
, a.GLCostCentreID
, e.Description costCentre
, a.SummaryDate Period1Date
, COALESCE(d.normalizationfactor * a.' || l_Variable1 || ',0) as Amount1
, j.summarydate Period2Date
, COALESCE(d.normalizationfactor * j.' || l_Variable1 || ',0) as Amount2
from glAccountBalance a
join GLAccount b on b.ID = a.GLAccountID
left Join GLAccountType d on d.id=b.GLAccountTypeid';
    IF p_GLAccountID IS NOT NULL
    THEN
        l_SQL := l_SQL || '
join t_GLAccounts c on c.id=b.id';
    END IF;
    l_SQL := l_SQL || '
join GLCostCentre e on e.ID = a.GLCostCentreID';
    IF p_GLCostCentreID IS NOT NULL
    THEN
        l_SQL := l_SQL || '
join t_GLCostCentre f on f.id=e.id';
    END IF;
    l_SQL := l_SQL || '
-- Check if next month exists
left join glAccountBalance i on i.GLAccountID=a.GLAccountID
and i.GLCostCentreID=a.GLCostCentreID
and i.summarydate = dateadd(''m'',1,a.summarydate)::date
join glAccountBalance j on j.GLAccountID=a.GLAccountID
and j.GLCostCentreID=a.GLCostCentreID
and j.summarydate = dateadd(''m'',2,a.summarydate)::date
where i.id is null
and abs(a.' || l_Variable1 || ') <> 0
and a.summarydate >=date ''' || TO_CHAR(l_StartMonth, 'yyyy-mm-dd') || '''
and a.summarydate <=date ''' || TO_CHAR(l_EndMonth, 'yyyy-mm-dd') || '''';
    IF p_MinimumAmount IS NOT NULL
    THEN
        l_sql := l_sql || '
and abs(a.' || l_Variable1 || ') >= ' || CAST(p_MinimumAmount AS varchar);
    END IF;
    l_sql := l_sql || '
order by abs(a.' || l_Variable1 || ') desc
' || CASE
        WHEN p_Rows IS NULL
            THEN ''
        ELSE 'LIMIT ' || CAST(p_Rows AS varchar)
        END;
    RAISE NOTICE '%', l_SQL;
    RETURN QUERY EXECUTE l_sql;
END;
$$ LANGUAGE plpgsql
