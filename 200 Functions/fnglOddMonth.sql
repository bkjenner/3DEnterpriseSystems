DROP FUNCTION IF EXISTS S0000V0000.fnGLOddMonth CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnGLOddMonth(p_StartMonth DATE = NULL,
                                               p_EndMonth DATE = NULL,
                                               p_GlAccountID BIGINT = NULL, -- Net Income
                                               p_GLCostCentreID BIGINT = NULL, -- Highest CostCentre
                                               p_Rows INT = 100,
                                               p_ValueType CHAR(1) = 'f',
                                               p_MinPercentage INT = NULL,
                                               p_MinAmount DECIMAL(19, 2) = NULL,
                                               p_normalpercent int = 10,
                                               p_oddpercent int = 20
)
    RETURNS
        TABLE
        (
            GLACCOUNTID    BIGINT,
            GLACCOUNT      varchar,
            GLCOSTCENTREID BIGINT,
            GLCOSTCENTRE   varchar,
            ReportingDate  DATE,
            CurrentMonthAmount DECIMAL(19, 2),
            PreviousMonthAmount DECIMAL(19, 2),
            DiffAmount DECIMAL(19, 2),
            DiffPercent DECIMAL(9, 2)
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
This function checks for situations where we see odd months.  An odd month is where we see a recurring pattern
of a number but one month in the middle has a different value.

p_StartMonth       - Starting month to check.  Defaults to beginning of fiscal year.
p_EndMonth         - Ending month to check.  Defaults to end of fiscal year.
p_GLAccountID      - If passed, the query results will traverse down from p_GLAccountID.  If it is not passed,
                     it will default to the Income Statement account.
p_GLCostCentreID   - If passed, glAccountBalance information will be selected based on p_GLCostCentreID.  If null is
                     passed then the GLCostCentre defaults to the highest level (root) cost centre.
p_Rows             - Number of rows to return.  Defaults to 100.
p_ValueType        - Allowable values are 'f' fiscal period amount or 'r' Reporting Period Amount
p_MinPercentage    - Minimum percentage changes to look for.
p_MinimumAmount    - Minimum amount to check for.

20210328    Blair Kjenner   Initial Code

select * from fnGLOddMonth() (p_StartMonth:= date '2020-01-01', p_EndMonth:= date '2020-12-31',p_GlAccountID:=2400, p_GLCostCentreID:=0,p_Rows:=100,p_ValueType:='f', p_MinAmount:=1)

*/
DECLARE
    l_SourceVariable      varchar;
    l_SQL                 varchar;
    l_StartMonth          DATE;
    l_EndMonth            DATE;
    l_fiscalstartdate   DATE;
    l_fiscalenddate     DATE;
BEGIN

    p_ValueType := LOWER(p_ValueType);

    SELECT a.fiscalstartdate, a.fiscalenddate INTO l_fiscalstartdate, l_fiscalenddate FROM glsetup a LIMIT 1;
    l_StartMonth := COALESCE(p_StartMonth, l_fiscalstartdate);
    l_EndMonth := COALESCE(p_EndMonth, l_fiscalenddate);
    l_StartMonth := l_StartMonth - DAY(l_StartMonth) + 1; --BOM
    l_EndMonth := l_EndMonth - DAY(l_EndMonth) + 1;
    l_SourceVariable := case when p_ValueType = 'f' THEN 'GLAccountAmount' ELSE 'GLReportingPeriodAmount' end;
    --BOM

-- Min Percent has to be negated because it operates opposite of what you would expect.
    p_MinPercentage := p_MinPercentage * -1;

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
    l_SQL := 'select
b.ID as GLAccountID
, b.Description Account
, e.ID as GLCostCentreID
, e.Description CostCentre
, CM.SummaryDate Period
, d.normalizationfactor * CM.' || l_SourceVariable || ' as CurrentMonth
, d.normalizationfactor * (PM1.' || l_SourceVariable || ') as PreviousMonth
, d.normalizationfactor * DiffAmount.calc
, DiffPercent.calc
from GLAccountBalance CM
join GLAccount b on b.ID = CM.glAccountID
left Join GLAccountType d on d.id=b.GLAccountTypeid';
    IF p_GLAccountID IS NOT NULL
    THEN
        l_SQL := l_SQL || '
join t_GLAccounts c on c.id=b.id';
    END IF;
    l_SQL := l_SQL || '
join GLCostCentre e on e.ID = CM.glCostCentreID';
    IF p_GLCostCentreID IS NOT NULL
    THEN
        l_SQL := l_SQL || '
join t_GLCostCentre f on f.id=e.id';
    END IF;
    l_SQL := l_SQL || replace(replace(replace('
-- Check 5 concurrent months to see if an odd month exists in the middle
join glAccountBalance PM1 on PM1.GLAccountID=CM.GLAccountID and PM1.GLCostCentreID=CM.GLCostCentreID
and PM1.summarydate = dateadd(''m'',-1,CM.summarydate)::date
join glAccountBalance PM2 on PM2.GLAccountID=CM.GLAccountID and PM2.GLCostCentreID=CM.GLCostCentreID
and PM2.summarydate = dateadd(''m'',-2,CM.summarydate)::date
join glAccountBalance NM1 on NM1.GLAccountID=CM.GLAccountID and NM1.GLCostCentreID=CM.GLCostCentreID
and NM1.summarydate = dateadd(''m'',1,CM.summarydate)::date
join glAccountBalance NM2 on NM2.GLAccountID=CM.GLAccountID and NM2.GLCostCentreID=CM.GLCostCentreID
and NM2.summarydate = dateadd(''m'',2,CM.summarydate)::date
JOIN LATERAL (select case when coalesce(PM1.%Amount%,0) <> 0
		and sign(PM1.%Amount%) = sign(NM1.%Amount%) and (ABS((NM1.%Amount%-PM1.%Amount%)/PM1.%Amount%)*100)<=%normalpercent%
		and sign(PM1.%Amount%) = sign(NM2.%Amount%) and (ABS((NM2.%Amount%-PM1.%Amount%)/PM1.%Amount%)*100)<=%normalpercent%
		and sign(PM1.%Amount%) = sign(PM2.%Amount%) and (ABS((PM2.%Amount%-PM1.%Amount%)/PM1.%Amount%)*100)<=%normalpercent%
		and sign(PM1.%Amount%) = sign(coalesce(CM.%Amount%,PM1.%Amount%)) and (ABS((CM.%Amount%-coalesce(PM1.%Amount%,0))/PM1.%Amount%)*100)>=%oddpercent%
		then TRUE else FALSE end calc) RPOdd ON TRUE
JOIN LATERAL (select coalesce(CM.%Amount%,0)-PM1.%Amount% Calc) DiffAmount ON TRUE
JOIN LATERAL (select (case when PM1.%Amount% = 0 then 1000 else DiffAmount.calc/PM1.%Amount%*100 end) Calc) DiffPercent ON TRUE
where COALESCE(CM.%Amount%,0) <> 0','%Amount%', l_SourceVariable), '%normalpercent%', p_normalpercent::text), '%oddpercent%', p_oddpercent::text) ;
        l_SQL := l_SQL || '
and cm.summarydate >=date ''' || TO_CHAR(l_StartMonth, 'yyyy-mm-dd') || '''
and cm.summarydate <=date ''' || TO_CHAR(l_EndMonth, 'yyyy-mm-dd') || '''';
    IF p_MinAmount IS NOT NULL
    THEN
        l_SQL := l_SQL || '
and RPOdd.calc=TRUE
and DiffAmount.calc*d.normalizationfactor >= ' || CAST(p_MinAmount AS varchar);
    END IF;
    IF p_MinPercentage IS NOT NULL
    THEN
        l_SQL := l_SQL || '
and DiffPercent.calc*d.normalizationfactor ' || CASE
            WHEN p_MinPercentage < 0
                THEN '<='
            ELSE '>='
            END ||
                 CAST(p_MinPercentage AS varchar);
    END IF;
    l_sql := l_sql || '
order by abs(DiffAmount.calc) desc
' || CASE
        WHEN p_Rows IS NULL
            THEN ''
        ELSE 'LIMIT ' || CAST(p_Rows AS varchar)
        END;
    RAISE NOTICE '%', l_SQL;
    RETURN QUERY EXECUTE l_sql;
END;
$$ LANGUAGE plpgsql
