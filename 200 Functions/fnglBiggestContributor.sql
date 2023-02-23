DROP FUNCTION IF EXISTS S0000V0000.fnGLBiggestContributor CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnGLBiggestContributor(p_ReportingDate DATE = NULL,
                                                  p_GlAccountID BIGINT = NULL,
                                                  p_GLCostCentreID BIGINT = NULL,
                                                  p_Rows INT = NULL,
                                                  p_ValueType CHAR(1) = 'f',
                                                  p_Period varchar = 'm',
                                                  p_StatementType varchar = 'is',
                                                  p_All BOOLEAN = FALSE,
                                                  p_CallingReport varchar = 'glaccount'
)
    RETURNS TABLE
            (
                SummaryDate           DATE,
                GLAccountID           BIGINT,
                GLAccount             varchar,
                GLCostCentreID        BIGINT,
                GLCostCentre          varchar,
                Amount                DECIMAL(19, 2)
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
This function allows users to narrow down what was the main contributing factor that caused a difference
between periods to occur.

p_ReportingDate    - Query date used for the date of the financial information to be displayed,
p_GLAccountID      - If passed, the query results will traverse down from p_GLAccountID.  If it is not passed,
                     it will default to the Income Statement account.
p_GLCostCentreID   - If passed, glAccountBalance information will be selected based on p_GLCostCentreID.  If null is
                     passed then the GLCostCentre defaults to the highest level (root) cost centre.
p_ValueType        - Allowable values are 'f' fiscal period amount or 'r' Reporting Period Amount
p_Period           - 'm' Month Amount or 'y' Annual Amount
p_StatementType    - Allowable values are 'is' Income Statement or 'bs' Balance Sheet
p_All              - Causes the report to analyze all child records
p_CallingReport    - The browse that called this procedure.  This in combination with p_all determines whether
                     to recurse down the cost centre hierarchy or the account hierarchy.  For example,
                     if we are called from glaccount browse and the user wishes to include all child
                     records then we need to recurse down the account hierarchy based on the glaccountid
                     passed.

20210325    Blair Kjenner   Initial Code

select * from fnGLBiggestContributor (p_ReportingDate:='2020-11-30',p_GLCostCentreID:=0 ,p_Rows:=NULL,p_GlAccountID:=2400,p_ValueType:='f',p_Period:='y', p_StatementType:='is', p_All:=FALSE, p_CallingReport := 'glaccount')

*/
DECLARE
    l_TopGLCostCentreID BIGINT;
    l_Variable1         varchar;
    l_SQL               varchar;
    l_periodStart       DATE;
    l_fiscalenddate     DATE;
    BEGIN
    p_callingreport := LOWER(p_callingreport);
    p_StatementType := LOWER(p_StatementType);
    p_ValueType := LOWER(p_ValueType);

    p_ValueType := LOWER(p_ValueType);
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

    IF p_GLCostCentreID IS NULL
    THEN
        SELECT a.id
        INTO p_GLCostCentreID
        FROM GLCostCentre a
        WHERE a.glCostCentreidparent IS NULL
          AND a.rowstatus = 'a'
        LIMIT 1;
    END IF;

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
    CREATE TEMP TABLE t_GLAccounts
    (
        id BIGINT
    );
    IF p_All = FALSE
        AND p_CallingReport = 'glaccount'
    THEN
        INSERT INTO t_GLAccounts
        VALUES (p_GlAccountID);
    ELSE
        WITH RECURSIVE Accounts (ID,
                                 topdownlevel,
                                 lowestlevel)
                           AS (
                                  SELECT a.id,
                                         a.topdownlevel,
                                         CASE
                                             WHEN a.BottomUpLevel = 1
                                                 THEN 1
                                                 ELSE 0
                                             END LowestLevel
                                  FROM GLAccount a
                                  WHERE a.id = p_GLAccountID
                                  UNION ALL
                                  SELECT C.ID,
                                         C.topdownlevel,
                                         CASE
                                             WHEN C.BottomUpLevel = 1
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
        WHERE lowestlevel = 1;
    END IF;
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
    IF (p_All = FALSE
        AND p_CallingReport = 'glcostcentre')
        OR p_GLCostCentreID = l_TopGLCostCentreID
    THEN
        INSERT INTO t_GLCostCentre
        VALUES (p_GLCostCentreID);
    ELSE
        WITH RECURSIVE GLCostCentre(ID,
                                    TopDownLevel,
                                    LowestLevel)
                           AS (
                                  SELECT a.id,
                                         a.TopDownLevel,
                                         CASE
                                             WHEN a.BottomUpLevel = 1
                                                 THEN 1
                                                 ELSE 0
                                             END LowestLevel
                                  FROM GLCostCentre a
                                  WHERE id = p_GLCostCentreID
                                  UNION ALL
                                  SELECT C.ID,
                                         C.TopDownLevel,
                                         CASE
                                             WHEN C.BottomUpLevel = 1
                                                 THEN 1
                                                 ELSE 0
                                             END LowestLevel
                                  FROM GLCostCentre AS C
                                  INNER JOIN GLCostCentre AS p
                                             ON p.ID = C.GLCostCentreIDParent)
        INSERT
        INTO t_GLCostCentre(ID)
        SELECT ID
        FROM GLCostCentre
        WHERE lowestlevel = 1;
    END IF;
    l_Variable1 := (
                       SELECT CASE
                                  WHEN p_Period = 'm'
                                      AND p_ValueType = 'f'
                                      THEN 'GLAccountAmount'
                                  WHEN p_Period = 'm'
                                      AND p_ValueType = 'r'
                                      THEN 'GLReportingPeriodAmount'
                                  WHEN p_Period = 'y'
                                      AND p_ValueType = 'f'
                                      THEN 'GLAccountAmountYTD'
                                  WHEN p_Period = 'y'
                                      AND p_ValueType IN ('r')
                                      THEN 'GLReportingPeriodAmountYTD'
                                  END);
    l_SQL := 'select
  a.SummaryDate
, a.GLAccountID
, b.Description Account
, a.GLCostCentreID
, c.Description costCentre
, COALESCE(e.normalizationfactor * a.' || l_Variable1 || ',0) as ActualAmount
from GLAccountBalance a
join GLAccount b on b.ID = a.GLAccountID and b.id in (select ID from t_GLAccounts)
left Join GLAccountType e on e.id=b.GLAccountTypeid
join GLCostCentre c on c.ID = a.GLCostCentreID ';
    IF p_GLCostCentreID = l_TopGLCostCentreID
    THEN
        l_SQL := l_SQL || CASE
                              WHEN p_StatementType = 'bs'
                                  THEN ''
                                  ELSE ' and coalesce(c.bottomuplevel,1) = 1'
                              END;
    ELSE
        l_SQL := l_SQL || ' and c.id in (select ID from t_GLCostCentre)';
    END IF;
    l_SQL := l_SQL || '
where a.' || l_Variable1 || ' <> 0';
    IF p_Period = 'm'
    THEN
        l_SQL := l_SQL || '
and a.SummaryDate =date ''' || TO_CHAR(p_ReportingDate, 'yyyy-mm-dd') || '''';
    ELSE
        l_SQL := l_SQL || '
and a.SummaryDate <= date ''' || TO_CHAR(p_ReportingDate, 'yyyy-mm-dd') || '''
and a.SummaryDate >= date ''' || TO_CHAR(l_periodStart, 'yyyy-mm-dd') || '''';
    END IF;
    l_SQL := l_SQL || '
order by a.' || l_Variable1 || ' desc
' || CASE
      WHEN p_Rows IS NULL
          THEN ''
          ELSE 'LIMIT ' || CAST(p_Rows AS varchar)
      END;
--       RAISE NOTICE '''p_GLCostCentreID'' % ''p_GlAccountID'' % ''l_fiscalenddate'' % ''l_SQL'' %  '
--         , p_GLCostCentreID, p_GlAccountID , l_fiscalenddate, l_SQL;
      RETURN QUERY EXECUTE l_sql;
END;
$$ LANGUAGE plpgsql

