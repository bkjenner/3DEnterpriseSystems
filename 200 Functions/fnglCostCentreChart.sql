DROP FUNCTION IF EXISTS S0000V0000.fnGLCostCentreChart;
CREATE OR REPLACE FUNCTION S0000V0000.fnGLCostCentreChart(p_ReportingDate DATE = NULL,
                                            p_GLCostCentreID BIGINT = NULL,
                                            p_GLAccountID BIGINT = NULL,
                                            p_ValueType CHAR(2)= 'pf',
                                            p_Months INT = 12,
                                            p_AllChildren BOOLEAN = FALSE
)
    RETURNS TABLE
            (
                ID            BIGINT,
                DESCRIPTION   varchar,
                VARIABLE      DECIMAL(19, 2),
                REPORTINGDATE DATE
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
This function returns data for a glCostCentre chart based on the parameters passed.

p_ReportingDate    - Query date used for the date of the financial information to be displayed,
p_GLCostCentreID   - The query results will traverse down 1 level from p_GLCostCentreID.
p_GLAccountID      - GLAccountBalance information will be selected based on p_GLAccountID.
p_ValueType        - pf - current period amount
                   - pr - current reporting period amount
                   - pb - current budget amount
                   - yf - ytd period amount
                   - yr - ytd reporting period amount
                   - yb - ytd budget amount
p_Months           - Number of months of data to be returned
p_AllChildren      - Causes the procedure to traverse all remaining glCostCentres below the p_GLCostCentreID

select * from fnGLCostCentreChart (p_ReportingDate:=date '2020-11-30',p_GLCostCentreID:=0,p_GLAccountID:=2400,p_ValueType:='YF',p_Months:=12,p_AllChildren:=false)

*/
DECLARE
    l_StartDate DATE;
    l_EndDate   DATE;
    l_SQL       varchar;
    l_Variable  varchar;
    l_glaccounttypeid BIGINT;
BEGIN
    p_ValueType := LOWER(p_ValueType);
    IF p_ReportingDate IS NULL
    THEN
        SELECT a.fiscalenddate INTO p_ReportingDate FROM glsetup a LIMIT 1;
    END IF;
    p_ReportingDate := p_ReportingDate - DAY(p_ReportingDate) + 1; --BOM
    l_EndDate := p_ReportingDate;
    l_StartDate := DATEADD('m', p_Months * -1, p_ReportingDate);

    SELECT b.id, a.id
    INTO l_glaccounttypeid, p_GLAccountID
    FROM glAccount a
    LEFT JOIN GLAccountType b
              ON b.id = a.glaccounttypeid
    WHERE (p_GLAccountID IS NOT NULL AND a.ID = p_GLAccountID)
       OR (p_GLAccountID IS NULL AND b.id = 50) --Net Income
    LIMIT 1;

    IF p_GLCostCentreID IS NULL
    THEN
        SELECT a.id
        INTO p_GLCostCentreID
        FROM glCostCentre a
        WHERE a.glcostcentreidparent is null --Top Level Cost Centre
        LIMIT 1;
    END IF;

    DROP TABLE IF EXISTS t_GLCostCentres;
    CREATE TEMP TABLE t_GLCostCentres
    (
        id                BIGINT,
        glCostCentreIDParent BIGINT
    );
    IF p_AllChildren = TRUE
    THEN
        WITH RECURSIVE CostCentres (ID,
                                 glCostCentreIDParent)
                           AS (
                                  SELECT a.id, a.glCostCentreidparent
                                  FROM GLCostCentre a
                                  WHERE a.id = p_GLCostCentreID
                                  UNION ALL
                                  SELECT c.ID,
                                         c.glCostCentreIDParent
                                  FROM GLCostCentre AS c
                                  INNER JOIN CostCentres AS p
                                             ON p.ID = c.GLCostCentreIDParent)
        INSERT
        INTO t_GLCostCentres(ID)
        SELECT a.id
        FROM CostCentres a
        WHERE a.glCostCentreIDParent IS NOT NULL; -- only want the lowest level CostCentres
    END IF;
    l_Variable := (
                      SELECT CASE
                                 WHEN p_ValueType = 'pf'
                                     THEN 'b.GLAccountAmount'
                                 WHEN p_ValueType = 'pr'
                                     THEN 'b.GLReportingPeriodAmount'
                                 WHEN p_ValueType = 'pb'
                                     THEN 'b.GLBudgetAmount'
                                 WHEN p_ValueType = 'yf'
                                     THEN 'b.GLAccountAmountYTD'
                                 WHEN p_ValueType = 'yr'
                                     THEN 'b.GLReportingPeriodAmountYTD'
                                 WHEN p_ValueType = 'yr'
                                     THEN 'b.GLBudgetAmountYTD'
                                 END);
    l_SQL := '
select
a.id
, a.Description
, COALESCE(e.normalizationfactor*' || l_Variable || ',0) Variable
, b.SummaryDate
from GLCostCentre a
left join GLAccountBalance b on b.GLCostCentreID=a.ID and b.GLAccountID=' || CAST(p_GLAccountID AS varchar) || '
and b.SummaryDate> DATE ''' || TO_CHAR(l_StartDate, 'yyyy-mm-dd') || ''' and b.SummaryDate <= date ''' ||
             TO_CHAR(l_EndDate, 'yyyy-mm-dd') || '''
left Join GLAccountType e on e.id=' || CAST(l_glaccounttypeid AS varchar) || '
where b.GLAccountAmount <> 0 ';
    IF p_AllChildren = TRUE
    THEN
        l_SQL := l_SQL || '
and a.id in (select aa.id from t_GLCostCentres aa)';
    ELSE
        l_SQL := l_SQL || '
and a.GLCostCentreIDParent=' || CAST(p_GLCostCentreID AS varchar) || ';';
    END IF;
    RAISE NOTICE '''l_startdate'' % ''l_enddate'' % ''l_Variable'' % ''l_glaccounttypeid'' % ''p_GLAccountID'' %  ''l_SQL'' % '
    , l_startdate, l_enddate, l_Variable, l_glaccounttypeid, p_GLAccountID, l_SQL;
    RETURN QUERY EXECUTE l_sql;

END;
$$ LANGUAGE plpgsql