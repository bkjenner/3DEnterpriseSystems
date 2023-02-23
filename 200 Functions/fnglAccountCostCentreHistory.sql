DROP FUNCTION IF EXISTS S0000V0000.fnGLAccountCostCentreHistory;
CREATE OR REPLACE FUNCTION S0000V0000.fnGLAccountCostCentreHistory(p_ReportingDate DATE = NULL,
                                                               p_GLCostCentreID BIGINT = NULL,
                                                               p_GLAccountID BIGINT = NULL,
                                                               p_Period varchar = 'm',
                                                               p_ValueType CHAR(1) = 'f',
                                                               p_ComparisonType CHAR(1) = 'x',
                                                               p_Rows INT = 12,
                                                               p_Statement varchar = 'is'
)
    RETURNS TABLE
            (
                CURRENTPERIOD  DATE,
                ENDPERIOD      DATE,
                STARTPERIOD    DATE,
                ACTUALCURRYEAR DECIMAL(19, 2),
                ACTUALLASTYEAR DECIMAL(19, 2)
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
This function summarizes account balance history based on the gl account and cost centre.

p_ReportingDate    - Query date used for the date of the financial information to be displayed.  Defaults
                     to the year end specified in the financial setup.
p_GLCostCentreID   - Can be any level in the Cost Centre hierarchy. Defaults to the highest level in the cost
                     centre hierarchy.
p_GLAccountID      - Can be any level in the GL Account hierarchy.  If it is null and the statement is
                     an income statement, then it defaults to the Net Income Account.  If the statement
                     is a balance sheet, then it defaults to the Balance Sheet account.
p_Period           - m - month, y - annual
p_ValueType        - Allowable values are 'f' fiscal period amount or 'r' Reporting Period Amount
p_ComparisonType   - Allowable values are 'p' Prev Year or 'b' Budget Amount
p_Rows             - Number of rows to return
p_Statement        - Allowable values are 'is' Income Statement or 'bs' Balance Sheet

select * from fnGLAccountCostCentreHistory(p_ReportingDate:='2020-11-30',p_GLCostCentreID:=0,p_GLAccountID:=310000,p_Period:='m',p_ValueType:='f',p_ComparisonType:='p',p_Rows:=12)

*/
DECLARE
    p_ReportingDate       DATE;
    l_NormalizationFactor INT;
    l_prevYear            DATE;
    l_Variable1           varchar;
    l_Variable2           varchar;
    l_SQL                 varchar;
    l_fiscalenddate       DATE;
BEGIN
    p_Statement := LOWER(p_Statement);
    p_Period := LOWER(p_Period);
    p_ValueType := LOWER(p_ValueType);
    p_ComparisonType := LOWER(p_ComparisonType);
    SELECT a.fiscalenddate INTO l_fiscalenddate FROM glsetup a LIMIT 1;
    p_ReportingDate := COALESCE(p_ReportingDate, l_fiscalenddate);
    p_ReportingDate := p_ReportingDate - DAY(p_ReportingDate) + 1; --BOM

    IF p_GLCostCentreID IS NULL
    THEN
        SELECT a.id
        INTO p_GLCostCentreID
        FROM GLCostCentre a
        WHERE a.glCostCentreidparent IS NULL
          AND a.rowstatus = 'a'
        LIMIT 1;
    END IF;

--      Set to default account to Income Statement if 'is' else balance sheet account
    if p_GLAccountID is NULL
    then
        p_GLAccountID = CASE WHEN p_statement = 'is' THEN 2400
                             ELSE 0
                             END;
    END IF;

    l_NormalizationFactor := (
                                 SELECT z.normalizationFactor
                                 FROM GLAccount b
                                 LEFT JOIN GLAccountType z
                                           ON z.id = b.GLAccountTypeid
                                 WHERE b.ID = p_GLAccountID);
    l_Variable1 := CASE
        WHEN p_Statement = 'is'
            AND p_ValueType = 'f'
            THEN 'b.GLAccountAmount'
        WHEN p_Statement = 'is'
            AND p_ValueType = 'r'
            THEN 'b.GLReportingPeriodAmount'
        WHEN p_Statement = 'bs'
            AND p_ValueType = 'f'
            THEN 'b.GLAccountAmountYTD'
        WHEN p_Statement = 'bs'
            AND p_ValueType = 'r'
            THEN 'b.GLReportingPeriodAmountYTD'
        END;
    l_Variable2 := CASE
        WHEN p_Statement = 'is'
            AND p_ComparisonType = 'x'
            THEN '0'
        WHEN p_Statement = 'is'
            AND p_ComparisonType = 'p'
            AND p_ValueType = 'f'
            THEN 'c.GLAccountAmount'
        WHEN p_Statement = 'is'
            AND p_ComparisonType = 'p'
            AND p_ValueType = 'r'
            THEN 'c.GLReportingPeriodAmount'
        WHEN p_Statement = 'is'
            AND p_ComparisonType = 'b'
            AND p_ValueType = 'f'
            THEN 'b.GLBudgetAmount'
        WHEN p_Statement = 'is'
            AND p_ComparisonType = 'b'
            AND p_ValueType = 'r'
            THEN 'b.GLBudgetAmount'
        WHEN p_Statement = 'is'
            AND p_ComparisonType = 'x'
            THEN '0'
        WHEN p_Statement = 'bs'
            AND p_ComparisonType = 'p'
            AND p_ValueType = 'f'
            THEN 'c.GLAccountAmountYTD'
        WHEN p_Statement = 'bs'
            AND p_ComparisonType = 'p'
            AND p_ValueType = 'r'
            THEN 'c.GLReportingPeriodAmountYTD'
        WHEN p_Statement = 'bs'
            AND p_ComparisonType = 'p'
            AND p_ValueType = 'v'
            THEN 'c.RPYTDActualVolume'
        WHEN p_Statement = 'bs'
            AND p_ComparisonType = 'b'
            AND p_ValueType = 'f'
            THEN 'b.GLBudgetAmountYTD'
        WHEN p_Statement = 'bs'
            AND p_ComparisonType = 'b'
            AND p_ValueType = 'r'
            THEN 'b.GLBudgetAmountYTD'
        WHEN p_Statement = 'bs'
            AND p_ComparisonType = 'x'
            THEN '0'
        END;
    l_SQL := 'select
z.CurrentPeriod
, max(b.summarydate) EndPeriod
, MIN(b.summarydate) StartPeriod
, sum(COALESCE(' || CAST(l_normalizationfactor AS varchar) || ' * ' || l_Variable1 || ',0)) ActualCurrYear
, cast(sum(COALESCE(' || CAST(l_normalizationfactor AS varchar) || ' * ' || l_Variable2 || ',0)) as decimal(19,2)) ActualLastYear
from glAccountBalance b
JOIN LATERAL (select ' ||
             CASE WHEN p_Period = 'y' THEN
                              ' fyearend(b.summarydate, date ''' || TO_CHAR(l_fiscalenddate, 'yyyy-mm-dd') || ''')'
                  ELSE 'b.summarydate'
                  END || ' CurrentPeriod) z ON TRUE';
    IF p_ComparisonType = 'p'
    THEN
        l_SQL := l_SQL || '
left join glAccountBalance c on b.GLCostCentreID=c.GLCostCentreID
and b.GLAccountID=c.GLAccountID
and dateadd(''y'', -1, z.CurrentPeriod) = c.SummaryDate';
    END IF;
    l_SQL := l_SQL || '
where b.GLCostCentreID=' || CAST(p_GLCostCentreID AS varchar) ||
             ' and b.GLAccountID=' || CAST(p_GLAccountID AS varchar) ||
             ' and b.summarydate <= date ''' || TO_CHAR(p_ReportingDate, 'yyyy-mm-dd') || '''
group by z.currentperiod
order by z.currentperiod desc
' || CASE
                 WHEN p_Rows IS NULL
                     THEN ''
                 ELSE 'LIMIT ' || CAST(p_Rows AS varchar)
                 END;
    RAISE NOTICE '%',  l_SQL;
    RETURN QUERY EXECUTE l_sql;

END;
$$ LANGUAGE plpgsql
