DROP FUNCTION IF EXISTS S0000V0000.fnGLCostCentreHierarchyBrowse CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnGLCostCentreHierarchyBrowse(p_ReportingDate DATE = NULL,
                                                                p_GLAccountID BIGINT = NULL,
                                                                p_BottomUpLevel INT = NULL,
                                                                p_TopDownLevel INT = NULL,
                                                                p_StartingPosition varchar = NULL,
                                                                p_GLCostCentreID BIGINT = NULL,
                                                                p_ValueType CHAR(1) = 'f',
                                                                p_ComparisonType CHAR(1) = 'p'
)
    RETURNS TABLE
            (
                ID                    BIGINT,
                DESCRIPTION           varchar,
                CURRENTMONTH          DECIMAL(19, 2),
                PREVIOUSMONTH         DECIMAL(19, 2),
                CURRENTYTD            DECIMAL(19, 2),
                PREVIOUSYTD           DECIMAL(19, 2),
                TOPDOWNLEVEL          INTEGER,
                BOTTOMUPLEVEL         INTEGER,
                NORMALIZATIONFACTOR   INTEGER,
                MONTHCHANGEPERCENTAGE NUMERIC,
                YTDCHANGEPERCENTAGE   NUMERIC
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
This function returns the GLCostCentreHierarchy query results based on the parameters passed.

p_ReportingDate      - Query date used for the date of the financial information to be displayed,
p_GLAccountID      - If passed, glAccountBalance information will be selected based on p_GLAccountID.  If null is
                     passed then the GLAccount defaults to Income Statement.
p_BottomUpLevel    - If passed, the query results will be limited to hierarchy results where the glCostCentre bottom up
                     value is <= the p_BottomUp_Level,
p_TopDownLevel     - If passed, the query results will be limited to hierarchy results where the glCostCentre top down
                     value is <= the p_TopDown_Level,
p_StartingPosition - If passed, the query results will traverse down from the first CostCentre where the description
                     is like p_StartingPosition.
p_GLCostCentreID   - If passed, the query results will traverse down from p_GLCostCentreID.  If it is not passed,
                     it will default to the Balance Sheet CostCentre.
p_ValueType        - Allowable values are 'f' fiscal period amount or 'r' Reporting Period Amount
p_ComparisonType   - Allowable values are 'p' Prev Year or 'b' Budget Amount

20210323    Blair Kjenner   Initial Code

select * from fnGLCostCentreHierarchyBrowse (p_ReportingDate:=date '2020-11-30',p_GLAccountID:=null,p_BottomUpLevel:=NULL,p_TopDownLevel:=4,p_StartingPosition:=NULL,p_GLCostCentreID:=NULL,p_ValueType:='F',p_ComparisonType:='b')

*/
DECLARE
    l_prevYear        DATE;
    l_prevMonth       DATE;
    l_FirstLevel      INT;
    l_FirstID         BIGINT;
    l_SQL             varchar;
    l_Variable1       varchar;
    l_Variable2       varchar;
    l_Variable3       varchar;
    l_Variable4       varchar;
    l_glaccounttypeid BIGINT;
    l_toplevelid      BIGINT;
BEGIN
    p_StartingPosition := LOWER(p_StartingPosition);
    p_ValueType := LOWER(p_ValueType);
    p_ComparisonType := LOWER(p_ComparisonType);
    IF p_ReportingDate IS NULL
    THEN
        SELECT a.fiscalenddate INTO p_ReportingDate FROM glsetup a LIMIT 1;
    END IF;

    p_ReportingDate := p_ReportingDate - DAY(p_ReportingDate) + 1; --BOM
    l_prevMonth := DATEADD('M', -1, p_ReportingDate);
    l_prevYear := DATEADD('M', -12, p_ReportingDate);

    SELECT b.id, a.id
    INTO l_glaccounttypeid, p_GLAccountID
    FROM glAccount a
    LEFT JOIN GLAccountType b
              ON b.id = a.glaccounttypeid
    WHERE (p_GLAccountID IS NOT NULL AND a.ID = p_GLAccountID)
       OR (p_GLAccountID IS NULL AND b.id = 50) --Net Income
    LIMIT 1;

    SELECT a.id
    INTO l_toplevelid
    FROM GLCostCentre a
    WHERE a.glCostCentreidparent IS NULL
      AND a.rowstatus = 'a'
    LIMIT 1;

    IF p_GLCostCentreID IS NULL
    THEN
        p_GLCostCentreID := l_toplevelid;
    END IF;

    IF p_StartingPosition IS NOT NULL
    THEN
        p_GLCostCentreID := (
                                SELECT a.ID
                                FROM GLCostCentre a
                                WHERE a.description ILIKE p_StartingPosition + '%'
                                  AND a.rowstatus = 'a'
                                LIMIT 1);
    END IF;

    DROP TABLE IF EXISTS t_GLCostCentre;
    CREATE TEMP TABLE t_GLCostCentre
    (
        id BIGINT
    );

    IF p_GLCostCentreID != l_toplevelid
    THEN
        WITH RECURSIVE Paths(ID)
                           AS (
                                  SELECT ID
                                  FROM GLCostCentre
                                  WHERE id = p_GLCostCentreID
                                  UNION ALL
                                  SELECT c.ID
                                  FROM GLCostCentre AS c
                                  INNER JOIN Paths AS p
                                             ON p.ID = c.GLCostCentreIDParent
                                  LIMIT 1)
        INSERT
        INTO t_GLCostCentre(ID)
        SELECT *
        FROM Paths;
    END IF;

    SELECT a.topdownlevel
    INTO l_FirstLevel
    FROM GLCostCentre a
    WHERE (COALESCE(p_GLCostCentreID, l_toplevelid) = l_toplevelid
        AND a.BottomUpLevel > COALESCE(p_BottomUpLevel, 0)
        AND a.TopDownLevel <= COALESCE(p_TopDownLevel, 10))
       OR (COALESCE(p_GLCostCentreID, l_toplevelid) != l_toplevelid
        AND a.ID IN
            (
                SELECT aa.ID
                FROM t_GLCostCentre aa
            ))
    ORDER BY a.displaysequence
    LIMIT 1;

    IF l_firstlevel IS NULL
    THEN
        l_FirstLevel = 1;
    END IF;
    IF l_firstID IS NULL
    THEN
        l_firstID = 999999;
    END IF;

    l_Variable1 := CASE
                       WHEN p_ValueType = 'f'
                           THEN 'b.glaccountamount'
                       WHEN p_ValueType = 'r'
                           THEN 'b.GLReportingPeriodAmount'
                       END;
    l_Variable2 := CASE
                       WHEN p_ValueType = 'f'
                           THEN 'b.glaccountamountYTD'
                       WHEN p_ValueType = 'r'
                           THEN 'b.GLReportingPeriodAmountYTD'
                       END;
    l_Variable3 := CASE
                       WHEN p_ComparisonType = 'p'
                           AND p_ValueType = 'f'
                           THEN 'c.glaccountamount'
                       WHEN p_ComparisonType = 'p'
                           AND p_ValueType = 'r'
                           THEN 'c.GLReportingPeriodAmount'
                       WHEN p_ComparisonType = 'b'
                           AND p_ValueType = 'f'
                           THEN 'b.GLBudgetAmount'
                       WHEN p_ComparisonType = 'b'
                           AND p_ValueType = 'r'
                           THEN 'b.GLBudgetAmount'
                       END;
    l_Variable4 := CASE
                       WHEN p_ComparisonType = 'p'
                           AND p_ValueType = 'f'
                           THEN 'c.glaccountamountYTD'
                       WHEN p_ComparisonType = 'p'
                           AND p_ValueType = 'r'
                           THEN 'c.GLReportingPeriodAmountYTD'
                       WHEN p_ComparisonType = 'b'
                           AND p_ValueType = 'f'
                           THEN 'b.GLBudgetAmountYTD'
                       WHEN p_ComparisonType = 'b'
                           AND p_ValueType = 'r'
                           THEN 'b.GLBudgetAmountYTD'
                       END;
    l_SQL := '
select
a.id
, a.Description
, COALESCE(e.normalizationfactor*bb.ActualAmount,0) CurrentMonth
, COALESCE(e.normalizationfactor*cc.ActualAmount,0) PreviousMonth
, COALESCE(e.normalizationfactor*bb.CummulativeActual,0) CurrentYTD
, COALESCE(e.normalizationfactor*cc.CummulativeActual,0) PreviousYTD
, a.TopDownLevel-' || COALESCE(l_FirstLevel, 0)::VARCHAR || ' TopDownLevel
, a.BottomUpLevel
, case when e.isBalanceSheetType = False then e.NormalizationFactor * -1 else e.NormalizationFactor end NormalizationFactor
, case when COALESCE(bb.ActualAmount,0)=0 then 100 else ((COALESCE(bb.ActualAmount,0)-COALESCE(cc.ActualAmount,0))/abs(COALESCE(bb.ActualAmount,0)))*100 end MonthChangePercentage
, case when COALESCE(bb.CummulativeActual,0)=0 then 100 else ((COALESCE(bb.CummulativeActual,0)-COALESCE(cc.CummulativeActual,0))/abs(COALESCE(bb.CummulativeActual,0)))*100 end YTDChangePercentage
from GLCostCentre a
LEFT JOIN LATERAL (select * from GLAccountBalance bb where bb.GLCostCentreID=a.ID and bb.GLAccountID=' ||
             CAST(p_GLAccountID AS varchar) ||
             ' and bb.SummaryDate <= date ''' || TO_CHAR(p_ReportingDate, 'yyyy-mm-dd') ||
             ''' and year(bb.SummaryDate)=' || year(p_ReportingDate)::VARCHAR ||
             ' order by bb.SummaryDate desc LIMIT 1) b ON TRUE';
    IF p_ComparisonType = 'p'
    THEN
        l_SQL := l_SQL || '
LEFT JOIN LATERAL (select * from GLAccountBalance cc where cc.GLCostCentreID=a.ID and cc.GLAccountID=' ||
                 CAST(p_GLAccountID AS varchar) ||
                 ' and cc.SummaryDate <= date ''' || TO_CHAR(l_PrevYear, 'yyyy-mm-dd') ||
                 ''' and year(cc.SummaryDate)=' ||
                 year(l_prevyear) ||
                 ' order by cc.SummaryDate desc LIMIT 1) c  ON TRUE';
    END IF;
    l_SQL := l_SQL || '
JOIN LATERAL (select case when b.SummaryDate <> date ''' || TO_CHAR(p_ReportingDate, 'yyyy-mm-dd') || ''' then 0 else ' ||
             l_Variable1 ||
             ' end ActualAmount,'
                 || l_Variable2 || ' CummulativeActual) bb ON TRUE
JOIN LATERAL (SELECT CASE WHEN ' || CASE
                                                                           WHEN p_ComparisonType = 'b'
                                                                               THEN 'b.'
                                                                               ELSE 'C.'
                                                                           END
                 || 'SummaryDate <> date ''' || TO_CHAR(l_PrevYear, 'yyyy-mm-dd') ||
             ''' THEN 0 ELSE ' || l_Variable3 || ' END ActualAmount,'
                 || l_Variable4 || ' CummulativeActual) cc ON TRUE
left Join GLAccountType e on e.id=' || CAST(l_glaccounttypeid AS varchar) || '
where bb.CummulativeActual <> 0';
    IF p_StartingPosition IS NOT NULL
    THEN
        l_SQL := l_SQL || '
and a.ID in (select aa.ID from t_GLCostCentre aa)';
    ELSE
        l_SQL := l_SQL || '
and coalesce(a.BottomUpLevel,1) > ' || CAST(COALESCE(p_BottomUpLevel, 0) AS varchar) || '
and coalesce(a.TopDownLevel,1) - ' || CAST(COALESCE(l_FirstLevel, 0) - 1 AS varchar) || ' <= ' ||
                 CAST(COALESCE(p_TopDownLevel, 10) AS varchar);
        IF p_GLCostCentreID <> l_toplevelid
        THEN
            l_SQL := l_SQL || '
and a.ID in (select aa.ID from t_GLCostCentre aa)';
        END IF;
        l_SQL := l_SQL || '
order by a.displaysequence';
--                 RAISE NOTICE '''l_prevYear'' % ''l_prevMonth'' % ''l_FirstLevel'' % ''l_FirstID'' % ''l_SQL'' % ''l_Variable1'' % ''l_Variable2'' % ''l_Variable3'' % ''l_Variable4'' % ''l_toplevelid '' % '
--             , l_prevYear, l_prevMonth, l_FirstLevel, l_FirstID, l_SQL, l_Variable1, l_Variable2, l_Variable3, l_Variable4, l_toplevelid;
        RETURN QUERY EXECUTE l_sql;
    END IF;
END ;

$$ LANGUAGE PLPGSQL

