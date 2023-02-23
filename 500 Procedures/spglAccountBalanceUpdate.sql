CREATE OR REPLACE PROCEDURE S0000V0000.spglAccountBalanceUpdate(p_RollupAll BOOLEAN DEFAULT FALSE,
                                                                p_sysChangeHistoryID BIGINT DEFAULT 1)
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
This procedure rolls up glentry and glbudget amounts up the glaccount and glcostcentre hierarchy
and updates the AccountBalance table.

Parameters
p_RollupAll - Causes all GLEntries to be re-rolled up.  This should only be necessary if the GLAccount or Cost Centre
structure has been re-organized.  Inserting new accounts or cost centres does not require an all rollup.  If this
parameter is false, only newly approved glentries need to be rolled up.
p_sysChangeHistoryID - defaults to 1.  Mostly this is done for consistency.  Allows us to see all records that
got generated or changed as a result of a change history event.  Change History should never be tracked on
on financial tables like this

20210314    Blair Kjenner   Initial Code

call spglAccountBalanceUpdate(p_rollupall:=true);


*/
DECLARE
    l_Error         RECORD;
    e_State         TEXT;
    e_Msg           TEXT;
    e_Context       TEXT;
    l_fiscalenddate DATE;
    l_return        BOOLEAN;
BEGIN
    SELECT fiscalenddate INTO l_fiscalenddate FROM glsetup;

    IF p_RollupAll = TRUE
    THEN
        UPDATE glentry SET RollupAmount=Amount WHERE COALESCE(RollupAmount, 0) <> Amount;
        UPDATE glbudget SET RollupAmount=Amount WHERE COALESCE(RollupAmount, 0) <> Amount;
    END IF;

    SELECT * INTO l_return FROM fnsysHierarchyUpdate('glaccount', 'referencenumber');

    IF l_return = FALSE
    THEN
        RAISE SQLSTATE '51032';
    END IF;

    SELECT * INTO l_return FROM fnsysHierarchyUpdate('glcostcentre', 'referencenumber');

    IF l_return = FALSE
    THEN
        RAISE SQLSTATE '51033';
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

    INSERT INTO t_glAccountHierarchy (RootID, glAccountIDParent, ID)
    WITH RECURSIVE Paths(RootID,
                         glAccountIDParent,
                         ID)
                       AS (
-- 						   	  The beginning select gets all glaccount records that have no children (i.e. the
-- 					          bottom of the hierarchy).  It also gets the Net Income account because it is
-- 					          the only glaccount that can be posted to that is not at the bottom of the hierarchy.
                              SELECT DISTINCT a.ID RootID,
                                              a.glAccountIDParent,
                                              a.ID ID
                              FROM glAccount a
                              LEFT JOIN glAccount b
                                        ON b.glAccountIDParent = a.id AND b.rowstatus = 'a'
                              WHERE a.rowstatus = 'a'
                                AND (a.glaccounttypeid = 50 --Net Income
                                  OR b.id IS NULL)
                              UNION ALL
-- 							  This loops from the bottom up to get every parent of every record.
                              SELECT p.RootID,
                                     C.glAccountIDParent,
                                     C.ID
                              FROM glAccount AS C
                              JOIN Paths AS p
                                   ON p.glAccountIDParent = C.id
                              WHERE c.rowstatus = 'a')

    SELECT RootID, glAccountIDParent, ID
    FROM Paths;

    DROP TABLE IF EXISTS t_glCostCentreHierarchy;

    CREATE TEMP TABLE t_glCostCentreHierarchy
    (
        RootID               BIGINT,
        glCostCentreIDParent BIGINT,
        ID                   BIGINT
    );

    INSERT INTO t_glCostCentreHierarchy (RootID, glCostCentreIDParent, ID)
    WITH RECURSIVE Paths(RootID,
                         glCostCentreIDParent,
                         ID)
                       AS (
-- 						   	  The beginning select gets all glcostcentre records that have no child (i.e. the
-- 					          bottom of the hierarchy).
                              SELECT a.ID RootID,
                                     a.glCostCentreIDParent,
                                     a.ID ID
                              FROM glCostCentre a
                              LEFT JOIN glCostCentre b
                                        ON b.glCostCentreIDParent = a.id
                                            AND b.rowstatus = 'a'
                              WHERE b.ID IS NULL
                                AND a.rowstatus = 'a'
                              UNION ALL
                              SELECT p.RootID,
                                     C.glCostCentreIDParent,
                                     C.ID
                              FROM glCostCentre AS C
                              JOIN Paths AS p
                                   ON p.glCostCentreIDParent = C.id
                              WHERE c.rowstatus = 'a')
    SELECT RootID,
           glCostCentreIDParent,
           ID
    FROM Paths;

    DROP TABLE IF EXISTS t_Balance;

    CREATE TEMP TABLE t_Balance
    AS
    SELECT glAccountid, glcostcentreid, summarydate, SUM(rollupamount) RollupAmount, summarytype
    FROM (
             SELECT c.id                        glAccountID,
                    d.id                        glCostCentreID,
                    e.bom                       SummaryDate,
                    COALESCE(a.RollupAmount, 0) RollupAmount,
                    'g'                         SummaryType
             FROM glEntry a
             JOIN GLTransaction b
                  ON b.id = a.glTransactionID
             JOIN t_glAccountHierarchy C
                  ON C.RootID = a.glAccountID
             JOIN t_glCostCentreHierarchy D
                  ON D.RootID = a.glCostCentreID
             JOIN LATERAL (SELECT b.transactiondate - DAY(b.transactiondate) + 1 BOM ) e
                  ON TRUE
             UNION ALL
             SELECT c.id                        glAccountID,
                    d.id                        glCostCentreID,
                    f.bom                       SummaryDate,
                    COALESCE(a.RollupAmount, 0) RollupAmount,
                    'r'                         SummaryType
             FROM glEntry a
             JOIN GLTransaction b
                  ON b.id = a.glTransactionID
             JOIN t_glAccountHierarchy C
                  ON C.RootID = a.glAccountID
             JOIN t_glCostCentreHierarchy D
                  ON D.RootID = a.glCostCentreID
             JOIN LATERAL (SELECT COALESCE(a.reportingperioddate, b.transactiondate) ReportingPeriodDate ) e
                  ON TRUE
             JOIN LATERAL (SELECT e.reportingperioddate - DAY(e.reportingperioddate) + 1 BOM ) f
                  ON TRUE
             UNION ALL
             SELECT c.id                        glAccountID,
                    d.id                        glCostCentreID,
                    e.bom                       SummaryDate,
                    COALESCE(a.RollupAmount, 0) RollupAmount,
                    'b'                         SummaryType
             FROM glBudget a
             JOIN t_glAccountHierarchy C
                  ON C.RootID = a.glAccountID
             JOIN t_glCostCentreHierarchy D
                  ON D.RootID = a.glCostCentreID
             JOIN LATERAL (SELECT a.budgetdate - DAY(a.budgetdate) + 1 BOM ) e
                  ON TRUE) a
    GROUP BY a.glaccountid,
             a.glcostcentreid,
             a.summarydate,
             a.SummaryType
    HAVING SUM(COALESCE(a.RollupAmount, 0)) <> 0;

    CREATE INDEX ix_t_Balance
        ON t_Balance (glAccountid, glcostcentreid, summarydate, SummaryType);

    DROP TABLE IF EXISTS t_glAccountBalance;

    CREATE TEMP TABLE t_glAccountBalance
    AS
    SELECT a.glaccountid,
           a.glcostcentreid,
           COALESCE(b.RollupAmount, 0) glaccountamount,
           COALESCE(c.RollupAmount, 0) glbudgetamount,
           COALESCE(d.RollupAmount, 0) glreportingperiodamount,
           a.SummaryDate,
           f.isbalancesheettype
    FROM (
             SELECT DISTINCT glaccountid, glcostcentreid, SummaryDate
             FROM t_Balance) a
    LEFT JOIN t_Balance b
              ON b.glAccountID = a.glAccountID AND b.glCostCentreID = a.glCostCentreID AND
                 b.SummaryDate = a.SummaryDate AND b.SummaryType = 'g'
    LEFT JOIN t_Balance c
              ON c.glAccountID = a.glAccountID AND c.glCostCentreID = a.glCostCentreID AND
                 c.SummaryDate = a.SummaryDate AND c.SummaryType = 'b'
    LEFT JOIN t_Balance d
              ON d.glAccountID = a.glAccountID AND d.glCostCentreID = a.glCostCentreID AND
                 d.SummaryDate = a.SummaryDate AND d.SummaryType = 'r'
    JOIN      glaccount e
              ON e.id = a.glAccountid
    JOIN      glaccounttype f
              ON f.id = e.glaccounttypeid;

    CREATE INDEX ix_t_glAccountBalance
        ON t_glAccountBalance (glAccountid, glcostcentreid, summarydate, isbalancesheettype);

    IF p_RollupAll = TRUE
    THEN
        TRUNCATE TABLE glAccountBalance;
    END IF;

    IF p_RollupAll = TRUE
    THEN
        DROP INDEX IF EXISTS ix_glAccountBalance;
        CREATE INDEX ix_glAccountBalance
            ON glAccountBalance (glAccountid, glcostcentreid, summarydate, isbalancesheettype);
    END IF;

    INSERT INTO glaccountbalance (
        glaccountid,
        glcostcentreid,
        glaccountamount,
        glbudgetamount,
        glreportingperiodamount,
        glaccountamountytd,
        glbudgetamountytd,
        glreportingperiodamountytd,
        SummaryDate,
        isbalancesheettype,
        syschangehistoryid)
    SELECT glaccountid,
           glcostcentreid,
           glaccountamount,
           glbudgetamount,
           glreportingperiodamount,
           0,
           0,
           0,
           SummaryDate,
           isbalancesheettype,
           p_sysChangeHistoryID
    FROM t_glAccountBalance a
    WHERE p_RollupAll = TRUE
       OR NOT EXISTS(SELECT 1
                     FROM glAccountBalance aa
                     WHERE aa.glcostcentreid = a.glcostcentreid
                       AND aa.glaccountid = a.glaccountid
                       AND aa.SummaryDate = a.SummaryDate
        );

    IF p_RollupAll = FALSE
    THEN
        UPDATE glAccountBalance a
        SET glaccountamount         = a.glaccountamount + b.glaccountamount,
            glbudgetamount          = a.glbudgetamount + b.glbudgetamount,
            glreportingperiodamount = a.glreportingperiodamount + b.glreportingperiodamount
        FROM t_glAccountBalance b
        WHERE b.glAccountID = a.glaccountid
          AND b.glCostCentreID = a.glcostcentreid
          AND b.SummaryDate = a.SummaryDate;
    END IF;

    DROP TABLE IF EXISTS t_ytd;
    CREATE TEMP TABLE t_ytd
    AS
    SELECT aa.glaccountid,
           aa.glcostcentreid,
           aa.summarydate,
           COALESCE(cc.glAccountAmountYTD, 0)         glAccountAmountYTD,
           COALESCE(cc.glBudgetAmountYTD, 0)          glBudgetAmountYTD,
           COALESCE(cc.glreportingperiodamountYTD, 0) glreportingperiodamountYTD
    FROM glAccountBalance aa
        -- We only need to update the YTD balances for any glaccount, glcostcentre records
        -- that are in t_glaccountbalance and have a summary date >= to the
        -- earliest summary date in the t_glaccountbalance table.
    JOIN (
             SELECT glaccountid, glcostcentreid, MIN(summarydate) SummaryDate
             FROM t_glAccountBalance
             GROUP BY glaccountid, glcostcentreid) bb
         ON bb.glaccountid = aa.glaccountid
             AND bb.glcostcentreid = aa.glcostcentreid AND aa.summarydate >= bb.summarydate
    JOIN LATERAL (SELECT SUM(COALESCE(aaa.glAccountAmount, 0))         AS glAccountAmountYTD
                       , SUM(COALESCE(aaa.glBudgetAmount, 0))          AS glBudgetAmountYTD
                       , SUM(COALESCE(aaa.glreportingperiodamount, 0)) AS glreportingperiodamountYTD
                  FROM glAccountBalance aaa
                  WHERE aaa.glAccountID = aa.glAccountID
                    AND aaa.glcostcentreid = aa.glcostcentreid
                    AND aaa.SummaryDate <= aa.SummaryDate
                    AND (aaa.isbalancesheettype = TRUE OR
                         aaa.SummaryDate >= fyearstart(aa.SummaryDate, l_fiscalenddate))
             ) cc
         ON TRUE;

    CREATE INDEX ix_t_ytd
        ON t_ytd (glAccountid, glcostcentreid, summarydate);

    UPDATE glAccountBalance a
    SET glAccountAmountYTD= coalesce(b.glAccountAmountYTD,0)
      , glBudgetAmountYTD= coalesce(b.glBudgetAmountYTD,0)
      , glreportingperiodamountYTD= coalesce(b.glreportingperiodamountYTD,0)
    FROM t_ytd b
    WHERE b.glcostcentreid = a.glcostcentreid
      AND b.glaccountid = a.glaccountid
      AND b.SummaryDate = a.summarydate;

    UPDATE glentry SET RollupAmount=NULL WHERE rollupamount IS NOT NULL;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_State = RETURNED_SQLSTATE,
            e_Msg = MESSAGE_TEXT,
            e_Context = PG_EXCEPTION_CONTEXT;
        l_Error := fnsysError(e_State, e_Msg, e_Context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', l_Error.Message;
        ELSE
            IF l_error.IsExceptionRaised = TRUE
            THEN
                RAISE EXCEPTION '%', l_Error.Message;
            ELSE
                RAISE NOTICE '%', l_Error.Message ;
            END IF;
        END IF;
END ;

$$ LANGUAGE plpgsql