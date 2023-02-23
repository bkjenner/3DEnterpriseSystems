CREATE OR REPLACE PROCEDURE S0000V0000.spglReconcileRemainingItems(p_sysChangeHistoryID BIGINT DEFAULT 1,
                                                                   p_BillingAccountID BIGINT DEFAULT NULL)
    LANGUAGE plpgsql
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
This procedure will attempt to reconcile any posted GL entries that are not already fully reconciled.

20210306    Blair Kjenner   Initial Code

call spglReconcileRemainingItems();

*/
DECLARE
    e_state   TEXT;
    e_msg     TEXT;
    e_context TEXT;
    l_error   RECORD;

BEGIN

    DROP TABLE IF EXISTS t_GLEntryToBeReconciledStage1;
    DROP TABLE IF EXISTS t_GLEntryToBeReconciledStage2;
    DROP TABLE IF EXISTS t_ReconciliationStage1;
    DROP TABLE IF EXISTS t_ReconciliationStage2;

    CREATE TEMP TABLE t_GLEntryToBeReconciledStage1
    (
        ID                    BIGINT,
        GLAccountID           BIGINT,
        glBillingAccountID    BIGINT,
        Amount                DECIMAL(19, 2),
        ReconciliationBalance DECIMAL(19, 2),
        TransactionDate       TIMESTAMP,
        Balance               DECIMAL(19, 2),
        NextID                BIGINT,
        IsFirstRecord         BOOLEAN,
        PRIMARY KEY (ID)
    );

    CREATE TEMP TABLE t_GLEntryToBeReconciledStage2
    (
        ID                    BIGINT,
        GLAccountID           BIGINT,
        glBillingAccountID    BIGINT,
        Amount                DECIMAL(19, 2),
        ReconciliationBalance DECIMAL(19, 2),
        TransactionDate       TIMESTAMP,
        Balance               DECIMAL(19, 2),
        NextID                BIGINT,
        IsFirstRecord         BOOLEAN,
        PRIMARY KEY (ID)
    );

    CREATE TEMP TABLE t_ReconciliationStage1
    (
        ID                    BIGINT,
        NextID                BIGINT,
        ReconciliationBalance DECIMAL(19, 2),
        RunningBalance        DECIMAL(19, 2),
        LoopCount             BIGINT
    );

    CREATE TEMP TABLE t_ReconciliationStage2
    (
        ID                    BIGINT,
        NextID                BIGINT,
        ReconciliationBalance DECIMAL(19, 2),
        RunningBalance        DECIMAL(19, 2),
        LoopCount             BIGINT,
        glEntryIDFrom         BIGINT,
        glEntryIDTo           BIGINT,
        sysChangeHistoryID    BIGINT
    );

    INSERT INTO t_GLEntryToBeReconciledStage1
    SELECT gl.ID,
           gl.GLAccountID,
           gl.glBillingAccountID,
           gl.Amount,
           COALESCE(gl.ReconciliationBalance, gl.Amount) ReconciliationBalance,
           gl.TransactionDate,
           0                                             Balance,
           NULL                                          NextID,
           FALSE                                         IsFirstRecord
    FROM vwGLEntryTransaction GL
    WHERE GL.glbillingaccountid IS NOT NULL-- gl.GLAccountid = 9
--and gl.glGLEntryStatusID = 482 -- Posted
      AND gl.Amount <> 0
      AND COALESCE(gl.ReconciliationBalance, gl.Amount) != 0
      AND (p_BillingAccountID IS NULL OR gl.glBillingAccountID = p_BillingAccountID);

    -- We only add records INTO stage2 where there IS more than one record for the account.
-- if there IS only one record there IS nothing to reconcile.
-- it IS faster to Create a new subset table than delete records from the old one.

    INSERT INTO t_GLEntryToBeReconciledStage2 (
        ID,
        GLAccountID,
        glBillingAccountID,
        Amount,
        ReconciliationBalance,
        TransactionDate,
        Balance,
        NextID,
        IsFirstRecord)
    SELECT a.ID,
           a.GLAccountID,
           a.glBillingAccountID,
           a.Amount,
           a.ReconciliationBalance,
           a.TransactionDate,
           a.Balance,
           a.NextID,
           a.IsFirstRecord
    FROM t_GLEntryToBeReconciledStage1 a
    LEFT JOIN(
                 SELECT glBillingAccountID, GLAccountID
                 FROM t_GLEntryToBeReconciledStage1
                 GROUP BY glBillingAccountID, GLAccountID
                 HAVING COUNT(0) = 1) b
             ON b.glBillingAccountID = a.glBillingAccountID AND b.GLAccountID = a.GLAccountID
    ORDER BY GLAccountID, TransactionDate, ID;

    CREATE INDEX ix_GLEntryToBeReconciledStage2 ON t_GLEntryToBeReconciledStage2 (glBillingAccountID, GLAccountID, TransactionDate, ID);

    -- Identify the first and next record IDs.
    UPDATE t_GLEntryToBeReconciledStage2 a
    SET IsFirstRecord=b.isfirstrecord, NextID=b.nextid
    FROM (
             SELECT aa.ID, cc.id IS NOT NULL IsFirstRecord, bb.id NextID
             FROM t_GLEntryToBeReconciledStage2 aa
             LEFT JOIN LATERAL (
                           SELECT aaa.ID
                           FROM t_GLEntryToBeReconciledStage2 aaa
                           WHERE aaa.glBillingAccountID = aa.glBillingAccountID
                             AND aaa.GLAccountID = aa.GLAccountID
                             AND (aaa.TransactionDate
                                      > aa.TransactionDate
                               OR (aaa.TransactionDate = aa.TransactionDate
                                   AND aaa.ID
                                       > aa.ID))
                           ORDER BY aaa.TransactionDate, aaa.ID
                           LIMIT 1) bb
                       ON TRUE
             LEFT JOIN (
                           SELECT MIN(aa.ID) ID
                           FROM t_GLEntryToBeReconciledStage2 aa
                           GROUP BY aa.glBillingAccountID, aa.glAccountid) cc
                       ON cc.id = aa.id) b
    WHERE b.id = a.id;

-- loop forward through the records and Set a running balance.
    WITH RECURSIVE GLEntryRunningBalance
                       AS (
-- the first select gets all the starting records that We will be starting our loops with.
-- I put a loop count in to show how you could Do something like that but it IS not required.
                              SELECT p.ID,
                                     p.NextID,
                                     p.ReconciliationBalance,
                                     CAST(p.ReconciliationBalance AS DECIMAL(19, 2)) RunningBalance,
                                     CAST(1 AS BIGINT)                               LoopCount
                              FROM t_GLEntryToBeReconciledStage2 p
-- We are only interested in getting the first record at this point. if the NextID IS NULL Then that means there IS only one record and We are not interested in it.
                              WHERE p.IsFirstRecord = TRUE
                              UNION ALL
-- this select will loop until there are no more records to sysProcess
                              SELECT CurrentRec.ID,
                                     CurrentRec.NextID,
                                     CurrentRec.ReconciliationBalance,
                                     CAST(
                                             PriorRec.RunningBalance + CurrentRec.ReconciliationBalance AS DECIMAL(19, 2)) RunningBalance,
                                     CAST(PriorRec.LoopCount + 1 AS BIGINT)                                                LoopCount
                              FROM t_GLEntryToBeReconciledStage2 CurrentRec
-- "Prior" is relative to the records in this loop.
                              JOIN GLEntryRunningBalance PriorRec
                                   ON CurrentRec.ID = PriorRec.NextID)

-- Once all the work IS done We can select from our record set. We could Do updates OR deletes based on the record set.
    INSERT
    INTO t_ReconciliationStage1
    SELECT DISTINCT *
    FROM GLEntryRunningBalance;

-- for every record where the running balance comes to zero, loop backward until you hit the last record OR another zero balance
    WITH RECURSIVE GLEntryRunningBalance
                       AS (
-- the first select gets all the starting records that We will be starting our loops with.
-- I put a loop count in to show how you could Do something like that but it IS not required.
                              SELECT p.ID,
                                     p.NextID,
                                     p.ReconciliationBalance,
                                     p.RunningBalance,
                                     CAST(1 AS BIGINT) LoopCount,
                                     p.ID              glEntryIDFrom,
                                     p.ID              glEntryIDTo
                              FROM t_ReconciliationStage1 p
                              WHERE p.RunningBalance = 0
                              UNION ALL
-- this select will loop until there are no more records to Process
                              SELECT CurrentRec.ID,
                                     CurrentRec.NextID,
                                     CurrentRec.ReconciliationBalance,
                                     CurrentRec.RunningBalance,
                                     CAST(PriorRec.LoopCount + 1 AS BIGINT) LoopCount,
                                     PriorRec.glEntryIDFrom,
                                     CurrentRec.ID                          glEntryIDTo
                              FROM t_ReconciliationStage1 CurrentRec
-- "Prior" is relative to the records in this loop.
                              JOIN GLEntryRunningBalance PriorRec
                                   ON CurrentRec.NextID = PriorRec.ID
                              WHERE COALESCE(CurrentRec.RunningBalance, 0) != 0)

-- Once all the work IS done We can select from our record set. We can Do updates OR deletes based on the record set.
    INSERT
    INTO t_ReconciliationStage2
    SELECT *, 10
    FROM GLEntryRunningBalance;

-- INSERT the records INTO the reconciliation table.
    INSERT INTO glReconciliation(
        glEntryIDFrom, glEntryIDTo, Amount, sysChangeHistoryID)
    SELECT glEntryIDFrom,
           glEntryIDTo,
           ReconciliationBalance * (-1)
               AS Amount,
           p_sysChangeHistoryID
    FROM t_ReconciliationStage2
    WHERE glEntryIDFrom != glEntryIDTo
    UNION ALL
    SELECT glEntryIDTo,
           glEntryIDFrom,
           ReconciliationBalance
               AS Amount,
           p_sysChangeHistoryID
    FROM t_ReconciliationStage2
    WHERE glEntryIDFrom != glEntryIDTo;

    TRUNCATE TABLE t_GLEntryToBeReconciledStage1;
    INSERT INTO t_GLEntryToBeReconciledStage1
    SELECT gl.ID,
           gl.GLAccountID,
           gl.glBillingAccountID,
           gl.Amount,
           COALESCE(gl.ReconciliationBalance, gl.Amount) ReconciliationBalance,
           gl.TransactionDate,
           0                                             Balance,
           NULL                                          NextID,
           FALSE                                         IsFirstRecord
    FROM vwGLEntryTransaction GL
    WHERE gl.glBillingAccountID IS NOT NULL
      AND gl.glPostingStatusID = 551076 -- Posted
      AND COALESCE(gl.ReconciliationBalance, gl.Amount) != 0
      AND (p_BillingAccountID IS NULL OR gl.glBillingAccountID = p_BillingAccountID);

-- Identify matching amounts by account that can be reconciled.
    DROP TABLE IF EXISTS t_ReconciliationsStage3;
    CREATE TEMP TABLE t_ReconciliationsStage3
    (
        glBillingAccountID BIGINT,
        Amount             DECIMAL(19, 2),
        DebitGLEntryID     BIGINT,
        CreditGLEntryID    BIGINT
    );

    LOOP

        INSERT INTO t_ReconciliationsStage3
        SELECT glBillingAccountID, amount, MAX(glEntryID) DebitGLEntryID, MIN(glEntryID) CreditGLEntryID
        FROM (
                 SELECT a.glBillingAccountID, a.amount, MIN(a.ID) glEntryID
                 FROM t_GLEntryToBeReconciledStage1 a
                 JOIN glEntry b
                      ON b.id = a.id AND COALESCE(b.ReconciliationBalance, 99) <> 0
                 WHERE a.amount > 0
                 GROUP BY a.glBillingAccountID, a.amount
                 UNION ALL
                 SELECT a.glBillingAccountID, a.amount * -1, MIN(a.ID * -1) glEntryID
                 FROM t_GLEntryToBeReconciledStage1 a
                 JOIN glEntry b
                      ON b.id = a.id AND COALESCE(b.ReconciliationBalance, 99) <> 0
                 WHERE a.amount < 0
                 GROUP BY a.glBillingAccountID, a.amount) a
        GROUP BY glBillingAccountID, amount
        HAVING COUNT(*) > 1;

        EXIT WHEN NOT FOUND;

        INSERT INTO glReconciliation(
            glEntryIDFrom, glEntryIDTo, Amount, sysChangeHistoryID)
        SELECT DebitGLEntryID,
               CreditGLEntryID * -1,
               Amount AS Amount,
               p_sysChangeHistoryID
        FROM t_ReconciliationsStage3
        UNION ALL
        SELECT CreditGLEntryID * -1,
               DebitGLEntryID,
               Amount * -1 AS Amount,
               p_sysChangeHistoryID
        FROM t_ReconciliationsStage3;

        TRUNCATE TABLE t_ReconciliationsStage3;

    END LOOP;

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
END;
$$ -- spReconcileRemainingItems



