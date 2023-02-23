CREATE OR REPLACE PROCEDURE S0000V0000.spglBatchApprove(p_BatchID BIGINT DEFAULT NULL, p_UserContactID BIGINT DEFAULT 1,
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
This procedure approves a batch based on the batchid passed

20210302    Blair Kjenner   Initial Code

Approves the last pending batch that exists
call spglBatchApprove()
*/

DECLARE
    l_FiscalStartDate TIMESTAMP;
    l_FiscalEndDate   TIMESTAMP;
    l_BatchTypeID     BIGINT;
    l_TransactionID   BIGINT;
    l_ConstGST        FLOAT;
    l_Error           RECORD;
    e_State           TEXT;
    e_Msg             TEXT;
    e_Context         TEXT;
BEGIN
    IF p_BatchID IS NULL
    THEN
        SELECT MAX(ID) INTO p_BatchID FROM glbatch WHERE glbatchstatusid = 1; --Pending
    END IF;

    SELECT Rate
    INTO l_ConstGST
    FROM glrate
    WHERE TemporalStartDate <= NOW()
      AND NOW() < TemporalEndDate
      AND RowStatus = 'A';

    ---------------
    -- VALIDATION
    --
---------------

    IF NOT EXISTS(SELECT 0 FROM glBatch WHERE ID = p_BatchID LIMIT 1)
    THEN
        RAISE SQLSTATE '51008' USING MESSAGE = fnsysIDView(p_BatchID);
    END IF;

-- Check if the glBatch IS already Approved
    IF EXISTS(SELECT 0 FROM glBatch WHERE ID = p_BatchID AND ApprovalDate IS NOT NULL LIMIT 1)
    THEN
        RAISE SQLSTATE '51009' USING MESSAGE = fnsysIDView(p_BatchID);
    END IF;

    SELECT glBatchTypeID
    INTO l_BatchTypeID
    FROM glBatch
    WHERE ID = p_BatchID;

    IF NOT EXISTS(SELECT 0 FROM glTransaction WHERE glBatchID = p_BatchID LIMIT 1)
    THEN
        RAISE SQLSTATE '51010' USING MESSAGE = fnsysIDView(p_BatchID);
    END IF;

--Set the Fiscal Start and End Year
    SELECT FiscalStartDate, FiscalEndDate INTO l_FiscalStartDate, l_FiscalEndDate FROM glSetUp;

    IF l_TransactionID IS NOT NULL
    THEN
        RAISE SQLSTATE '51012' USING MESSAGE = fnsysIDView(l_TransactionID);
    END IF;

    SELECT glTransactionID
    INTO l_TransactionID
    FROM (
             SELECT glTransactionID
             FROM vwGLEntryTransaction
             WHERE glBatchID = p_BatchID
             GROUP BY glTransactionID
             HAVING SUM(amount) != 0) a;

    IF l_TransactionID IS NOT NULL
    THEN
        RAISE SQLSTATE '51013' USING MESSAGE = fnsysIDView(l_TransactionID);
    END IF;

    l_TransactionID := NULL;
    SELECT a.glTransactionID
    INTO l_TransactionID
    FROM vwGLEntryTransaction a
    JOIN glReconciliation b
         ON b.GLEntryIDFrom = a.ID
    WHERE glBatchID = p_BatchID
      AND a.Amount != 0
      AND SIGN(a.Amount) != SIGN(b.Amount)
    LIMIT 1;

    IF l_TransactionID IS NOT NULL
    THEN
        RAISE SQLSTATE '51014' USING MESSAGE = fnsysIDView(l_TransactionID);
    END IF;

    SELECT glTransactionID
    INTO l_TransactionID
    FROM vwGLEntryTransaction a
    JOIN LATERAL (SELECT SUM(b.Amount) Amount
                  FROM glReconciliation b
                  WHERE a.ID = glentryidfrom) b
         ON TRUE
    WHERE b.Amount > a.Amount
      AND a.glBatchID = p_BatchID
    LIMIT 1;

    IF l_TransactionID IS NOT NULL
    THEN
        RAISE SQLSTATE '51015' USING MESSAGE = fnsysIDView(l_TransactionID);
    END IF;

    l_TransactionID := NULL;
    SELECT glTransactionID
    INTO l_TransactionID
    FROM vwGLEntryTransaction a
    WHERE a.glBatchID = p_BatchID
      AND a.GLAccountID IS NULL
    LIMIT 1;

    IF l_TransactionID IS NOT NULL
    THEN
        RAISE SQLSTATE '51016' USING MESSAGE = fnsysIDView(l_TransactionID);
    END IF;

    SELECT glTransactionID
    INTO l_TransactionID
    FROM vwGLEntryTransaction a
    LEFT JOIN glAccount b
              ON b.ID = a.GLAccountID
    WHERE a.glBatchID = p_BatchID
      AND b.sysmultilinktableruleid = 1030 -- BillingAccounts
      AND a.billingaccountname IS NULL
    LIMIT 1;

    IF l_TransactionID IS NOT NULL
    THEN
        RAISE SQLSTATE '51017' USING MESSAGE = fnsysIDView(l_TransactionID);
    END IF;

    UPDATE glTransaction
    SET glPostingStatusID=fnsysGlobal('ps-posted'), sysChangeHistoryID=p_sysChangeHistoryID
    WHERE glBatchID = p_BatchID;

-- if all glTransaction have a balance of zero, mark the glBatch as being approved.
    UPDATE glBatch
    SET ApprovalDate=NOW(), glbatchstatusid=fnsysGlobal('bs-approved'), sysChangeHistoryID=p_sysChangeHistoryID
    WHERE ID = p_BatchID;

    CALL spglReconcileRemainingItems(p_sysChangeHistoryID);

    CALL spglExport(p_BatchID::VARCHAR, p_sysChangeHistoryID);

EXCEPTION
    WHEN OTHERS THEN --587-594-7372
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
$$ LANGUAGE plpgsql
