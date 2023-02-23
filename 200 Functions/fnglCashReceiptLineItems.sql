DROP FUNCTION IF EXISTS S0000V0000.fnglCashReceiptLineItems CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnglCashReceiptLineItems(p_Mode INT, p_LookupValue INT)
    RETURNS TABLE
            (
                ReconciledAmount    DECIMAL(19, 2),
                Balance             DECIMAL(19, 2),
                TransactionDate     DATE,
                TransactionType     VARCHAR,
                Description         VARCHAR,
                Amount              DECIMAL(19, 2),
                ContactNumber       VARCHAR,
                BillingAccountName  VARCHAR,
                ID                  BIGINT,
                glBillingAccountID  BIGINT,
                GLAccountID         BIGINT,
                ReconciledGLEntryID BIGINT,
                glBatchID           BIGINT,
                OriginalBarcode     BIGINT,
                GLEntryID           BIGINT,
                GLReconciliationID  BIGINT
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
p_Mode:
1 = Populate line items for a cash receipt based on the TransactionID for an existing cash receipt
2 = Populate line items based on a barcode for an existing invoice
3 = Populate line items based on the id of the contact that is paying

20210614 Blair Initial Code

SELECT id gltransactionid FROM gltransaction WHERE gltransactiontypeid = 2 ORDER BY id DESC LIMIT 1;
SELECT id barcodeid, crmcontactid
FROM vwglentrytransaction
WHERE crmcontactid is not null
and lower(transactiontype)='billing'
and reconciliationbalance is null
ORDER BY transactiondate DESC
LIMIT 1;

select * from fnglCashReceiptLineItems(p_mode:=1, p_LookupValue:=513467)
select * from fnglCashReceiptLineItems(p_mode:=2, p_LookupValue:=443916)
select * from fnglCashReceiptLineItems(p_mode:=3, p_LookupValue:=158027)
*/
DECLARE
    l_InvoiceID       INT;
    l_crmContactID    INT;
    l_glTransactionID INT;
    l_Error           RECORD;
    e_State           TEXT;
    e_Msg             TEXT;
    e_Context         TEXT;
BEGIN
    IF p_mode = 1
    THEN
        l_glTransactionID := p_LookupValue;
    ELSEIF p_mode = 2
    THEN
        l_InvoiceID := p_LookupValue;
    ELSEIF p_mode = 3
    THEN
        l_crmContactID := p_LookupValue;
    END IF;

    IF p_mode = 1
        AND NOT EXISTS(SELECT FROM glTransactionSubTypeCashReceipt a WHERE a.ID = l_glTransactionID LIMIT 1)
    THEN
        --Transaction ID % does not exists.
        RAISE SQLSTATE '51038' USING MESSAGE = l_glTransactionID;
    END IF;

    IF p_mode = 2
        AND NOT EXISTS(
                SELECT
                FROM glEntry a
                WHERE a.id = l_InvoiceID
                  AND sysdictionarytableidchargedto = fnsysGlobal('dt-BillingAccount'))
    THEN
        -- Invoice for Barcode % does not exist
        RAISE SQLSTATE '51034' USING MESSAGE = l_InvoiceID;
    END IF;

    DROP TABLE IF EXISTS t_LineItem;
    IF p_Mode = 1
    THEN
        CREATE TEMP TABLE t_LineItem
        AS
        SELECT COALESCE(r.Amount, 0)                               ReconciledAmount,
               COALESCE(pInvG.ReconciliationBalance, pInvG.Amount) Balance,
               pInvT.TransactionDate                               TransactionDate,
               pInvTT.description                                  TransactionType,
               pInvT.Description                                   Description,
               pInvG.Amount                                        Amount,
               pInvC.ContactNumber                                 ContactNumber,
               pInvB.Name                                          BillingAccountName,
               pPTI.ID                                             ID,
               pInvB.id                                            glBillingAccountID,
               pInvG.GLAccountID                                   GLAccountID,
               pInvG.ID                                            ReconciledGLEntryID,
               pInvT.glBatchID                                     glBatchID,
               pInvG.ID                                            OriginalBarcode,
               pInvG.id                                            GLEntryID,
               r.ID                                                GLReconciliationID
        FROM glEntry pPTI
        JOIN glTransactionSubTypeCashReceipt cr
             ON pPTI.glTransactionID = cr.ID
        JOIN glReconciliation r
             ON pPTI.ID = r.glEntryIDTo
        JOIN GLEntry pInvG
             ON r.glEntryIDFrom = pInvG.ID
        JOIN GLBillingAccount pInvB
             ON pInvB.ID = pInvG.Rowidchargedto AND
                pInvG.sysdictionarytableidchargedto = fnsysglobal('dt-billingaccount')
        JOIN crmcontact pInvC
             ON pInvC.ID = pInvB.crmcontactid
        JOIN GLTransaction pInvT
             ON pInvG.GLTransactionId = pInvT.ID
        JOIN GLTransactionType pInvTT
             ON pInvTT.Id = pInvT.gltransactiontypeid
        WHERE pPTI.glentrytypeid = 4 -- payment toward invoice
            AND cr.ID = l_glTransactionID;
    ELSE
        CREATE TEMP TABLE t_LineItem
        AS
        SELECT 0::DECIMAL(19, 2)                                   ReconciledAmount,
               COALESCE(pInvG.ReconciliationBalance, pInvG.Amount) Balance,
               pInvT.TransactionDate                               TransactionDate,
               pInvTT.description                                  TransactionType,
               pInvT.Description                                   Description,
               pInvG.Amount                                        Amount,
               pInvC.ContactNumber                                 ContactNumber,
               pInvB.Name                                          BillingAccountName,
               0::BIGINT                                           ID,
               pInvB.id                                            glBillingAccountID,
               pInvG.GLAccountID                                   GLAccountID,
               pInvG.ID                                            ReconciledGLEntryID,
               pInvT.glBatchID                                     glBatchID,
               pInvG.ID                                            OriginalBarcode,
               pInvG.id                                            GLEntryID,
               CAST(NULL AS BIGINT)                                GLReconciliationID
        FROM glEntry pINVg
        JOIN GLTransaction pInvT
             ON pInvG.GLTransactionId = pInvT.ID
        JOIN GLTransactionType pInvTT
             ON pInvTT.Id = pInvT.gltransactiontypeid
        JOIN GLBillingAccount pInvB
             ON pInvB.ID = pInvG.Rowidchargedto AND
                pInvG.sysdictionarytableidchargedto = fnsysglobal('dt-billingaccount')
        JOIN crmcontact pInvC
             ON pInvC.ID = pInvB.crmcontactid
        WHERE (p_Mode = 2
            AND COALESCE(pInvG.ReconciliationBalance, pInvG.Amount) <> 0
            AND pInvG.ID = l_InvoiceID)
           OR (p_mode = 3
            AND pInvC.ID = l_crmContactID
            AND COALESCE(pInvG.ReconciliationBalance, pInvG.Amount) <> 0);
    END IF;

    RETURN QUERY SELECT *
                 FROM t_LineItem a
                 ORDER BY a.ReconciledAmount DESC, a.glBillingAccountID DESC, a.TransactionDate DESC, a.Amount DESC;

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
$$ LANGUAGE plpgsql
