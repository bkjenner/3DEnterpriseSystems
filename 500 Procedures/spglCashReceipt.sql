CREATE OR REPLACE PROCEDURE S0000V0000.spglCashReceipt(
    p_Barcode BIGINT DEFAULT NULL -- Barcode ID used to lookup the associated glentry when a barcode is specified on the main page of the cash receipt
, p_crmContactIDPaidFor BIGINT DEFAULT NULL -- Id of crmContact that the Cash Receipt is pay for. If not specified it will default to the crmContact id associated with the barcode (GLEntryID)
, p_crmContactIDUser BIGINT DEFAULT 1 -- Id of user that created the cash receipt. Default to System Administrator
, INOUT p_glBatchID BIGINT DEFAULT NULL -- If null will attach new cash receipt record to an open cash receipt glBatch. If one doesnt exist for the current day, it will add it.
, p_glPaymentMethodid INT DEFAULT 2 -- Payment Method of the cash receipt. If it is null, it will default to Cheque
--, p_SubmitToGateway BOOLEAN -- Controls whether the cash receipt will get submitted to the payment gateway
, INOUT p_glTransactionID INT DEFAULT NULL -- If null will set it to the the cash receipt id if it exists
, p_Name VARCHAR DEFAULT NULL -- Name of entity that paid the cash receipt. If null, will default to name of entity associated with crmContact id
--, p_OrderID varchar DEFAULT NULL
, p_ReceiptAmount DECIMAL(19, 2) DEFAULT NULL -- Amount of the cash receipt. If the Amount is null then use the amount from the glEntry
, p_ReferenceNumber VARCHAR DEFAULT NULL -- Either the cheque number if it is paid by cheque or the "transaction tag" if the user supplied an existing one that existed from some other call to the payment gateway.
, p_TransactionDate DATE DEFAULT NULL -- Date of the cash receipt. If null, use the current date
, p_TransactionState VARCHAR DEFAULT 'add' -- One of {Add, Edit}
, p_WriteOffAmount DECIMAL(19, 2) DEFAULT NULL -- Amount that should be written off in order to balance
, p_CashReceiptItems JSONB DEFAULT NULL -- Identifies the glEntry ID of the item being paid, Amount paid. If it is null, it will be filled in based on the barcode if it is supplied
-- glEntryID IS the glEntry ID of the item that was paid and/OR cleared as a result of being selected. this value IS mandatory
-- amount IS the amount that was paid toward the glEntry. this value IS mandatory
-- Example [{"glentryid" : 457729, "amount" : 5000.00}]
, p_ManualItems JSONB DEFAULT NULL -- Identifies the glEntry ID of a manual item, Amount, Description, GLAccountID, glBillingAccountID. p_manualItems is an optional parameter.
-- if the amount passed equal zero, no record will be inserted
-- if the amount passed IS zero and a glEntryID IS passed, the record will be deleted.
-- Example
-- [{"amount" : 100,"description" : "Test Manual Item","glaccountid" : 3120480,"glcostcentreid" : 0,"billingaccountid" : null}]
, p_sysChangeHistoryID BIGINT DEFAULT 1
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
20210606 Blair - Created.

--Generates call based on all outstanding invoices.
WITH a AS (
select *
from vwglentrytransaction a
where a.glaccountid=1400
  and a.reconciliationbalance is NULL
    and lower(a.transactiontype)='billing'
)
select 'call spglCashReceipt (p_barcode :=null,p_crmcontactidpaidfor:='||min(crmcontactid)::varchar||
',p_crmcontactiduser:=149675,p_glbatchid:=null,p_glpaymentmethodid:=2,p_gltransactionid:=null,p_name:=''test name'',p_receiptamount:='
||(sum(amount)+100-.13)::varchar||',p_referencenumber:=''ref 11'',p_transactiondate:=null,p_transactionstate:=''add'',p_writeoffamount:=.13,p_cashreceiptitems:='''
||json_agg(json_build_object('glentryid', a.id, 'amount', amount, 'glreconciliationid', null))
||''',p_manualitems:=''[{"amount" : 100,"description" : "Test Manual Item","glaccountid" : 3120480,"glcostcentreid" : 0,"billingaccountid" : null}]'',p_syschangehistoryid:=1);'
from a;

-- Sample call using all defaults
call spglCashReceipt (p_barcode :=443916, p_transactionstate := 'add');

-- For additional test scripts see spglCashReceiptGenerateEditScript in Test Scripts
*/
DECLARE
    l_Address            VARCHAR;
    l_crRec              RECORD;
    l_Description        VARCHAR;
    l_Error              RECORD;
    l_GLAccountIDCash    BIGINT;
    l_glCostCentreID     BIGINT;
    l_glEntryIDMain      BIGINT;
    l_iscostcentreused   BOOLEAN;
    l_iscreditcard       BOOLEAN;
    l_miRec              RECORD;
    l_sysChangeHistoryID INT := 0;
    l_Today              DATE;
    l_TransactionBalance DECIMAL(19, 2);
    l_transactionTag     VARCHAR;
    l_WriteOffAccount    BIGINT;
    l_WriteOffCostCentre BIGINT;
    l_WriteOffTolerance  DECIMAL(19, 2);
    e_State              TEXT;
    e_Msg                TEXT;
    e_Context            TEXT;
BEGIN

    DROP TABLE IF EXISTS t_CashReceiptItems;
    CREATE TEMP TABLE t_CashReceiptItems
    (
        ReconciledGlEntryID BIGINT,
        ReconciledAmount    DECIMAL(19, 2),
        GLReconciliationID  BIGINT
    );

    DROP TABLE IF EXISTS t_ManualItems;
    CREATE TEMP TABLE t_ManualItems
    (
        GlEntryID          BIGINT,
        Amount             DECIMAL(19, 2),
        Description        VARCHAR,
        GLAccountID        BIGINT,
        GLCostCentreID     BIGINT,
        GLBillingAccountID BIGINT
    );
    DROP TABLE IF EXISTS t_CashReceiptItemDetail;
    CREATE TEMP TABLE t_CashReceiptItemDetail
    (
        id                                SERIAL         NOT NULL,
        pReconciledGLEntryID              BIGINT         NULL,
        pReconciledAmount                 DECIMAL(19, 2) NOT NULL,
        pGLReconciliationID               BIGINT         NULL,
        ReconciledGLEntryID               BIGINT         NULL,
        ReconciledGLEntryInvoiceID        BIGINT         NULL,
        ReconciledGLEntryType             VARCHAR        NULL,
        ReconciledGLEntryDescription      VARCHAR        NULL,
        ReconciledGLEntryBillingAccountID BIGINT         NULL,
        ReconciledGLEntryGLAccountID      BIGINT         NULL,
        GLReconciliationID1               BIGINT,
        GLReconciliationID2               BIGINT,
        ReconciliationAmount              DECIMAL(19, 2),
        POAGLEntryID                      BIGINT
    );

    DROP TABLE IF EXISTS t_ManualItemDetail;

    CREATE TEMP TABLE t_ManualItemDetail
    (
        id                      SERIAL NOT NULL,
        pManualGLEntryID        BIGINT,
        pManualAmount           DECIMAL(19, 2),
        pManualDescription      VARCHAR,
        pManualGLAccountID      BIGINT,
        pManualGLCostCentreID   BIGINT,
        pManualBillingAccountID BIGINT,
        ManualGLEntryID         BIGINT,
        ManualAmount            DECIMAL(19, 2),
        ManualDescription       VARCHAR,
        ManualGLAccountID       BIGINT,
        ManualGLCostCentreID    BIGINT,
        ManualBillingAccountID  BIGINT
    );

    DROP TABLE IF EXISTS t_ExistingPostingsNotReferenced;

    CREATE TEMP TABLE t_ExistingPostingsNotReferenced
    (
        ID BIGINT
    );

    l_Today := NOW();

    l_isCreditCard := (
                          SELECT LOWER(LEFT(shortcode, 2)) FROM glPaymentMethod WHERE id = p_glPaymentMethodid) = 'cr';
/*
    -- Payment Gateway code
    IF l_isCreditCard = TRUE
    THEN
        IF p_SubmitToGateway = FALSE
        THEN
            l_transactionTag := p_referenceNumber;
        END IF;

        p_referenceNumber := NULL;
    END IF;

*/
    IF p_barcode IS NOT NULL
    THEN
        IF NOT EXISTS(
                SELECT 1
                FROM glEntry
                WHERE id = p_barcode
                  AND sysdictionarytableidchargedto = fnsysGlobal('dt-BillingAccount'))
        THEN
            -- Invoice for Barcode % does not exist
            RAISE SQLSTATE '51034' USING MESSAGE = p_barcode;
        END IF;
    END IF;

    IF (p_transactionState = 'edit') AND (p_glTransactionID IS NULL)
    THEN
        --When p_TransactionState = ''edit'', the p_glTransactionID is required.
        RAISE SQLSTATE '51035';
    END IF;

    IF (p_glTransactionID IS NOT NULL)
    THEN
        IF NOT EXISTS(SELECT 0 FROM glTransactionSubTypeCashReceipt WHERE ID = p_glTransactionID)
        THEN
            --Cash Receipt p_glTransactionID does not exist
            RAISE SQLSTATE '51036' USING MESSAGE = p_glTransactionID;
        END IF;

        IF (p_transactionState = 'edit')
        THEN
            SELECT glEntryIDMain INTO l_glEntryIDMain FROM glTransaction WHERE ID = p_glTransactionID;

            IF NOT EXISTS(SELECT 0 FROM glEntry WHERE ID = COALESCE(l_glEntryIDMain, 0) LIMIT 1)
            THEN
                --Cash Receipt p_glTransactionID does not have an associated CR glEntry that exists
                RAISE SQLSTATE '51036' USING MESSAGE = p_glTransactionID;
            END IF;
        END IF;
    END IF;

    IF (p_glTransactionID IS NOT NULL)
    THEN
        IF NOT EXISTS(SELECT 0 FROM glTransaction WHERE ID = p_glTransactionID LIMIT 1)
        THEN
            --Cash Receipt p_glTransactionID does not have an associated transaction that exists
            RAISE SQLSTATE '51038' USING MESSAGE = p_glTransactionID;
        END IF;

        IF (
               SELECT COALESCE(glPostingStatusID, 0) FROM glTransaction WHERE ID = p_glTransactionID) != 1 -- Pending
        THEN
            --Transaction p_glTransactionID is not Pending
            RAISE SQLSTATE '51039' USING MESSAGE = p_glTransactionID;
        END IF;
        IF NOT EXISTS(SELECT 0 FROM gltransactionsubtypecashreceipt WHERE p_glTransactionID = p_glTransactionID)
        THEN
            --Cash Receipt p_glTransactionID does not have an associated transaction that exists
            RAISE SQLSTATE '51036' USING MESSAGE = p_glTransactionID;
        END IF;

    END IF;

    IF (p_crmContactIDPaidFor IS NULL)
    THEN
        SELECT crmContactID
        INTO p_crmContactIDPaidFor
        FROM vwGLEntryTransaction
        WHERE ID = p_barcode
           OR (id = p_barcode AND glBillingAccountID IS NOT NULL)
        LIMIT 1;
    END IF;

    IF l_Address IS NULL
    THEN
        SELECT address1 || COALESCE(', ' || city, '')
        INTO l_Address
        FROM vwcrmcontactaddress
        WHERE crmContactID = p_crmContactIDPaidFor
        ORDER BY isprimaryaddress DESC
        LIMIT 1;
    END IF;

    IF p_name IS NULL
    THEN
        SELECT name INTO p_name FROM crmContact WHERE id = p_crmContactIDPaidFor;
    END IF;

    IF p_TransactionDate IS NULL
    THEN
        p_TransactionDate := l_Today;
    END IF;

    IF p_CashReceiptItems IS NOT NULL
    THEN
        INSERT INTO t_CashReceiptItems(ReconciledGlEntryID, ReconciledAmount, GLReconciliationID)
        SELECT (z.spec ->> 'glentryid')::BIGINT          ReconciledGLEntryID,
               (z.spec ->> 'amount')::DECIMAL(19, 2)     ReconciledAmount,
               (z.spec ->> 'glreconciliationid')::BIGINT GLReconciliationID
        FROM JSONB_ARRAY_ELEMENTS(p_CashReceiptItems) AS z(spec);
        -- The following code does not require explicit typing.  It just requires that the
        -- column names in p_CashReceiptItems match the names in t_CashReceiptItems
--         SELECT (JSONB_POPULATE_RECORD(NULL::t_CashReceiptItems, j.spec)).*
--         FROM JSONB_ARRAY_ELEMENTS(p_CashReceiptItems) AS j(spec);
    END IF;

    IF p_cashReceiptItems IS NULL AND p_transactionState = 'add'
    THEN
        INSERT INTO t_CashReceiptItems (ReconciledGlEntryID, ReconciledAmount)
        SELECT a.ID, Amount
        FROM vwGLEntryTransaction a
        WHERE (a.id = p_barcode AND a.glBillingAccountID IS NOT NULL)
          AND ReconciliationBalance IS NULL
        LIMIT 1;
    END IF;

    IF p_ManualItems IS NOT NULL
    THEN
        INSERT INTO t_ManualItems(
            GLEntryID, Amount, Description, glAccountID, GLCostCentreID, GLBillingAccountID)
        SELECT (z.spec ->> 'glentryid')::BIGINT          GLEntryID,
               (z.spec ->> 'amount')::DECIMAL(19, 2)     Amount,
               (z.spec ->> 'description')::VARCHAR       Description,
               (z.spec ->> 'glaccountid')::BIGINT        GLAccountID,
               (z.spec ->> 'glcostcentreid')::BIGINT     GLCostCentreID,
               (z.spec ->> 'glbillingaccountid')::BIGINT GLBillingAccountID
        FROM JSONB_ARRAY_ELEMENTS(p_ManualItems) AS z(spec);
    END IF;

    IF p_ReceiptAmount IS NULL
    THEN
        SELECT SUM(Amount)
        INTO p_ReceiptAmount
        FROM (
                 SELECT ReconciledAmount Amount
                 FROM t_CashReceiptItems
                 UNION
                 SELECT Amount
                 FROM t_ManualItems
                 UNION
                 SELECT COALESCE(p_WriteOffAmount * -1, 0)) a;
    END IF;

    --raise notice '% %', p_receiptamount, (SELECT JSONB_AGG(ROW_TO_JSON(a)) FROM t_ManualItems AS a);

    IF p_cashReceiptItems IS NULL AND p_manualItems IS NULL AND p_barcode IS NULL
    THEN
        --At least one invoice or manual item must be specified
        RAISE SQLSTATE '51040';
    END IF;

    SELECT glaccountidcash,
           glaccountidWriteOff,
           glCostCentreidWriteOff,
           WriteoffAmount,
           COALESCE(iscostcentreused, FALSE)
    INTO
        l_glaccountidcash, l_WriteOffAccount, l_WriteOffCostCentre, l_WriteOffTolerance, l_iscostcentreused
    FROM glSetUp
    LIMIT 1;

    SELECT ID
    INTO l_glCostCentreID
    FROM glcostcentre b
    WHERE b.rowstatus = 'a'
      AND b.glcostcentreidparent IS NULL;

    IF l_WriteOffTolerance IS NOT NULL AND ABS(COALESCE(p_WriteOffAmount, 0)) > l_WriteOffTolerance
    THEN
        --WriteOffAmount than tolerance set in setup
        RAISE SQLSTATE '51041';
    END IF;

    -- END VALIDATION ---------------

    IF p_glBatchID IS NULL
    THEN
        SELECT ID
        INTO p_glBatchID
        FROM glBatch
        WHERE glBatchTypeID = 2 -- Cash Receipt Batch
          AND approvaldate IS NULL
        LIMIT 1;
        IF p_glBatchID IS NULL
        THEN
            INSERT INTO glBatch(
                CreateDate, HasBatchError, sysChangeHistoryID, Description, glBatchTypeID, glBatchStatusID)
            SELECT l_Today                             CreateDate,
                   FALSE                               HasBatchError,
                   l_sysChangeHistoryID                sysChangeHistoryID,
                   'Cash Receipts - ' || l_today::DATE Description,
                   2                                   glBatchTypeID,
                   fnsysGlobal('bs-pending')           glBatchStatusID
            RETURNING ID INTO p_glBatchID;
        END IF;
    ELSE
        IF NOT EXISTS(SELECT * FROM glbatch WHERE glbatchstatusid = fnsysGlobal('bs-pending') AND id = p_glBatchID)
        THEN
            --Batch ID must exist in a Pending state
            RAISE SQLSTATE '51042' USING MESSAGE = p_glBatchID;
        END IF;
    END IF;

    SELECT COALESCE(p_name, 'Null Name') || ' paid $' ||
           COALESCE(CAST(p_receiptAmount AS VARCHAR), 'Null Amount') || ' by ' || description
    INTO l_description
    FROM glpaymentmethod
    WHERE id = p_glPaymentMethodID;

    IF p_glTransactionID IS NULL
    THEN
        INSERT INTO glTransaction (
            createdate, transactiondate, glpostingstatusid, syschangehistoryid)
        SELECT l_Today,
               p_TransactionDate,
               fnsysGlobal('ps-posted'),
               p_sysChangeHistoryID
        RETURNING ID INTO p_glTransactionID;

        INSERT INTO glTransactionSubTypeCashReceipt(gltransactionid, syschangehistoryid)
        SELECT p_glTransactionID, p_sysChangeHistoryID;

        INSERT INTO glEntry(gltransactionid, syschangehistoryid)
        SELECT p_glTransactionID, p_sysChangeHistoryID
        RETURNING ID INTO l_glEntryIDMain;
    END IF;

    UPDATE gltransaction
    SET glbatchid=p_glbatchid,
        glpostingstatusid=fnsysGlobal('ps-pending'),
        gltransactiontypeid=2, --Cash Receipt
        sysdictionarytableidsubtype=fnsysGlobal('dt-cashreceipt'),
        createdate=l_Today,
        description=l_Description,
        referencenumber=p_ReferenceNumber,
        transactiondate=p_TransactionDate,
        syschangehistoryid=p_sysChangeHistoryID,
        glentryidmain=l_glEntryIDMain
    WHERE id = p_glTransactionID;

    UPDATE gltransactionsubtypecashreceipt
    SET crmcontactidenteredby=p_crmcontactiduser,
        crmcontactidpaidfor=p_crmcontactidpaidfor,
        glpaymentmethodid=p_glpaymentmethodid,
        address=l_address,
        amount=p_receiptamount,
        --authorizationno=p_authorizationno, -- Necessary for Moneris
        invoicenumber=p_barcode,
        name=p_name,
        referencenumber=p_referencenumber,
        transactiontag=l_transactiontag,
        syschangehistoryid=p_syschangehistoryid
    WHERE gltransactionidid = p_glTransactionID;

    UPDATE glentry AS a
    SET glaccountid=l_GLAccountIDCash,
        glcostcentreid=l_glCostCentreID,
        glentrytypeid=1, --Cash Receipt Main GlEntry
        amount=p_ReceiptAmount,
        description=l_Description,
        referencenumber=p_ReferenceNumber
        -- When Moneris is implemented this code will be added in
--         referencenumber= CASE WHEN l_isCreditCard = TRUE THEN p_orderID
--                               ELSE p_referenceNumber
--                               END
    WHERE id = l_glEntryIDMain;

/******************************** Create the PTI Postings *******************************************/

    IF p_transactionState = 'edit'
    THEN
/*
         The existing glReconciliation and Manual postings are deleted and re-added. This IS necessary
         because updating existing glReconciliation with new amount IS complex to Create and maintain
         Furthermore, We may have less PTI Entries which means We would need to Get rid of the superfluous items.
         this occurs whether We have been passed CashReceiptItems OR not
*/
        INSERT INTO t_ExistingPostingsNotReferenced(ID)
        SELECT id
        FROM glEntry a
        WHERE a.glTransactionID = p_glTransactionID
          AND a.glentrytypeid = 4; --Payment To Item
    END IF;

    IF EXISTS(SELECT FROM t_CashReceiptItems)
    THEN
        INSERT INTO t_CashReceiptItemDetail(
            pReconciledGLEntryID,
            pReconciledAmount,
            pGLReconciliationID,
            ReconciledGLEntryID,
            ReconciledGLEntryInvoiceID,
            ReconciledGLEntryType,
            ReconciledGLEntryDescription,
            ReconciledGLEntryBillingAccountID,
            ReconciledGLEntryGLAccountID,
            GLReconciliationID1,
            GLReconciliationID2,
            ReconciliationAmount,
            POAGLEntryID)
        SELECT a.ReconciledGLEntryID           pReconciledGLEntryID,
               COALESCE(a.ReconciledAmount, 0) pReconciledAmount,
               a.GLReconciliationID            pGLReconciliationID,
               b.ID                            ReconciledGLEntryID,
               b.ID                            ReconciledGLEntryInvoiceID,
               b.PostingType                   ReconciledGLEntryType,
               b.description                   ReconciledGLEntryDescription,
               b.glBillingAccountID            ReconciledGLEntryBillingAccountID,
               b.GLAccountID                   ReconciledGLEntryGLAccountID,
               r1.GLReconciliationID1          GLReconciliationID1,
               r2.id                           GLReconciliationID2,
               r2.amount                       ReconciliationAmount,
               r1.POAGLEntryID                 POAGLEntryID
        FROM t_CashReceiptItems a
        LEFT JOIN vwGLEntryTransaction b
                  ON b.id = a.ReconciledGLEntryID
        LEFT JOIN LATERAL (SELECT cc.id GLReconciliationID1,
                                  cd.id POAGLEntryID,
                                  cc.glEntryIDFrom,
                                  cc.glEntryIDTo,
                                  cc.Amount
                           FROM glReconciliation cc
                           JOIN glEntry cd
                                ON cd.id = cc.glEntryIDTo
                           JOIN glTransactionSubTypeCashReceipt ce
                                ON ce.glTransactionID = cd.glTransactionID
                           WHERE cc.glEntryIDFrom = b.id
                             AND ce.ID = p_glTransactionID
                           LIMIT 1 ) r1
                  ON TRUE
        LEFT JOIN LATERAL (SELECT *
                           FROM glReconciliation r2
                           WHERE r2.glEntryIDTo = r1.glEntryIDFrom
                             AND r2.glEntryIDFrom = r1.glEntryIDTo
                             AND r2.Amount = r1.Amount * -1
                           LIMIT 1 ) r2
                  ON TRUE;

        FOR l_crRec IN SELECT * FROM t_CashReceiptItemDetail
        LOOP
            IF l_crRec.POAGLEntryID IS NULL
            THEN
-- if the POA glEntry does not exist Then add the POA glEntry as long as the reconciled amount <> 0
                IF l_crRec.pReconciledAmount <> 0
                THEN
                    INSERT INTO glEntry(
                        glTransactionID,
                        GLAccountID,
                        GLCostCentreID,
                        RowIdChargedTo,
                        sysdictionarytableidchargedto,
                        Amount,
                        ReferenceNumber,
                        glentrytypeid,
                        Description,
                        sysChangeHistoryID)
                    SELECT p_glTransactionID                                      glTransactionID,
                           l_crRec.ReconciledGLEntryGLAccountID                   GLAccountID,
                           l_GLCostCentreID                                       GLCostCentreID,
                           l_crRec.ReconciledGLEntryBillingAccountID              RowIdChargedTo,
                           fnsysGlobal('dt-BillingAccount')                       sysdictionarytableidchargedto,
                           l_crRec.pReconciledAmount * -1                         Amount,
                           p_ReferenceNumber                                      ReferenceNumber,
                           4                                                      PostingType,
                           'Payment for ' || l_crRec.ReconciledGLEntryDescription Description,
                           l_sysChangeHistoryID                                   sysChangeHistoryID
                    RETURNING ID INTO l_crRec.POAGLEntryID;

                    INSERT INTO glReconciliation(
                        glEntryIDFrom, glEntryIDTo, Amount, sysChangeHistoryID)
                    SELECT l_crRec.ReconciledGLEntryID,
                           l_crRec.POAGLEntryID,
                           l_crRec.pReconciledAmount,
                           l_sysChangeHistoryID
                    UNION ALL
                    SELECT l_crRec.POAGLEntryID,
                           l_crRec.ReconciledGLEntryID,
                           l_crRec.pReconciledAmount * -1,
                           l_sysChangeHistoryID;
                END IF;
            ELSE
                IF l_crRec.pReconciledAmount <> 0
                THEN
-- Since it is referenced and has Reconciled Amount > 0 then dont delete it
                    DELETE FROM t_ExistingPostingsNotReferenced WHERE id = l_crRec.POAGLEntryID;
-- Otherwise update the glEntry and the related glReconciliation
                    UPDATE GLENTRY
                    SET RowIdChargedTo=l_crRec.ReconciledGLEntryBillingAccountID,
                        Amount=l_crRec.pReconciledAmount * -1,
                        Referencenumber = p_ReferenceNumber,
                        Description='Payment for ' || l_crRec.ReconciledGLEntryDescription
                    WHERE id = l_crRec.POAGLEntryID;

                    IF l_crRec.ReconciliationAmount <> l_crRec.pReconciledAmount
                    THEN
-- the glReconciliation are deleted and re-Added because the ReconciliationBalance trigger IS based on adding and deleting glReconciliation
                        DELETE
                        FROM glReconciliation
                        WHERE id IN (l_crRec.GLReconciliationID1, l_crRec.GLReconciliationID2);
                        INSERT INTO glReconciliation(
                            glEntryIDFrom, glEntryIDTo, Amount, sysChangeHistoryID)
                        SELECT l_crRec.ReconciledGLEntryID,
                               l_crRec.POAGLEntryID,
                               l_crRec.pReconciledAmount,
                               l_sysChangeHistoryID
                        UNION ALL
                        SELECT l_crRec.POAGLEntryID,
                               l_crRec.ReconciledGLEntryID,
                               l_crRec.pReconciledAmount * -1,
                               l_sysChangeHistoryID;
                    END IF;
                END IF;
            END IF;
        END LOOP;
    END IF;

/********************************* Create the Manual Postings *******************************************/

    IF p_manualItems IS NOT NULL
    THEN

        INSERT INTO t_ManualItemDetail(
            pManualGLEntryID,
            pManualAmount,
            pManualDescription,
            pManualGLAccountID,
            pManualGLCostCentreID,
            pManualBillingAccountID,
            ManualGLEntryID,
            ManualAmount,
            ManualDescription,
            ManualGLAccountID,
            ManualGLCostCentreID,
            ManualBillingAccountID)
        SELECT a.glEntryID   PManualGLEntryID
             , a.Amount      pManualAmount
             , a.Description pManualDescription
             , a.glAccountID
             , COALESCE(a.glCostCentreID, l_glCostCentreID)
             , a.glBillingAccountID
             , b.ID                 --glEntryID
             , b.Amount             --Amount Posted
             , b.description        --Description of glEntry being paid
             , b.GLAccountID        --GLAccount of glEntry being paid
             , b.GLCostCentreID     --GLCostCentre of glEntry being paid
             , b.glBillingAccountID --glBillingAccount of glEntry being paid
        FROM t_ManualItems a
        LEFT JOIN vwGLEntryTransaction b
                  ON b.id = a.GLEntryID;

--         raise notice '%', (SELECT JSONB_AGG(ROW_TO_JSON(a)) FROM t_ManualItemDetail AS a);

        IF P_TRANSACTIONSTATE = 'edit'
        THEN
            -- existing glEntry that were created but not referenced in the new Set of data need to be deleted
            INSERT INTO t_ExistingPostingsNotReferenced(ID)
            SELECT ID
            FROM glEntry
            WHERE glTransactionID = p_glTransactionID
              AND glentrytypeid IN (3, 5, 6) -- Payment on Account, Manual Cash Receipt Item (CRM), Write-off (W/O)
              AND ID NOT IN (
                                SELECT ManualGLEntryID FROM t_ManualItemDetail WHERE ManualGLEntryID IS NOT NULL);
        END IF;

        FOR l_miRec IN SELECT * FROM t_ManualItemDetail
        LOOP
            IF l_miRec.pManualGLEntryID IS NOT NULL AND l_miRec.ManualGLEntryID IS NULL
            THEN
                -- Manual glEntry pManualGLEntryID does not exist';
                RAISE SQLSTATE '51043' USING MESSAGE = COALESCE(l_miRec.pManualGLEntryID, 'null');
            END IF;

            IF l_miRec.pManualDescription IS NULL
            THEN
                --Manual glEntry Description must be specified for Manual glEntry
                RAISE SQLSTATE '51044' USING MESSAGE = COALESCE(l_miRec.pManualGLEntryID, 'null');
            END IF;

            IF l_miRec.pManualAmount IS NULL
            THEN
                -- Manual glEntry Amount must be specified for Manual glEntry pManualGLEntryID
                RAISE SQLSTATE '51045' USING MESSAGE = COALESCE(l_miRec.pManualGLEntryID, 'null');
            END IF;

            IF l_miRec.pManualGLAccountID IS NULL
            THEN
                --Manual glEntry GLAccountID must be specified for Manual glEntry pManualGLEntryID
                RAISE SQLSTATE '51046' USING MESSAGE = COALESCE(l_miRec.pManualGLEntryID, 'null');
            END IF;

            IF l_miRec.pManualGLEntryID IS NULL
            THEN
-- if the Manual glEntry does not exist Then add the Manual glEntry as long as the reconciled amount <> 0
                INSERT INTO glEntry(
                    glTransactionID,
                    GLAccountID,
                    GLCostCentreID,
                    RowIdChargedTo,
                    sysdictionarytableidchargedto,
                    Amount,
                    ReferenceNumber,
                    glentrytypeid,
                    Description,
                    sysChangeHistoryID)
                SELECT p_glTransactionID                                         glTransactionID,
                       l_miRec.pManualGLAccountID                                GLAccountID,
                       COALESCE(l_miRec.pManualGLCostCentreID, l_glCostCentreID) GLCostCentreID,
                       l_miRec.pManualBillingAccountID                           RowIdChargedTo,
                       CASE WHEN l_miRec.pManualBillingAccountID IS NOT NULL THEN fnsysGlobal('dt-BillingAccount')
                            END                                                  sysdictionarytableidchargedto,
                       l_miRec.pManualAmount * -1                                Amount,
                       NULL                                                      ReferenceNumber,
                       5                                                         PostingType, -- Manual Cash Receipt Item
                       l_miRec.pManualDescription                                Description,
                       l_sysChangeHistoryID                                      sysChangeHistoryID;
            ELSE
-- Otherwise update the glEntry
                UPDATE glEntry
                SET RowIdChargedTo=l_miRec.ManualBillingAccountID,
                    sysdictionarytableidchargedto=CASE WHEN l_miRec.ManualBillingAccountID IS NOT NULL
                                                           THEN fnsysGlobal('dt-BillingAccount')
                                                       END,
                    Amount=l_miRec.pManualAmount * -1,
                    Description=l_miRec.pManualDescription,
                    GLAccountID=l_miRec.pManualGLAccountID,
                    GLCostCentreID=COALESCE(l_miRec.pManualGLCostCentreID, l_glCostCentreID)
                WHERE id = l_miRec.pManualGLEntryID
                  AND ((l_miRec.ManualGLAccountID <> l_miRec.pManualBillingAccountID) OR
                       (l_miRec.ManualAmount <> l_miRec.pManualAmount) OR
                       (l_miRec.ManualGLAccountID <> l_miRec.pManualGLAccountID) OR
                       (l_miRec.ManualGLCostCentreID <> COALESCE(l_miRec.pManualGLCostCentreID, l_glCostCentreID)) OR
                       (l_miRec.ManualDescription <> l_miRec.pManualDescription));
            END IF;

        END LOOP;

    ELSE
        -- if We were not passed Manual Items Then We need to delete any that exist
        IF p_transactionState = 'edit'
        THEN
            INSERT INTO t_ExistingPostingsNotReferenced(ID)
            SELECT ID
            FROM glEntry
            WHERE glTransactionID = p_glTransactionID
              AND glEntryTypeid IN (3, 5, 6); -- Payment to Account, Manual Cash Receipt Item (CRM), Write-off (W/O);
        END IF;
    END IF;

    IF EXISTS(SELECT 1 FROM t_ExistingPostingsNotReferenced)
    THEN
        DELETE
        FROM glReconciliation
        WHERE glEntryIDFrom IN (
                                   SELECT id
                                   FROM t_ExistingPostingsNotReferenced)
           OR glEntryIDTo IN (
                                 SELECT id
                                 FROM t_ExistingPostingsNotReferenced);

        DELETE
        FROM glEntry
        WHERE id IN (
                        SELECT id
                        FROM t_ExistingPostingsNotReferenced);
    END IF;

/******************************** Create Refund/Write off postings *******************************************/

    IF COALESCE(p_writeOffAmount, 0) <> 0
    THEN
        INSERT INTO glEntry(
            glTransactionID,
            GLAccountID,
            glcostcentreid,
            RowIdChargedTo,
            sysdictionarytableidchargedto,
            Amount,
            ReferenceNumber,
            glentrytypeid,
            Description,
            sysChangeHistoryID)
        SELECT p_glTransactionID                  glTransactionID,
               l_WriteOffAccount                  GLAccountID,
               l_glcostcentreid                   GLCostCentreID,
               NULL                               RowIdChargedTo,
               NULL                               sysdictionarytableidchargedto,
               p_writeOffAmount                   Amount,
               NULL                               ReferenceNumber,
               6                                  PostingType, --write off
               'Write Off for ' || p_name || ' on Cash Receipt ' ||
               CAST(p_glTransactionID AS VARCHAR) Description,
               l_sysChangeHistoryID               sysChangeHistoryID;
    END IF;

    -- '%', (SELECT to_jsonb(array_agg(a.amount)) FROM S0001V0000.vwglentrytransaction a where gltransactionid=p_gltransactionid);

    l_TransactionBalance := (
                                SELECT SUM(amount)
                                FROM glEntry
                                WHERE glTransactionID = p_glTransactionID);
    IF COALESCE(l_TransactionBalance, 0) <> 0
    THEN
        --Cash receipt does not balance. It is out by TransactionBalance
        RAISE SQLSTATE '51048' USING MESSAGE = l_TransactionBalance;
    END IF;

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
$$
    LANGUAGE plpgsql


