DROP VIEW IF EXISTS S0000V0000.vwGLEntryTransaction CASCADE;
CREATE VIEW S0000V0000.vwGLEntryTransaction
/*
This view is the primary view for connecting glentries and transactions.

20210217    Blair Kjenner   Initial Code

select * from vwGLEntryTransaction

*/
AS
SELECT GE.ID,
       GE.glTransactionID,
       GE.GLAccountID,
       GE.GLCostCentreId,
       GE.Description,
       GE.Amount,
       GE.ReconciliationBalance,
       COALESCE(GE.ReportingPeriodDate, TX.transactionDate) ReportingPeriodDate,
       TX.TransactionDate,
       TX.glPostingStatusID,
       TX.glBatchID,
       DT.name                                              SubType,
       TX.ReferenceNumber,
       GE.Comments,
       TX.Description  AS                                   TransactionDescription,
       GE.glentrytypeid,
       RGT.Description AS                                   PostingType,
       RTT.Description AS                                   TransactionType,
       TX.gltransactionidreversed                           ReversalID,
       B.glBatchTypeID,
       GE.sysChangeHistoryID,
       bl.crmContactID,
       c.ContactNumber                                      ContactNumber,
       bl.Name                                              BillingAccountName,
       bl.id                                                glBillingAccountID,
       CASE
           WHEN tx.gltransactionidreversed IS NOT NULL THEN 'Reverses Transaction ' ||
                                                            CAST(tx.gltransactionidreversed AS varchar)
           WHEN rtx.id IS NOT NULL                     THEN 'Reversed by Transaction ' || CAST(rtx.id AS varchar)
           END                                              ReversalStatus,
       EXTRACT(YEAR FROM tx.TransactionDate)                BillingYear
FROM glEntry GE
JOIN      glTransaction TX
          ON GE.glTransactionID = TX.ID
JOIN      glBatch B
          ON TX.glBatchID = B.ID
LEFT JOIN glBillingAccount BL
          ON ge.Rowidchargedto = BL.ID AND ge.sysdictionarytableidchargedto = fnsysGlobal('DT-BillingAccount')
LEFT JOIN crmContact c
          ON bl.crmContactID = c.id
LEFT JOIN glTransaction rtx
          ON ge.glTransactionID = rtx.gltransactionidreversed
LEFT JOIN gltransactiontype RTT
          ON tx.gltransactiontypeid = RTT.ID
LEFT JOIN glEntryType RGT
          ON ge.glentrytypeid = RGT.ID
LEFT JOIN sysDictionaryTable DT
          ON DT.ID = tx.sysdictionarytableidsubtype;

