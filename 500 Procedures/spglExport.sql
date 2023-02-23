CREATE OR REPLACE PROCEDURE S0000V0000.spglExport(p_BatchIDs VARCHAR DEFAULT NULL,
                                                  p_sysChangeHistoryID BIGINT DEFAULT NULL)
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

This procedure exports data to an external financial system like Great Plains.

20210306    Blair Kjenner   Initial Code

call spglExport()

*/
DECLARE
    l_BatchExportID BIGINT;
    l_batchid       BIGINT;
    l_batchidarray  BIGINT[];
    l_glaccountname VARCHAR;
    e_State         TEXT;
    e_Msg           TEXT;
    e_Context       TEXT;
    l_error         RECORD;
BEGIN
    p_batchids := COALESCE(TRIM(p_batchids), '');

    l_batchidarray := STRING_TO_ARRAY(p_BatchIDs, ',');

    SELECT a.id
    INTO l_batchid
    FROM glBatch a
    WHERE a.glbatchstatusid = fnsysGlobal('bs-pending')
      AND ID = ANY (l_batchidarray)
    LIMIT 1;

    IF l_batchid IS NOT NULL
    THEN
        RAISE SQLSTATE '51021' USING MESSAGE = l_BatchID;
    END IF;

    SELECT a.id
    INTO l_batchid
    FROM glBatch a
    WHERE a.glbatchstatusid = fnsysGlobal('bs-integrated')
      AND ID = ANY (l_batchidarray)
    LIMIT 1;

    IF l_batchid IS NOT NULL
    THEN
        RAISE SQLSTATE '51022' USING MESSAGE = l_BatchID;
    END IF;

    DROP TABLE IF EXISTS t_FinancialExport;

    CREATE TEMP TABLE t_FinancialExport
    AS
    SELECT *
    FROM (
             SELECT a.glBatchID,
                    CAST(a.TransactionDate AS DATE) TransactionDate,
                    c.ReferenceNumber               AccountNumber,
                    c.description                   GLAccountName,
                    b.Description,
                    SUM(a.amount)                   Amount
             FROM vwGLEntryTransaction a
             JOIN vwglbatch b
                  ON b.id = a.glBatchID
             JOIN glAccount c
                  ON c.id = a.GLaccountID
             WHERE b.glbatchstatusid = fnsysGlobal('bs-approved')
               AND c.iscollapseonexportrequired = TRUE
               AND a.glPostingStatusID = fnsysGlobal('PS-Posted')
               AND (p_BatchIDs = '' OR a.glBatchID = ANY (l_batchidarray))
             GROUP BY a.glBatchID, a.TransactionDate, c.ReferenceNumber, b.Description, c.description, b.BatchType
             HAVING SUM(a.amount) <> 0
             UNION ALL
             SELECT a.glBatchID,
                    CAST(a.TransactionDate AS DATE) TransactionDate,
                    c.ReferenceNumber               AccountNumber,
                    c.description                   GLAccountName,
                    b.Description,
                    a.amount
             FROM vwGLEntryTransaction a
             JOIN vwglbatch b
                  ON b.id = a.glBatchID
             JOIN glAccount c
                  ON c.id = a.GLaccountID
             WHERE b.glbatchstatusid = fnsysGlobal('bs-approved')
               AND c.iscollapseonexportrequired = FALSE
               AND a.glPostingStatusID = fnsysGlobal('PS-Posted')
               AND a.amount <> 0
               AND (p_BatchIDs = '' OR a.glBatchID = ANY (l_batchidarray))) a
    ORDER BY glBatchID, transactiondate, AccountNumber;

    IF EXISTS(SELECT FROM t_FinancialExport)
    THEN
        INSERT INTO glExportBatch(ExportDate, sysChangeHistoryID)
        VALUES (NOW()::TIMESTAMP, p_sysChangeHistoryID)
        RETURNING id INTO l_BatchExportID;
    ELSE
        RAISE SQLSTATE '51023';
    END IF;

    SELECT glaccountname INTO l_glaccountname FROM t_FinancialExport WHERE COALESCE(AccountNumber, '') = '' LIMIT 1;
    IF l_glaccountname IS NOT NULL
    THEN
        RAISE SQLSTATE '51024' USING MESSAGE = l_glaccountname;
    END IF;

    INSERT INTO glExportTransaction (glbatchid, description, transactiondate, glexportbatchid)
    SELECT DISTINCT glBatchID,
                    Description,
                    TransactionDate,
                    l_BatchExportID
    FROM t_FinancialExport;
    INSERT INTO glexportentry(AccountNumber, Amount, glExportTransactionID, Description)
    SELECT AccountNumber,
           SUM(Amount),
           b.ID,
           a.Description
    FROM t_FinancialExport a
    JOIN glexporttransaction b
         ON b.glBatchID = a.glBatchID AND b.TransactionDate = a.TransactionDate AND b.glexportbatchid = l_BatchExportID
    GROUP BY b.id, AccountNumber, a.Description;

    UPDATE glBatch a
    SET glExportBatchID=l_BatchExportID
    FROM LATERAL (SELECT glexportbatchid
                  FROM glExportTransaction b
                  WHERE b.glBatchID = id
                    AND b.glexportbatchid = l_BatchExportID
                  LIMIT 1 ) b;

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



