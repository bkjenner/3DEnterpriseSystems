DROP FUNCTION IF EXISTS S0000V0000.fnglCashReceiptManualItems CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnglCashReceiptManualItems(p_glTransactionID BIGINT)
    RETURNS TABLE
            (
                ID                 BIGINT,
                GLAccountID        BIGINT,
                GLAccount          VARCHAR,
                glBillingAccountID BIGINT,
                ContactNumber      VARCHAR,
                Name               VARCHAR,
                Description        VARCHAR,
                Amount             DECIMAL(19, 2)
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
20210821 Blair Initial Code
select * from fnglCashReceiptManualItems(p_glTransactionID:=507767)
*/
DECLARE
    l_InvoiceID    BIGINT;
    l_crmContactID BIGINT;
    l_Error        RECORD;
    e_State        TEXT;
    e_Msg          TEXT;
    e_Context      TEXT;
BEGIN

    IF NOT EXISTS(SELECT FROM glTransactionSubTypeCashReceipt a WHERE a.ID = p_glTransactionID LIMIT 1)
    THEN
        --Transaction ID % does not exists.
        RAISE SQLSTATE '51038' USING MESSAGE = p_glTransactionID;
    END IF;

    DROP TABLE IF EXISTS t_LineItem;
    CREATE TEMP TABLE t_LineItem
    AS
    SELECT pManG.ID            ID,
           pManG.GLAccountID   GLAccountID,
           ga.Description      GLAccount,
           pManB.id            glBillingAccountID,
           pManC.ContactNumber ContactNumber,
           pManB.Name          BillingAccountName,
           pManG.Description   Description,
           pManG.Amount * -1   Amount
    FROM glEntry pManG
    JOIN      GLAccount ga
              ON ga.id = pManG.GLAccountid
    LEFT JOIN GLBillingAccount pManB
              ON pManB.ID = pManG.Rowidchargedto AND
                 pManG.sysdictionarytableidchargedto = fnsysglobal('dt-billingaccount')
    LEFT JOIN crmcontact pManC
              ON pManC.ID = pManB.crmcontactid
    WHERE pManG.glentrytypeid = 4 -- payment toward invoice
      AND pManG.gltransactionid = p_glTransactionID
      AND pManG.glentrytypeid IN (3, 5, 6); --Payment on Account, Refund, Write-off

    RETURN QUERY SELECT *
                 FROM t_LineItem a;

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
