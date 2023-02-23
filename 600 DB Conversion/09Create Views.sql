/*This file is part of the 3D Enterprise System Platform. The 3D Enterprise System Platform is free
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
OVERVIEW
This creates views based on the dictionary
*/

DO
$ManualConv$
    DECLARE
        l_SelectViewsSQL VARCHAR := '';
        l_replaceSQL1    VARCHAR := '';
        l_replaceSQL2    VARCHAR := '';

    BEGIN

        SET SEARCH_PATH TO DEFAULT;

        CALL spsysViewCreate('s0001v0000');

        SET SEARCH_PATH TO s0000v0000, public;

        SELECT STRING_AGG(REPLACE(REPLACE('
alter table l_tablename rename to l_newtablename;
', 'l_tablename', table_name), 'l_newtablename', SUBSTRING(table_name, 3, 999)), '')
        INTO l_replacesql1
        FROM information_schema.tables
        WHERE table_schema = 's0000v0000'
          AND table_type = 'BASE TABLE'
          AND table_name LIKE 'c\_%';

        IF l_replacesql1 IS NOT NULL
        THEN
            EXECUTE (l_replacesql1);
        END IF;

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
                                                                    CAST(tx.gltransactionidreversed AS VARCHAR)
                   WHEN rtx.id IS NOT NULL THEN 'Reversed by Transaction ' || CAST(rtx.id AS VARCHAR)
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

        DROP VIEW IF EXISTS S0000V0000.vwcrmcontactaddress CASCADE;
        CREATE VIEW S0000V0000.vwcrmcontactaddress
        AS
/*
This view is the primary view for displaying addresses.  Note addresses are typically connected to add contact
but can also be connected to add location.

20210608    Blair Kjenner   Initial Code

*/
        SELECT add.id,
               at.description                        AS addresstype,
               crm.name                              AS contactname,
               add.address1,
               add.address2,
               add.address3,
               add.address4,
               COALESCE(cit.name, add.city)          AS city,
               COALESCE(prv.name, add.provincestate) AS provincestate,
               cou.name,
               add.postalzip,
               add.additionalinformation,
               add.isprimaryaddress,
               add.crmaddressidinheritedfrom,
               add.comcityid,
               cit.comprovincestateid,
               cit.comcountryid,
               add.crmaddresstypeid,
               crm.id                                AS crmContactID,
               add.effectivedate,
               add.rowstatus
        FROM crmaddress add
        JOIN      crmcontact crm
                  ON crm.id = add.crmContactID
        LEFT JOIN comcity cit
                  ON cit.id = add.comcityid
        LEFT JOIN comprovincestate prv
                  ON prv.id = cit.comprovincestateid
        LEFT JOIN comcountry cou
                  ON cou.id = cit.comcountryid
        LEFT JOIN crmaddresstype at
                  ON at.id = add.crmaddresstypeid
        WHERE add.rowstatus = 'a';

        SELECT STRING_AGG(REPLACE(REPLACE('
alter table l_tablename rename to l_newtablename;
', 'l_tablename', table_name), 'l_newtablename', 'c_' || table_name), '')
        INTO l_replacesql2
        FROM information_schema.tables
        WHERE table_schema = 's0000v0000'
          AND table_type = 'BASE TABLE';

        RAISE NOTICE '%', l_SelectViewsSQL;
        IF l_replacesql2 IS NOT NULL
        THEN
            EXECUTE (l_replacesql2);
        END IF;

        SET SEARCH_PATH TO DEFAULT;

    END;
$ManualConv$

