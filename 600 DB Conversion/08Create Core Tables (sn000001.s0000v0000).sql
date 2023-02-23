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
This script creates the core data in the s0000v0000 schema
*/

DO
$$
    BEGIN

        CALL spsysChangeHistoryDisableTriggers(p_debug := FALSE);
        CALL spsysForeignKeyConstraintGenerate(TRUE);

        CALL spsysSchemaCopy('s0001V0000', 's0000V0000', NULL, 'c_', TRUE);

        PERFORM SET_CONFIG('search_path', 's0000V0000,' || CURRENT_SETTING('search_path'), FALSE);

        -- Temporarily disables triggers for foreign key constraints
        -- and potentially change history triggers
        SET session_replication_role = REPLICA;
        DELETE FROM c_acttype;
        DELETE FROM c_fatype;
        DELETE FROM c_glAccountBalance;
        DELETE FROM c_glReconciliation;
        DELETE FROM c_glEntry;
        DELETE FROM c_glBudget;
        DELETE FROM c_glTransactionSubTypeCashReceipt;
        DELETE FROM c_glTransactionSubTypeCheque;
        DELETE FROM c_gltransactionsubtypeinvoice;
        DELETE FROM c_glTransaction;
        DELETE FROM c_glBatch;
        DELETE FROM c_glBillingAccount;
        DELETE FROM c_crmcontactsubtypeuser;
        DELETE FROM c_crmcontactsubtypeemployee;
        DELETE FROM c_crmcontact;
        DELETE FROM c_crmAddress;
        DELETE FROM c_crmAddressEmail;
        DELETE FROM c_crmAddressPhone;
        DELETE FROM c_crmRelationship;
        DELETE FROM c_faFixedAsset;
        DELETE FROM c_faLocationHistory;
        DELETE FROM c_glDeposit;
        DELETE FROM c_actProjectResourceAllocation;
        DELETE FROM c_actRateBilling;
        DELETE FROM c_actRateExpense;
        DELETE FROM c_actRateFactor;
        DELETE FROM c_actActivity;
        DELETE FROM c_actActivitySubTypeBilling;
        DELETE FROM c_actProject;
        DELETE FROM c_comAttributeDetail;
        DELETE FROM c_comCrossReference;
        DELETE FROM c_comPersonalComment;
        DELETE FROM c_hrPosition;
        DELETE FROM c_sysChangeHistory;
        DELETE FROM c_comAttribute;
        DELETE FROM c_comCrossReferenceType;
        DELETE FROM c_sysForeignKeyCache;

        DELETE FROM c_glaccount WHERE id >= 10000000000000;

        -- Enables triggers
        SET session_replication_role = origin;

        SET SEARCH_PATH TO DEFAULT;

        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyConstraintGenerate();

    END
$$



