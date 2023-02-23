CREATE OR REPLACE FUNCTION S0000V0000.fnsysGetTranslation(p_sysDictionaryTableID BIGINT, p_RowID BIGINT,
                                                          p_schemaname VARCHAR DEFAULT NULL)
    RETURNS VARCHAR
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
This function translates ids into the associated description or name from the associated table

20210317    Blair Kjenner   Initial Code

select fnsysGetTranslation(300, 1300) -- returns description for the record with id 10 in the glaccount table

*/
DECLARE
    l_sTranslation VARCHAR;
    l_searchpath   VARCHAR;
BEGIN
    l_searchpath := CURRENT_SETTING('search_path');
    p_schemaname := LOWER(COALESCE(p_schemaname, CURRENT_SCHEMA()));

    PERFORM SET_CONFIG('search_path', p_schemaname || ',' || l_searchpath, TRUE);

    IF p_RowID IS NOT NULL
    THEN
        l_sTranslation := (
                              SELECT CASE
                                         WHEN p_sysDictionaryTableID = 1 THEN (
                                                                                  SELECT x.Name
                                                                                  FROM sysDictionaryTable x
                                                                                  WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 2 THEN (
                                                                                  SELECT x.Name
                                                                                  FROM sysDictionaryColumn x
                                                                                  WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 8 THEN (
                                                                                  SELECT x.Message
                                                                                  FROM sysError x
                                                                                  WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 9 THEN (
                                                                                  SELECT x.Description
                                                                                  FROM sysMessage x
                                                                                  WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 13 THEN (
                                                                                   SELECT x.Name
                                                                                   FROM sysCommand x
                                                                                   WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 42 THEN (
                                                                                   SELECT x.Description
                                                                                   FROM sysMultilinkTableRule x
                                                                                   WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 100 THEN (
                                                                                    SELECT x.Name
                                                                                    FROM crmContact x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 110 THEN (
                                                                                    SELECT (
                                                                                               SELECT aa.name || ' - ' || x.address1
                                                                                               FROM crmContact aa
                                                                                               WHERE aa.id = x.crmContactID
                                                                                               LIMIT 1)
                                                                                    FROM crmAddress x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 113 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM crmAddressPhoneType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 114 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM crmAddressType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 121 THEN (
                                                                                    SELECT x.PrimaryName
                                                                                    FROM crmRelationshipType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 130 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM crmGender x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 131 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM crmLanguage x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 132 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM crmSalutation x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 200 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM faFixedAsset x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 206 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM faDepreciationMethod x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 207 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM faDisposalReason x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 208 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM faStatus x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 209 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM faType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 300 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glAccount x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 301 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glCostCentre x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 310 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glBatch x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 311 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glTransaction x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 315 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glTransactionType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 316 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glEntry x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 317 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glEntryType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 321 THEN (
                                                                                    SELECT x.Name
                                                                                    FROM glBillingAccount x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 323 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glBillingAccountStatus x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 340 THEN (
                                                                                    SELECT CAST(x.DepositDate AS VARCHAR)
                                                                                    FROM glDeposit x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 391 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glBatchType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 392 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glBillingMethod x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 393 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glEFTType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 395 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glPostingStatus x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 396 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glRate x
                                                                                    WHERE x.ID = p_RowID
                                                                                      AND x.TemporalEndDate = '9999-12-31')
                                         WHEN p_sysDictionaryTableID = 397 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glRateType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 398 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM glAccountType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 400 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 401 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actProject x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 403 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actBillingStatus x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 404 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actSource x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 405 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actStatus x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 406 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actCostUnit x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 407 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actBillingMethod x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 408 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actPriority x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 420 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM actActivity x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 500 THEN (
                                                                                    SELECT x.Name
                                                                                    FROM comCity x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 501 THEN (
                                                                                    SELECT x.Name
                                                                                    FROM comProvinceState x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 502 THEN (
                                                                                    SELECT x.name
                                                                                    FROM comCountry x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 503 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM comLocation x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 510 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM comAttribute x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 520 THEN (
                                                                                    SELECT x.Number
                                                                                    FROM comCrossReference x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 521 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM comCrossReferenceType x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 600 THEN (
                                                                                    SELECT x.WorkingTitle
                                                                                    FROM hrPosition x
                                                                                    WHERE x.ID = p_RowID
                                                                                      AND x.TemporalEndDate = '9999-12-31')
                                         WHEN p_sysDictionaryTableID = 601 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM hrEmploymentStatus x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 602 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM hrGrade x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 603 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM hrPositionClassification x
                                                                                    WHERE x.ID = p_RowID)
                                         WHEN p_sysDictionaryTableID = 604 THEN (
                                                                                    SELECT x.Description
                                                                                    FROM hrPositionType x
                                                                                    WHERE x.ID = p_RowID)
                                         ELSE CAST(p_RowID AS VARCHAR)
                                         END);
        IF p_RowID IS NOT NULL
            AND l_sTranslation != p_ROWID::VARCHAR
            AND NOT EXISTS(SELECT
                           FROM sysforeignkeycache s
                           WHERE s.sysdictionarytableid = p_sysDictionaryTableID
                             AND s.rowid = p_RowID)
        THEN
            --RAISE NOTICE 'l_sTranslation % p_RowID % p_sysDictionaryTableID %', l_sTranslation, p_RowID, p_sysDictionaryTableID;
            INSERT INTO sysforeignkeycache (translation, rowid, sysdictionarytableid)
            SELECT l_sTranslation, p_RowID, p_sysDictionaryTableID;
        END IF;
    END IF;
    PERFORM SET_CONFIG('search_path', l_searchpath, TRUE);
    RETURN COALESCE(l_sTranslation, fnsysIDView(p_RowID));
END;
$$ LANGUAGE plpgsql
