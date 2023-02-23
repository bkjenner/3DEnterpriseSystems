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
This script does all conversion tasks that could not be automatically completed
  in the previous steps
*/

DO
$$
    DECLARE
        l_crmContactidSystem       BIGINT;
        l_crmContactidOrganization BIGINT;
        l_SQL                      TEXT;
    BEGIN

        PERFORM SET_CONFIG('search_path',
                           CURRENT_SETTING('search_path') || ',_staging', FALSE);

        -- I tried creating these indexes and running spglAccountBalanceUpdate
        -- and these indexes slowed it down??
        -- 20210819
        -- Later I found these indexes make a massive difference
        DROP INDEX IF EXISTS ix_glentry_gltransactionid;
        CREATE INDEX ix_glentry_gltransactionid
            ON glentry (gltransactionid, id);

        DROP INDEX IF EXISTS ix_gltransaction_gltransactionidreversedid;
        CREATE INDEX ix_gltransaction_gltransactionidreversedid
            ON gltransaction (gltransactionidreversed, id);

        DROP INDEX IF EXISTS ix_gltransaction_glpostingstatusid;
        CREATE INDEX ix_gltransaction_glpostingstatusid
            ON gltransaction (glpostingstatusid);
        DROP INDEX IF EXISTS ix_gltransaction_glbatchid;
        CREATE INDEX ix_gltransaction_glbatchid
            ON gltransaction (glbatchid);
        DROP INDEX IF EXISTS ix_gltransaction_transactiondate;
        CREATE INDEX ix_gltransaction_transactiondate
            ON gltransaction (transactiondate);
        DROP INDEX IF EXISTS ix_glentry_rollupamount;
        CREATE INDEX ix_glentry_rollupamount
            ON glentry (rollupamount);
        DROP INDEX IF EXISTS ix_glentry_glaccountid;
        CREATE INDEX ix_glentry_glaccountid
            ON glentry (glaccountid, glcostcentreid);
        DROP INDEX IF EXISTS ix_glentry_glcostcentreid;
        CREATE INDEX ix_glentry_glcostcentreid
            ON glentry (glcostcentreid, glaccountid);
        DROP INDEX IF EXISTS ix_glreconciliation_glentryidfrom;
        CREATE INDEX ix_glreconciliation_glentryidfrom
            ON glreconciliation (glentryidfrom, id);

        UPDATE crmAddress
        SET crmAddresstypeid=(
                                 SELECT id
                                 FROM crmAddressType
                                 WHERE Description = 'Business')
          , IsPrimaryAddress= TRUE
          , crmContactID=ID, rowstatus='a';

        UPDATE crmAddressPhone
        SET crmAddresstypeid=(
                                 SELECT id
                                 FROM crmAddressType
                                 WHERE Description = 'Business'
                                 LIMIT 1), crmContactID=ID, rowstatus='a';

        DELETE
        FROM crmAddressPhone
        WHERE Phone IS NULL;

        UPDATE crmAddressEmail
        SET crmAddresstypeid=(
                                 SELECT id
                                 FROM crmAddressType
                                 WHERE Description = 'Business'), crmContactID=ID, rowstatus='a';

        DELETE
        FROM crmAddressEmail
        WHERE Email IS NULL;

        INSERT INTO KeyTranslation(OldId, TableName)
        SELECT DISTINCT ID, 'aacountry'
        FROM aacountry a
        WHERE NOT EXISTS(SELECT 1
                         FROM KeyTranslation b
                         WHERE b.oldid = a.id::VARCHAR
                           AND TableName = 'aacountry'
                         LIMIT 1);

        TRUNCATE TABLE comCountry;

        INSERT INTO comCountry(ID, ShortCode, Name, RowStatus)
        SELECT DISTINCT bb.Newid,    --ID,
                        a.ShortCode, --ShortCode,
                        a.name,      --Name,
                        'a'          --RecordStatus
        FROM aacountry a
        LEFT JOIN KeyTranslation bb
                  ON bb.OldId = a.ID::VARCHAR AND bb.TableName = 'aacountry';


        INSERT INTO KeyTranslation(OldId, TableName)
        SELECT DISTINCT ID, 'aaProvince'
        FROM aaProvince a
        WHERE NOT EXISTS(SELECT 1
                         FROM KeyTranslation b
                         WHERE b.oldid = a.id::VARCHAR
                           AND TableName = 'aaProvince'
                         LIMIT 1);

        TRUNCATE TABLE comProvinceState;

        INSERT INTO comProvinceState(ID, comCountryID, ShortCode, Name, RowStatus)
        SELECT DISTINCT bb.Newid,    --ID,
                        cc.NewID,    --comCountry,
                        a.ShortCode, --ShortCode,
                        a.Name,      --Name,
                        'a'          --RecordStatus
        FROM aaProvince a
        LEFT JOIN KeyTranslation bb
                  ON bb.OldId = a.ID::VARCHAR AND bb.TableName = 'aaProvince'
        LEFT JOIN KeyTranslation cc
                  ON cc.OldId = a.CountryID::VARCHAR AND bb.TableName = 'aaCountry';

        INSERT INTO KeyTranslation(OldId, TableName)
        SELECT DISTINCT id, 'aaCity'
        FROM aaCity a
        WHERE NOT EXISTS(SELECT 1
                         FROM KeyTranslation b
                         WHERE b.oldid = a.id::VARCHAR
                           AND TableName = 'aaCity'
                         LIMIT 1);

        TRUNCATE TABLE comCity;

        INSERT INTO comCity(ID, comProvinceStateID, Name, RowStatus)
        SELECT bb.Newid ID, dd.NewID Province, a.Name, 'a' RecordStatus
        FROM aaCity a
        LEFT JOIN KeyTranslation bb
                  ON bb.OldId = a.ID::VARCHAR AND bb.TableName = 'aaCity'
        LEFT JOIN KeyTranslation dd
                  ON DD.OldId = a.ProvinceID::VARCHAR AND dd.TableName = 'aaProvince';

        UPDATE crmAddress
        SET ProvinceState='ON'
        WHERE ProvinceState LIKE 'ON%';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE PostalZip LIKE 'T%';

        UPDATE crmAddress
        SET ProvinceState='BC'
        WHERE PostalZip LIKE 'V%';

        UPDATE crmAddress
        SET ProvinceState='NT'
        WHERE ProvinceState LIKE 'northwest%'
           OR REPLACE(ProvinceState, '.', '') = 'NWT';

        UPDATE crmAddress
        SET postalzip='T5J3G2'
        WHERE provincestate = 'Alberta T5J 3G2';

        UPDATE crmAddress
        SET postalzip='T6E3P4'
        WHERE provincestate = 'Alberta T6E 3P4';

        UPDATE crmAddress
        SET postalzip='T5J2V4'
        WHERE provincestate = 'Alberta, T5J 2V4';

        UPDATE crmAddress
        SET postalzip='90066'
        WHERE provincestate = 'CA 90066';

        UPDATE crmAddress
        SET postalzip='91423'
        WHERE provincestate = 'CA 91423, USA';

        UPDATE crmAddress
        SET postalzip='94550'
        WHERE provincestate = 'CA 94550';

        UPDATE crmAddress
        SET postalzip='01742'
        WHERE provincestate = 'MA 01742';

        UPDATE crmAddress
        SET postalzip='02114'
        WHERE provincestate = 'MA 02114-0034';

        UPDATE crmAddress
        SET postalzip='63146'
        WHERE provincestate = 'MO 63146';

        UPDATE crmAddress
        SET postalzip='45056'
        WHERE provincestate = 'Ohio, USA 45056-0070';

        UPDATE crmAddress
        SET postalzip='74169'
        WHERE provincestate = 'OK 74169-0360';

        UPDATE crmAddress
        SET postalzip='T5K1H5'
        WHERE provincestate = 'T5K 1H5';

        UPDATE crmAddress
        SET postalzip='T6H5K8'
        WHERE provincestate = 'T6H 5K8';

        UPDATE crmAddress
        SET postalzip='T6P1N5'
        WHERE provincestate = 'T6P 1N5';

        UPDATE crmAddress
        SET postalzip='98003'
        WHERE provincestate = 'Washington 98003';

        UPDATE crmAddress
        SET postalzip='53150'
        WHERE provincestate = 'WI, 53150';

        UPDATE crmAddress
        SET postalzip='53150'
        WHERE provincestate = 'WI, USA 53150';

        UPDATE crmAddress
        SET ProvinceState=NULL
        WHERE provincestate = '07675';

        UPDATE crmAddress
        SET ProvinceState=NULL
        WHERE provincestate = '73034';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Alberta T5J 3G2';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Alberta T6E 3P4';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Alberta, T5J 2V4';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Allberta';

        UPDATE crmAddress
        SET ProvinceState='BC'
        WHERE provincestate = 'B.C.';

        UPDATE crmAddress
        SET ProvinceState='CA'
        WHERE provincestate = 'CA 90066';

        UPDATE crmAddress
        SET ProvinceState='CA'
        WHERE provincestate = 'CA 91423, USA';

        UPDATE crmAddress
        SET ProvinceState='CA'
        WHERE provincestate = 'CA 94550';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Calgary';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Edmonton';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Edmonton';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'Edmonton';

        UPDATE crmAddress
        SET ProvinceState=NULL
        WHERE provincestate = 'Ext 6794';

        UPDATE crmAddress
        SET ProvinceState='MA'
        WHERE provincestate = 'MA 01742';

        UPDATE crmAddress
        SET ProvinceState='MA'
        WHERE provincestate = 'MA 02114-0034';

        UPDATE crmAddress
        SET ProvinceState='MO'
        WHERE provincestate = 'MO 63146';

        UPDATE crmAddress
        SET ProvinceState='NB'
        WHERE provincestate = 'N.B.';

        UPDATE crmAddress
        SET ProvinceState='NF'
        WHERE provincestate = 'New Foundland';

        UPDATE crmAddress
        SET ProvinceState='OH'
        WHERE provincestate = 'Ohio, USA 45056-0070';

        UPDATE crmAddress
        SET ProvinceState='OK'
        WHERE provincestate = 'OK 74169-0360';

        UPDATE crmAddress
        SET ProvinceState='PQ'
        WHERE provincestate = 'QUE';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'T5K 1H5';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'T6H 5K8';

        UPDATE crmAddress
        SET ProvinceState='AB'
        WHERE provincestate = 'T6P 1N5';

        UPDATE crmAddress
        SET ProvinceState='WA'
        WHERE provincestate = 'Washington 98003';

        UPDATE crmAddress
        SET ProvinceState='WI'
        WHERE provincestate = 'WI, 53150';

        UPDATE crmAddress
        SET ProvinceState='WI'
        WHERE provincestate = 'WI, USA 53150';

        UPDATE crmAddress a
        SET comCityID=b.id
        FROM (
                 SELECT bb.id,
                        bb.name City,
                        cc.Name ProvinceState
                 FROM comCity bb
                 JOIN comProvinceState cc
                      ON cc.id = bb.comProvinceStateID
                 JOIN comCountry dd
                      ON dd.id = cc.comCountryID) b
        WHERE b.City = a.City
          AND b.provincestate = a.ProvinceState;

        UPDATE actActivity a
        SET RowIDPerformedFor=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM Activities a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Contacts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'Activities') b
        WHERE b.id = a.id;

        UPDATE comAttributeDetail a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM Attributelinkstoclient a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Contacts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'Attributelinkstoclient'
                 WHERE a.dbid = 'CL') b
        WHERE b.id = a.id;
        UPDATE comAttributeDetail a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM Attributelinkstoclient a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Relationships'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'Attributelinkstoclient'
                 WHERE a.dbid = 'RL') b
        WHERE b.id = a.id;
        UPDATE comAttributeDetail a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM Attributelinkstoclient a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'ActivityAllocations'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'Attributelinkstoclient'
                 WHERE a.dbid = 'AA') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Employees'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'IR') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'FixedAssets'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'FA') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Relationships'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'RL') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Contacts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'CL') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'GLEntries'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'PS') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Batches'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'BA') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'FixedAssetLocation'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'FL') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'ContactSetup'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'FS') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'ActivityProjects'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'AP') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Transactions'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'TX') b
        WHERE b.id = a.id;
        UPDATE sysChangeHistory a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM AuditLogs a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Activities'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'AuditLogs'
                 WHERE a.dbid = 'AC') b
        WHERE b.id = a.id;
        UPDATE syschangehistory s SET isexported= TRUE;
        UPDATE glBillingAccountBalance a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM BillingAccountBalances a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'BillingAccounts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'BillingAccountBalances'
                 WHERE a.dbid = 'BL') b
        WHERE b.id = a.id;
        UPDATE glBillingAccountBalance a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM BillingAccountBalances a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Contacts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'BillingAccountBalances'
                 WHERE a.dbid = 'CL') b
        WHERE b.id = a.id;
        UPDATE glBillingAccountBalance a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM BillingAccountBalances a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'ActivityProjects'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'BillingAccountBalances'
                 WHERE a.dbid = 'AP') b
        WHERE b.id = a.id;
        UPDATE glBillingAccountBalance a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM BillingAccountBalances a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'FixedAssets'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'BillingAccountBalances'
                 WHERE a.dbid = 'FA') b
        WHERE b.id = a.id;
        UPDATE comCrossReference a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM CrossReference a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Contacts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'CrossReference'
                 WHERE a.dbid = 'CL') b
        WHERE b.id = a.id;
        UPDATE faLocationHistory a
        SET RowIDLocation=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM FixedAssetLocationHistory a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Fixed Asset Disposal Code'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'FixedAssetLocationHistory'
                 WHERE a.dbid = 'SI') b
        WHERE b.id = a.id;
        UPDATE faLocationHistory a
        SET RowIDLocation=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM FixedAssetLocationHistory a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'FixedAssets'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'FixedAssetLocationHistory'
                 WHERE a.dbid = 'FA') b
        WHERE b.id = a.id;
        UPDATE faLocationHistory a
        SET RowIDLocation=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM FixedAssetLocationHistory a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'FixedAssetLocation'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'FixedAssetLocationHistory'
                 WHERE a.dbid = 'LO') b
        WHERE b.id = a.id;
        DELETE FROM falocationhistory f WHERE f.rowidlocation IS NULL OR f.sysdictionarytableidlocation IS NULL;
        UPDATE glEntry a
        SET RowIDChargedTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM GLEntries a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'BillingAccounts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'GLEntries'
                 WHERE a.dbid = 'BL') b
        WHERE b.id = a.id;
        UPDATE glEntry a
        SET RowIDChargedTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM GLEntries a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'FixedAssets'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'GLEntries'
                 WHERE a.dbid = 'FA') b
        WHERE b.id = a.id;
        UPDATE glEntry a
        SET RowIDChargedTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM GLEntries a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'ActivityProjects'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'GLEntries'
                 WHERE a.dbid = 'AP') b
        WHERE b.id = a.id;
        UPDATE glEntry a
        SET RowIDChargedTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM GLEntries a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Contacts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'GLEntries'
                 WHERE a.dbid = 'CL') b
        WHERE b.id = a.id;

        UPDATE glentry g
        SET sysdictionarytableidchargedto=NULL
        WHERE g.sysdictionarytableidchargedto IS NOT NULL
          AND g.rowidchargedto IS NULL;

        UPDATE glentry g
        SET rowidchargedto=NULL
        WHERE g.sysdictionarytableidchargedto IS NULL
          AND g.rowidchargedto IS NOT NULL;

        UPDATE comPersonalComment a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM PersonalComments a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Activities'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'PersonalComments'
                 WHERE a.dbid = 'AC') b
        WHERE b.id = a.id;
        UPDATE comPersonalComment a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM PersonalComments a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'GLEntries'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'PersonalComments'
                 WHERE a.dbid = 'PS') b
        WHERE b.id = a.id;
        UPDATE comPersonalComment a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM PersonalComments a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Relationships'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'PersonalComments'
                 WHERE a.dbid = 'RL') b
        WHERE b.id = a.id;
        UPDATE comPersonalComment a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM PersonalComments a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Attributelinkstoclient'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'PersonalComments'
                 WHERE a.dbid = 'AL') b
        WHERE b.id = a.id;
        UPDATE comPersonalComment a
        SET RowIDAppliesTo=b.dbcodeid
        FROM (
                 SELECT c.newid + 10000000000000 ID, b.newid + 10000000000000 dbcodeid
                 FROM PersonalComments a
                 LEFT JOIN keytranslation b
                           ON b.oldid = a.dbcodeid::VARCHAR AND b.tablename = 'Contacts'
                 JOIN      keytranslation c
                           ON c.oldid = a.id::VARCHAR AND c.tablename = 'PersonalComments'
                 WHERE a.dbid = 'CL') b
        WHERE b.id = a.id;

        SELECT id
        INTO l_crmContactidOrganization
        FROM crmcontact
        WHERE name ILIKE 'm1%'
           OR name ILIKE 'method1%'
        LIMIT 1;

        IF NOT EXISTS(SELECT FROM crmRelationshipType)
        THEN
            INSERT INTO crmRelationshipType(id, PrimaryName, SecondaryName, RowStatus)
            SELECT newid, a.Description, a.Description, 'a'
            FROM ReferenceFields a
            JOIN KeyTranslation b
                 ON b.oldid = a.id::VARCHAR
            WHERE a.id IN (3390, 3692, 6446, 6450, 6451, 7519, 63490, 6560)
              AND NOT EXISTS(SELECT 1 FROM crmRelationshipType c WHERE c.id = b.newid LIMIT 1);
        END IF;


        -- I commented the following code on Oct 18th, 2021 because I didnt want to add relationships
        -- for every contact for M1
--         IF NOT EXISTS(SELECT 1 FROM crmContact WHERE ID = 1000)
--         THEN
--             INSERT INTO crmRelationshipType(id, PrimaryName, SecondaryName, RowStatus)
--             SELECT newid, a.Description, a.Description, 'a'
--             FROM ReferenceFields a
--             JOIN KeyTranslation b
--                  ON b.oldid = a.id::VARCHAR
--             WHERE a.id IN (3390, 3692, 6446, 6450, 6451, 7519, 63490, 6560)
--               AND NOT EXISTS(SELECT 1 FROM crmRelationshipType c WHERE c.id = b.newid LIMIT 1);
--
--             INSERT INTO crmContact(ID, Name, RowStatus)
--             SELECT 1000, 'Method1 Enterprise Software Inc.', 'a';
--
--             select distinct tablename from keytranslation k where tablename ilike 'reference%'
--             INSERT INTO crmRelationship(
--                 TemporalStartDate, TemporalEndDate, crmContactID1, crmContactID2, crmRelationshipTypeID, RowStatus)
--             SELECT '1000-01-01', '9999-12-31', c.NewID, 1000, COALESCE(d.NewID, e.id), 'a'
--             --  select *
--             FROM contacts a
--             JOIN      KeyTranslation C
--                       ON C.oldid = a.id::VARCHAR and lower(c.tablename)='contacts'
--             LEFT JOIN KeyTranslation D
--                       ON D.oldid = a.reftypeid::VARCHAR and lower(c.tablename)='referencefields'
--             JOIN      LATERAL (SELECT id
--                                FROM crmRelationshipType
--                                WHERE PrimaryName = 'Organization'
--                                LIMIT 1 ) e
--                       ON TRUE
--             UNION ALL
--             SELECT DATE '1000-01-01', DATE '9999-12-31', 1000, c.NewID, COALESCE(d.NewID, e.id), 'a'
--             FROM contacts a
--             JOIN      KeyTranslation C
--                       ON C.oldid = a.id::VARCHAR and lower(c.tablename)='contacts'
--             LEFT JOIN KeyTranslation D
--                       ON D.oldid = a.reftypeid::VARCHAR and lower(c.tablename)='referencefields'
--             JOIN      LATERAL (SELECT id
--                                FROM crmRelationshipType
--                                WHERE PrimaryName = 'Organization'
--                                LIMIT 1 ) e
--                       ON TRUE;
--         END IF;

        DELETE
        FROM crmRelationship a USING crmRelationshipType b
        WHERE b.id = a.crmRelationshipTypeID
          AND b.PrimaryName LIKE 'x%';

        DELETE
        FROM crmRelationshipType
        WHERE PrimaryName LIKE 'x%';

        IF NOT EXISTS(SELECT 1 FROM sysMultilinkTableRule LIMIT 1)
        THEN
            TRUNCATE TABLE sysMultilinkTableRule;
            INSERT INTO sysMultilinkTableRule(
                id,
                sysdictionarycolumniddest,
                sysdictionarycolumnidsource,
                sysdictionarytableid,
                description,
                seqno,
                whereclause,
                rowstatus,
                syschangehistoryid)
            SELECT id,
                   sysdictionarycolumniddest,
                   sysdictionarycolumnidsource,
                   sysdictionarytableid,
                   description,
                   seqno,
                   whereclause,
                   'a',
                   0
            FROM aaMultilinkTableRule a;
        END IF;

        UPDATE actactivitysubtypebilling a
        SET actactivityid=id;

        UPDATE gltransactionsubtypeinvoice
        SET gltransactionid=id;

        UPDATE gltransactionsubtypecheque
        SET gltransactionid=id;

        UPDATE crmcontactsubtypeemployee c2
        SET crmcontactid=id;

        UPDATE crmcontactsubtypeuser c2
        SET crmcontactid=id;

        UPDATE actactivitysubtypebilling a
        SET gltransactionid=NULL
        WHERE gltransactionid IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM gltransaction aa WHERE aa.id = a.gltransactionid);

        DELETE
        FROM actActivity a
        WHERE actPriorityID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actpriority aa WHERE aa.id = a.actpriorityid);

        DELETE
        FROM actActivity a
        WHERE actTypeID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actType aa WHERE aa.id = a.actTypeID);

        DELETE
        FROM actactivitysubtypebilling
        WHERE id IN (
                        SELECT a2.id
                        FROM actactivitysubtypebilling a2
                        LEFT JOIN actactivity a3
                                  ON a2.id = a3.id
                        WHERE a3.id IS NULL)
           OR (gltransactionid IS NULL);

        UPDATE actActivity a
        SET actProjectID=NULL
        WHERE actProjectID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actProject aa WHERE aa.id = a.actProjectID);

        UPDATE actProjectResourceAllocation a
        SET actTypeID=NULL
        WHERE actTypeID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actType aa WHERE aa.id = a.actTypeID);

        DELETE
        FROM actRateBilling a
        WHERE actProjectID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actProject aa WHERE aa.id = a.actProjectID);

        DELETE
        FROM actRateBilling a
        WHERE actTypeID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actType aa WHERE aa.id = a.actTypeID);

        DELETE
        FROM actRateExpense a
        WHERE actTypeID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actType aa WHERE aa.id = a.actTypeID);

        UPDATE actType a
        SET actTypeIDParent=NULL
        WHERE actTypeIDParent IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM actType aa WHERE aa.id = a.actTypeIDParent);

        UPDATE acttype a
        SET sysmultilinktableruleidperformedfor=b.id
        FROM (
                 SELECT a.id, b.newid + 10000000000000 NewID
                 FROM aamultilinktablerule a
                 JOIN keytranslation b
                      ON b.oldid = a.refid::VARCHAR AND b.tablename = 'ReferenceFields') b
        WHERE b.newid = a.sysmultilinktableruleidperformedfor;

        UPDATE actType a
        SET sysMultilinkTableRuleIDPerformedBy=1130;

        -- delete rate associated with Travel Expenses (US)
        DELETE FROM actratebilling WHERE acttypeid = 1000000158184;

        DELETE
            -- select *
        FROM acttype
        WHERE id IN (
                        SELECT id
                        FROM acttype aa
                        WHERE aa.rowstatus != 'a'
                          AND NOT EXISTS(SELECT FROM actactivity aaa WHERE aaa.acttypeid = aa.id)
                          AND NOT EXISTS(SELECT FROM actratebilling aaa WHERE aaa.acttypeid = aa.id)
                          AND NOT EXISTS(SELECT FROM actrateexpense aaa WHERE aaa.acttypeid = aa.id)
                          AND NOT EXISTS(SELECT FROM acttype aaa WHERE aaa.acttypeidparent = aa.id));

        UPDATE comAttribute a
        SET sysMultilinkTableRuleID=b.id
        FROM (
                 SELECT a.id, b.newid + 10000000000000 NewID
                 FROM aamultilinktablerule a
                 JOIN keytranslation b
                      ON b.oldid = a.refid::VARCHAR AND b.tablename = 'ReferenceFields') b
        WHERE b.newid = a.sysMultilinkTableRuleID;

        DELETE
        FROM crmRelationship a
        WHERE crmContactID1 IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM crmContact aa WHERE aa.id = a.crmContactID1);

        DELETE
        FROM crmRelationship a
        WHERE crmContactID2 IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM crmContact aa WHERE aa.id = a.crmContactID2);

        DELETE
        FROM crmRelationship a
        WHERE crmRelationshipTypeID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM crmRelationshipType aa WHERE aa.id = a.crmRelationshipTypeID);

        UPDATE faType a
        SET faTypeIDParent=NULL
        WHERE faTypeIDParent IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM faType aa WHERE aa.id = a.faTypeIDParent);

        DELETE
        FROM fatype
        WHERE id IN (
                        SELECT id
                        FROM fatype aa
                        WHERE aa.rowstatus != 'a'
                          AND NOT EXISTS(SELECT FROM fafixedasset aaa WHERE aaa.fatypeid = aa.id)
                          AND NOT EXISTS(SELECT FROM fatype aaa WHERE aaa.fatypeidparent = aa.id));

        UPDATE glAccount a
        SET sysMultilinkTableRuleID=b.id
        FROM (
                 SELECT a.id, b.newid
                 FROM aamultilinktablerule a
                 JOIN keytranslation b
                      ON b.oldid = a.refid::VARCHAR AND b.tablename = 'ReferenceFields') b
        WHERE b.newid = a.sysMultilinkTableRuleID;

        UPDATE glBatch a
        SET glAccountID=NULL
        WHERE glAccountID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM glAccount aa WHERE aa.id = a.glAccountID);

        DELETE
        FROM glEntry a
        WHERE glTransactionID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM glTransaction aa WHERE aa.id = a.glTransactionID);

        DELETE
        FROM glReconciliation a
        WHERE glEntryIDTo IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM glEntry aa WHERE aa.id = a.glEntryIDTo);

        DELETE
        FROM hrPosition a
        WHERE comLocationID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM comLocation aa WHERE aa.id = a.comLocationID);

        DELETE
        FROM hrPosition a
        WHERE glCostCentreID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM glCostCentre aa WHERE aa.id = a.glCostCentreID);

        UPDATE glTransaction a
        SET glEntryIDMain=NULL
        WHERE glEntryIDMain IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM glEntry aa WHERE aa.id = a.glEntryIDMain);

        DELETE
        FROM glTransactionSubTypeCheque a
        WHERE glBillingAccountID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM glBillingAccount aa WHERE aa.id = a.glBillingAccountID);

        DELETE
        FROM glTransactionSubTypeInvoice a
        WHERE glBillingAccountID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM glBillingAccount aa WHERE aa.id = a.glBillingAccountID);

        UPDATE glBillingAccount a
        SET crmContactID=(
                             SELECT id
                             FROM crmcontact
                             WHERE name LIKE '%method1%'
                             LIMIT 1)
        WHERE crmContactID IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM crmContact aa WHERE aa.id = a.crmContactID);

        -- Convert reconciliations

        UPDATE actactivity SET sysdictionarytableidperformedby=100;

        TRUNCATE TABLE glReconciliation;

        UPDATE glEntries a
        SET glEntryID = NULL
        WHERE glEntryID IS NOT NULL
          AND glEntryID IN
              (
                  SELECT glEntryID
                  FROM glEntries
                  WHERE glEntryID IS NOT NULL
                  GROUP BY glEntryID
                  HAVING SUM(amount) <> 0);

        UPDATE glEntry
        SET reconciliationbalance = NULL
        WHERE reconciliationbalance IS NOT NULL;

        DROP TABLE IF EXISTS t_PreviouslyReconciledPostings;
        CREATE TEMP TABLE t_PreviouslyReconciledPostings
        (
            glEntryIDFrom BIGINT,
            glEntryIDTo   BIGINT,
            amount        DECIMAL(19, 2)
        );

        INSERT INTO t_PreviouslyReconciledPostings (glEntryIDFrom, glEntryIDTo, amount)
        SELECT b.newid + 10000000000000 glEntryIDFrom,
               c.newid + 10000000000000 glEntryIDTo,
               Amount
        FROM glentries A
        JOIN keytranslation b
             ON b.oldid = a.id::VARCHAR AND b.tablename = 'GLEntries'
        JOIN keytranslation c
             ON c.oldid = a.glentryid::VARCHAR AND c.tablename = 'GLEntries'
        WHERE glEntryID IS NOT NULL
          AND amount <> 0;
--truncate table glreconciliation;

        INSERT INTO glReconciliation (
            glentryidfrom, glentryidto, amount, syschangehistoryid)
        SELECT glEntryIDFrom,
               glEntryIDTo,
               Amount,
               -1
        FROM t_PreviouslyReconciledPostings
        WHERE glEntryIDFrom <> glEntryIDTo
        UNION ALL
        SELECT glEntryIDTo,
               glEntryIDFrom,
               Amount * -1,
               -2
        FROM t_PreviouslyReconciledPostings
        WHERE glEntryIDFrom <> glEntryIDTo;

        UPDATE glentry a
        SET ReconciliationBalance = 0
        FROM (
                 SELECT glEntryIDFrom,
                        SUM(amount) ReconciliationBalance
                 FROM glReconciliation
                 GROUP BY glEntryIDFrom) b
        WHERE b.glEntryIDFrom = a.id;

        DROP TABLE IF EXISTS aasysMessage;
        CREATE TEMP TABLE aasysMessage
        (
            ID                 INT,
            Description        VARCHAR,
            Detail             VARCHAR,
            Hint               VARCHAR,
            IsMessageLogged    BOOLEAN,
            IsExceptionRaised  BOOLEAN,
            State              NUMERIC(10),
            RoutineName        VARCHAR,
            sysChangeHistoryID NUMERIC(10)
        );

        COPY aasysMessage FROM 'C:\temp\bmsdata\aasysMessage.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        TRUNCATE TABLE sysmessage;
        INSERT INTO sysmessage (
            id, description, detail, hint, ismessagelogged, isexceptionraised, state, routinename, syschangehistoryid)

        SELECT ID,
               description,
               detail,
               hint,
               ismessagelogged,
               isexceptionraised,
               state,
               routinename,
               syschangehistoryid
        FROM aasysMessage;

        UPDATE glbatch SET approvaldate=createdate WHERE createdate < DATE '2020-12-31' AND approvaldate IS NULL;

        UPDATE glbatch
        SET glbatchstatusid= CASE WHEN approvaldate IS NULL THEN 1
                                  ELSE 2
                                  END;

        UPDATE gltransaction a
        SET glbatchid=(
                          SELECT MIN(aa.id)
                          FROM glbatch aa)
        WHERE a.glbatchid IS NULL;

        UPDATE gltransaction a
        SET glpostingstatusid=b.glbatchstatusid
        FROM glbatch b
        WHERE b.id = a.glbatchid;

        UPDATE gltransaction a
        SET description=COALESCE(b.description, '')
        FROM glentry b
        WHERE b.gltransactionid = a.id
          AND a.description IS NULL;

        TRUNCATE TABLE glaccounttype;
        INSERT INTO glaccounttype (id, description, isbalancesheettype, normalizationfactor, rowstatus)
        SELECT id, description, isbalancesheet, normalizationfactor, 'a'
        FROM aaglaccounttype;

        /*
        20210606  This is here in case we add a new GLAccount since the aaGLAccount hierarchy was created.  If
        so I would have to add the glaccount to aaGLAccount and fit it into the hieararchy.
*/

        IF iscolumnfound('aaglaccount', 'stageid') = FALSE
        THEN
            IF EXISTS(SELECT *
                      FROM glaccount a
                      JOIN      keytranslation b
                                ON b.newid = a.id
                      LEFT JOIN aaglaccount c
                                ON c.oldid = b.oldid::INT
                      WHERE c.id IS NULL)
            THEN
                RAISE EXCEPTION 'A new glaccount has been added since the default glaccounts were defined';
            END IF;
            ALTER TABLE aaglaccount
                ADD stageid INT;
        END IF;

        UPDATE aaglaccount a
        SET stageid=b.stageid
        FROM (
                 SELECT b.id NewID, a.NewId + 10000000000000 StageID
                 FROM keytranslation a
                 LEFT JOIN aaglaccount b
                           ON a.oldid::INT = b.oldid::INT
                 WHERE LOWER(tablename) = 'glaccounts') b
        WHERE b.NewID = a.id;

        TRUNCATE TABLE glaccount;

        INSERT INTO glaccount (
            id,
            glaccountidparent,
            glaccounttypeid,
            sysmultilinktableruleid,
            bankaccountnumber,
            bottomuplevel,
            comments,
            description,
            displaysequence,
            iscollapseonexportrequired,
            isusedtoclassifyrecords,
            quickcode,
            referencenumber,
            topdownlevel,
            rowstatus,
            syschangehistoryid)
        SELECT id,
               glaccountidparent,
               glaccounttypeid,
               sysmultilinktableruleid,
               bankaccountnumber,
               bottomuplevel,
               comments,
               description,
               displaysequence,
               iscollapseonexportrequired,
               isusedtoclassifyrecords,
               quickcode,
               referencenumber,
               topdownlevel,
               rowstatus,
               syschangehistoryid
        FROM aaglaccount;

        UPDATE glentry a
        SET glaccountid=b.id, glcostcentreid=0
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountid;

        UPDATE glbillingaccount a
        SET glaccountid=b.id
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountid;

        UPDATE glbatch a
        SET glaccountid=b.id
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountid;

        UPDATE glaccountcostcentre a
        SET glaccountid=b.id, glcostcentreid=0
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountid;

        UPDATE fatype a
        SET glaccountidaccummulateddepreciation=b.id
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountidaccummulateddepreciation;

        UPDATE fatype a
        SET glaccountidasset=b.id
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountidasset;

        UPDATE fatype a
        SET glaccountiddepreciationexpense=b.id
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountiddepreciationexpense;

        UPDATE acttype a
        SET glaccountid=b.id
        FROM aaglaccount b
        WHERE b.stageid = a.glaccountid;

        UPDATE actProject SET glcostcentreid=0;
        UPDATE glAccountBalance SET glcostcentreid=0;
        UPDATE glBudget SET glcostcentreid=0;
        UPDATE glEntry SET glcostcentreid=0;
        UPDATE hrPosition SET glcostcentreid=0;

        UPDATE glBillingAccount SET glbillingaccountstatusid=20;
        DELETE
        FROM gltransactionsubtypecheque
        WHERE glbillingaccountid IN (
                                        SELECT id
                                        FROM glbillingaccount
                                        WHERE crmcontactid IS NULL
                                           OR glaccountid IS NULL);
        DELETE FROM glBillingAccount WHERE crmcontactid IS NULL OR glaccountid IS NULL;

        IF NOT EXISTS(SELECT * FROM glcostcentre WHERE id = 0)
        THEN
            INSERT INTO glcostcentre (
                id,
                glcostcentreidparent,
                bottomuplevel,
                comments,
                description,
                displaysequence,
                isusedtoclassifyrecords,
                referencenumber,
                topdownlevel,
                rowstatus,
                syschangehistoryid)
            SELECT 0,
                   NULL,
                   1,
                   NULL,
                   'Organization',
                   1,
                   TRUE isusedtoclassifyrecords,
                   NULL referencenumber,
                   1    topdownlevel,
                   'a'  rowstatus,
                   NULL syschangehistoryid;
        END IF;

        DELETE FROM glcostcentre WHERE id != 0;

        UPDATE glsetup
        SET gstrate=0.05,
            fiscalyearincrement=12,
            crmcontactidcompany=NULL,
            glaccountidpayable=NULL,
            glaccountidreceivable=NULL,
            glaccountidgst=NULL,
            glaccountidcash=NULL,
            rowstatus='a',
            accountlabel='Account',
            fiscalstartdate=NULL,
            fiscalenddate=NULL,
            costcentrelabel='Department',
            gstnumber=NULL,
            iscostcentreused= FALSE,
            isforcebalanceused= TRUE;

        CALL spglAccountBalanceUpdate(p_rollupall := TRUE);

        CALL spIDConversion('actBillingMethod', 'description');
        CALL spIDConversion('actBillingStatus', 'Description');
        CALL spIDConversion('actCostUnit', 'Description');
        CALL spIDConversion('actPriority', 'Description');
        CALL spIDConversion('actSource', 'Description');
        CALL spIDConversion('actStatus', 'Description');
        CALL spIDConversion('comCity', 'comprovincestateid, name');
        CALL spIDConversion('comcountry', 'name');
        CALL spIDConversion('comCrossReferenceType', 'Description');
        CALL spIDConversion('comLocation', 'description', 1);

        UPDATE falocationhistory a
        SET rowidlocation=b.newid
        FROM aaIDConversion b
        WHERE b.id = a.rowidlocation
          AND a.sysdictionarytableidlocation = 207;

        CALL spIDConversion('comprovincestate', 'name');
        CALL spIDConversion('crmAddressPhoneType', 'Description');
        CALL spIDConversion('crmAddressType', 'Description');
        CALL spIDConversion('crmGender', 'Description');
        CALL spIDConversion('crmLanguage', 'Description');
        CALL spIDConversion('crmRelationshipType', 'ID');
        CALL spIDConversion('crmSalutation', 'Description');
        CALL spIDConversion('faDepreciationMethod', 'Description');
        CALL spIDConversion('faDisposalReason', 'Description', 1);

        UPDATE falocationhistory a
        SET rowidlocation=b.newid
        FROM aaIDConversion b
        WHERE b.id = a.rowidlocation
          AND a.sysdictionarytableidlocation = 503;

        CALL spIDConversion('faStatus', 'Description');
        CALL spIDConversion('glBillingAccountStatus', 'description');
        CALL spIDConversion('glbatchtype', 'description');
        CALL spIDConversion('glsetup', 'id', 1);
        CALL spIDConversion('hrEmploymentStatus', 'Description');
        CALL spIDConversion('hrGrade', 'Description');
        CALL spIDConversion('hrPositionClassification', 'Description');
        CALL spIDConversion('hrPositionType', 'Description');
        PERFORM fnsysIDSetValAll('s0001v0000', 1);

        -- If batchtype is activity billing then set to 3 else set to manual (1)
        UPDATE glbatch a
        SET glbatchtypeid=CASE WHEN glbatchtypeid = 30 THEN 3
                               ELSE 1
                               END;

        TRUNCATE TABLE glbatchtype;

        INSERT INTO glbatchtype (id, description, rowstatus)
        SELECT id, description, 'a'
        FROM aareferencefields
        WHERE type ILIKE 'batch type';

        SELECT id INTO l_crmContactidSystem FROM crmcontact WHERE LOWER(name) = 'system admin';

        IF l_crmContactidSystem IS NULL
        THEN
            INSERT INTO crmcontact (firstname, lastname, name, rowstatus, syschangehistoryid)
            VALUES ('Systemx', 'Adminx', 'System Admin', 'a', NULL)
            RETURNING id INTO l_crmContactidSystem;
        END IF;

        TRUNCATE TABLE gltransactionsubtypecashreceipt;

        INSERT INTO gltransactionsubtypecashreceipt (
            id,
            gltransactionid,
            crmcontactidenteredby,
            crmcontactidpaidfor,
            glpaymentmethodid,
            address,
            amount,
            careof,
            invoicenumber,
            name,
            referencenumber)
        SELECT a.id,
               a.id,
               l_crmContactidSystem                                                        crmcontactidenteredby,
               d.id                                                                        crmcontactidpaidfor,
               CASE WHEN LOWER(d.name) LIKE '%cpa%' THEN 9
                    ELSE 2
                    END                                                                    glpaymentmethodid,
               e.address1 || COALESCE(', ' || e.city, '') || COALESCE(', ' || e.postalzip) address,
               f.amount,
               d.contactperson                                                             careof,
               b.referencenumber                                                           invoicenumber,
               d.name,
               a.referencenumber                                                           referencenumber
        FROM gltransaction AS a
        JOIN      LATERAL (SELECT *
                           FROM glentry aa
                           WHERE aa.gltransactionid = a.id
                             AND aa.glaccountid = 1400
                           LIMIT 1 ) b
                  ON TRUE
        JOIN      glbillingaccount c
                  ON c.id = b.Rowidchargedto
        JOIN      crmcontact d
                  ON d.id = c.crmcontactid
        LEFT JOIN LATERAL (SELECT *
                           FROM crmaddress aa
                           WHERE aa.crmContactID = d.id
                           ORDER BY aa.crmaddresstypeid
                           LIMIT 1 ) e
                  ON TRUE
        JOIN      LATERAL (SELECT SUM(amount) amount
                           FROM glentry aa
                           WHERE aa.gltransactionid = a.id
                             AND aa.glaccountid = 1300) f
                  ON TRUE
        WHERE EXISTS(SELECT * FROM glentry AS aa WHERE aa.gltransactionid = a.id AND aa.glaccountid = 1400)
          AND EXISTS(SELECT * FROM glentry AS aa WHERE aa.gltransactionid = a.id AND aa.glaccountid = 1300)
          AND f.amount > 0;

        UPDATE gltransaction a
        SET gltransactiontypeid=2
        WHERE EXISTS(SELECT * FROM gltransactionsubtypecashreceipt aa WHERE aa.id = a.id);

        UPDATE gltransaction a
        SET gltransactiontypeid=3
        WHERE EXISTS(SELECT * FROM gltransactionsubtypeinvoice aa WHERE aa.id = a.id);

        UPDATE gltransaction a
        SET gltransactiontypeid=4
        WHERE EXISTS(SELECT * FROM gltransactionsubtypecheque aa WHERE aa.id = a.id);

        UPDATE gltransaction a
        SET gltransactiontypeid=1
        WHERE gltransactiontypeid IS NULL;

        UPDATE glentry a
        SET glentrytypeid=4
        WHERE a.glaccountid = 1400
          AND EXISTS(SELECT * FROM gltransactionsubtypecashreceipt aa WHERE aa.id = a.gltransactionid);

        UPDATE glentry a
        SET glentrytypeid=1
        WHERE a.glaccountid = 1300
          AND EXISTS(SELECT * FROM gltransactionsubtypecashreceipt aa WHERE aa.id = a.gltransactionid);

        UPDATE gltransaction a
        SET glentryidmain=b.id
        FROM glentry AS b
        WHERE b.gltransactionid = a.id
          AND a.gltransactiontypeid = 2
          AND b.glaccountid = 1300;

        DELETE
        FROM gltransaction a
        WHERE NOT EXISTS(SELECT 1 FROM glentry aa WHERE aa.gltransactionid = a.id)
          AND NOT EXISTS(SELECT 1 FROM actactivitysubtypebilling aa WHERE aa.gltransactionid = a.id);

        DELETE
        FROM crmcontactsubtypeemployee
        WHERE id IN (
                        SELECT a.id
                        FROM crmcontactsubtypeemployee a
                        LEFT JOIN crmcontact c
                                  ON a.crmcontactid = c.id
                        WHERE c.id IS NULL);


        DELETE
        FROM crmcontactsubtypeuser
        WHERE id IN (
                        SELECT a.id
                        FROM crmcontactsubtypeuser a
                        LEFT JOIN crmcontact c
                                  ON a.crmcontactid = c.id
                        WHERE c.id IS NULL);

        DELETE
        FROM gltransactionsubtypecheque
        WHERE id IN (
                        SELECT a.id
                        FROM gltransactionsubtypecheque a
                        LEFT JOIN gltransaction c
                                  ON a.gltransactionid = c.id
                        WHERE c.id IS NULL);

        DELETE
        FROM gltransactionsubtypeinvoice
        WHERE id IN (
                        SELECT a.id
                        FROM gltransactionsubtypeinvoice a
                        LEFT JOIN gltransaction c
                                  ON a.gltransactionid = c.id
                        WHERE c.id IS NULL);

        CALL spsysForeignKeyCacheRefresh(p_debug := FALSE);

        CALL spsysChangeHistoryRefreshTriggers();

        CALL spsysForeignKeyConstraintGenerate();

        CALL spsysNotNullConstraintGenerate();

        CALL spsysUniqueConstraintGenerate();

        SET SEARCH_PATH TO DEFAULT;

    END ;

$$ LANGUAGE plpgsql;
