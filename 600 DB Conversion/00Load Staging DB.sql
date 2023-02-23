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
This script loads data from csv files into the staging database
*/
DO
$$
    BEGIN

        DROP SCHEMA IF EXISTS _Staging CASCADE;
        CREATE SCHEMA _Staging;

        PERFORM set_config('search_path', '_Staging,'||current_setting('search_path'), true);

        DROP TABLE IF EXISTS accountbalances;
        CREATE TABLE accountbalances
        (
            id            INT,
            glaccountid   INT,
            costcentreid  INT,
            date          TIMESTAMP,
            balance       NUMERIC(15, 2),
            budgetbalance NUMERIC(15, 2),
            commitbal     NUMERIC(15, 2),
            rollupamount  NUMERIC(15, 2),
            budgetrollup  NUMERIC(15, 2),
            recordstatus  VARCHAR
        );

        DROP TABLE IF EXISTS activities;
        CREATE TABLE activities
        (
            id                      INT,
            completiondate          TIMESTAMP,
            description             VARCHAR,
            contactidperformedby    INT,
            revenueamount           NUMERIC(10, 2),
            activitytypeid          INT,
            dbcodeid                INT,
            refbillingstatusid      INT,
            totalactual             NUMERIC(10, 2),
            dbid                    VARCHAR,
            employeeid              INT,
            transactionid           INT,
            createdate              TIMESTAMP,
            overridehours           VARCHAR,
            contactidinvoicethrough INT,
            kms                     NUMERIC(8, 2),
            override                VARCHAR,
            refsubclassifybyid      INT,
            refpriortyid            INT,
            refstatusid             INT,
            startdate               TIMESTAMP,
            subcnt                  INT,
            updatedate              TIMESTAMP,
            activityprojectid       INT,
            billedamount            NUMERIC(10, 2),
            notes                   VARCHAR,
            recordstatus            VARCHAR
        );

        DROP TABLE IF EXISTS activityallocations;
        CREATE TABLE activityallocations
        (
            id                INT,
            contactid         INT,
            activityprojectid INT,
            activitytypeid    INT,
            hours             NUMERIC(10, 2),
            dollars           NUMERIC(12, 2),
            recordstatus      VARCHAR
        );

        DROP TABLE IF EXISTS activityprojects;
        CREATE TABLE activityprojects
        (
            id                 INT,
            contactid          INT,
            refstatusid        INT,
            completiondate     TIMESTAMP,
            startdate          TIMESTAMP,
            description        VARCHAR,
            employeeid         INT,
            notes              VARCHAR,
            baseamount         NUMERIC(10, 2),
            acronym            VARCHAR,
            refbillingmethodid INT,
            contact            VARCHAR,
            expenseamount      NUMERIC(10, 2),
            refcontracteeid    INT,
            refsourceid        INT,
            activityprojectid  INT,
            referencenumber    VARCHAR,
            costcentreid       INT,
            recordstatus       VARCHAR
        );

        DROP TABLE IF EXISTS activitytypes;
        CREATE TABLE activitytypes
        (
            id                 INT,
            seqno              INT,
            description        VARCHAR,
            activitytypeid     INT,
            reftypeid          INT,
            narrative          VARCHAR,
            refsubclassifybyid INT,
            template           VARCHAR,
            subcnt             INT,
            refcostunitid      INT,
            glaccountid        INT,
            treestring         VARCHAR,
            treeseq            INT,
            recordstatus       VARCHAR
        );

        DROP TABLE IF EXISTS attributelinkstoclient;
        CREATE TABLE attributelinkstoclient
        (
            id           INT,
            dbid         VARCHAR,
            dbcodeid     INT,
            attributeid  INT,
            notes        VARCHAR,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS attributes;
        CREATE TABLE attributes
        (
            id           INT,
            attributeid  INT,
            description  VARCHAR,
            seqno        INT,
            extradata    VARCHAR,
            reftypeid    INT,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS auditlogs;
        CREATE TABLE auditlogs
        (
            id           INT,
            commandid    INT,
            dbid         VARCHAR,
            dbcodeid     INT,
            employeeid   INT,
            logdate      TIMESTAMP,
            logtime      VARCHAR,
            lognote      VARCHAR,
            comments     VARCHAR,
            xlog         VARCHAR,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS bmssource;
        CREATE TABLE bmssource
        (
            id         BIGINT NOT NULL,
            source     VARCHAR,
            program    VARCHAR,
            tablename  VARCHAR,
            action     VARCHAR,
            columnname VARCHAR,
            dfcode     INT
        );

        DROP TABLE IF EXISTS batchtypes;
        CREATE TABLE batchtypes
        (
            id            INT,
            description   VARCHAR,
            shortcode     VARCHAR,
            genblk        VARCHAR,
            startdate     TIMESTAMP,
            incrementdate VARCHAR,
            comments      VARCHAR,
            recordstatus  VARCHAR
        );

        DROP TABLE IF EXISTS batches;
        CREATE TABLE batches
        (
            id           INT,
            batchtypeid  INT,
            glaccountid  INT,
            approvaldate TIMESTAMP,
            createdate   TIMESTAMP,
            batcherror   VARCHAR,
            approvalyear VARCHAR,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS billingaccountbalances;
        CREATE TABLE billingaccountbalances
        (
            id           INT,
            dbid         VARCHAR,
            dbcodeid     INT,
            glaccountid  INT,
            balance      NUMERIC(15, 2),
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS billingaccounts;
        CREATE TABLE billingaccounts
        (
            id                 INT,
            contactid          INT,
            glaccountid        INT,
            name               VARCHAR,
            dbid               VARCHAR,
            dbcodeid           INT,
            refstatusid        INT,
            rectype            VARCHAR,
            glaccountiddefault INT,
            gstexempt          VARCHAR,
            payterms           INT,
            ledger             VARCHAR,
            recordstatus       VARCHAR
        );

        DROP TABLE IF EXISTS budgets;
        CREATE TABLE budgets
        (
            id           INT,
            glaccountid  INT,
            costcentreid INT,
            date         TIMESTAMP,
            rollupamount NUMERIC(15, 2),
            amount       NUMERIC(15, 2),
            notes        VARCHAR,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS cm;
        CREATE TABLE cm
        (
            cmcode    BIGINT,
            cmdesc    VARCHAR,
            prcname   VARCHAR,
            menuprmpt VARCHAR,
            menumsg   VARCHAR,
            menutype  VARCHAR,
            logexpr   VARCHAR,
            helptext  VARCHAR,
            updcnt    VARCHAR,
            logcm     VARCHAR,
            displayid VARCHAR,
            logdb     VARCHAR,
            sysid     VARCHAR,
            logcond   VARCHAR,
            logtype   VARCHAR
        );

        DROP TABLE IF EXISTS cx;
        CREATE TABLE cx
        (
            cxcode     BIGINT,
            screen     VARCHAR,
            field      VARCHAR,
            type       VARCHAR,
            row        BIGINT,
            col        BIGINT,
            label      VARCHAR,
            fldcnt     BIGINT,
            initial    VARCHAR,
            skip       VARCHAR,
            tablename  VARCHAR,
            tablekey   VARCHAR,
            tabledesc  VARCHAR,
            tableorder VARCHAR,
            maxrows    BIGINT,
            fieldlst   VARCHAR,
            scope      VARCHAR,
            scrnclaus  VARCHAR,
            sdid       VARCHAR,
            width      BIGINT,
            picture    VARCHAR,
            sikey      VARCHAR
        );

        DROP TABLE IF EXISTS commandjoins;
        CREATE TABLE commandjoins
        (
            id              INT,
            commandidparent INT,
            commandidchild  INT,
            seqno           INT,
            condition       VARCHAR,
            checkjoin       VARCHAR,
            treestring      VARCHAR,
            treeseq         INT,
            recordstatus    VARCHAR
        );

        DROP TABLE IF EXISTS commands;
        CREATE TABLE commands
        (
            id            INT,
            description   VARCHAR,
            procedurename VARCHAR,
            menuprmpt     VARCHAR,
            menumsg       VARCHAR,
            menutype      VARCHAR,
            logexpression VARCHAR,
            helptext      VARCHAR,
            logcommand    VARCHAR,
            displayid     VARCHAR,
            logdb         VARCHAR,
            sysid         VARCHAR,
            logcondition  VARCHAR,
            logtype       VARCHAR,
            recordstatus  VARCHAR
        );

        DROP TABLE IF EXISTS contactsetup;
        CREATE TABLE contactsetup
        (
            id                    INT,
            accountlabel          VARCHAR,
            costcentrelabel       VARCHAR,
            fiscalyearstart       TIMESTAMP,
            fiscalyearincrement   INT,
            fiscalyearend         TIMESTAMP,
            gstrate               NUMERIC(7, 3),
            forcebalancing        VARCHAR,
            chequenumber          INT,
            contactid             INT,
            deprdate              TIMESTAMP,
            deprinc               VARCHAR,
            refdepreciationtypeid INT,
            t0refer               VARCHAR,
            t2refer               VARCHAR,
            referencenumber       VARCHAR,
            wipbilling            VARCHAR,
            usecc                 VARCHAR,
            hoursperday           NUMERIC(5, 2),
            gstnum                VARCHAR,
            recordstatus          VARCHAR
        );

        DROP TABLE IF EXISTS contacts;
        CREATE TABLE contacts
        (
            id               INT,
            name             VARCHAR,
            phone1           VARCHAR,
            phone2           VARCHAR,
            address1         VARCHAR,
            address2         VARCHAR,
            city             VARCHAR,
            province         VARCHAR,
            mailingaddress1  VARCHAR,
            mailingaddress2  VARCHAR,
            mailingcity      VARCHAR,
            maillingprovince VARCHAR,
            postal           VARCHAR,
            reftypeid        INT,
            comments         VARCHAR,
            rectype          VARCHAR,
            contactid        INT,
            contact          VARCHAR,
            createdate       TIMESTAMP,
            date             INT,
            number           VARCHAR,
            hoursperday      NUMERIC(5, 2),
            billingmethod    VARCHAR,
            refindustryid    INT,
            reflocationid    INT,
            email            VARCHAR,
            kms              INT,
            recordstatus     VARCHAR
        );

        DROP TABLE IF EXISTS costcentres;
        CREATE TABLE costcentres
        (
            id              INT,
            seqno           INT,
            description     VARCHAR,
            costcentreid    INT,
            narrative       VARCHAR,
            referencenumber VARCHAR,
            classify        VARCHAR,
            treestring      VARCHAR,
            treeseq         INT,
            recordstatus    VARCHAR
        );

        DROP TABLE IF EXISTS crossreference;
        CREATE TABLE crossreference
        (
            id           INT,
            dbid         VARCHAR,
            dbcodeid     INT,
            number       VARCHAR,
            reftypeid    INT,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS db;
        CREATE TABLE db
        (
            dbcode      BIGINT,
            dbid        VARCHAR,
            description VARCHAR,
            drive       VARCHAR,
            path        VARCHAR,
            updcnt      VARCHAR,
            userpath    VARCHAR,
            sysid       BIGINT,
            dbcm        VARCHAR,
            dlookup     VARCHAR
        );

        DROP TABLE IF EXISTS df;
        CREATE TABLE df
        (
            dfcode    BIGINT,
            dfdb      BIGINT,
            dfsqno    BIGINT,
            fieldname VARCHAR,
            fieldtype VARCHAR,
            fieldlen  BIGINT,
            fielddec  BIGINT,
            descript  VARCHAR,
            updcnt    BIGINT,
            purpose   VARCHAR,
            hidden    VARCHAR,
            parentdb  VARCHAR,
            parentntx VARCHAR,
            autolink  VARCHAR,
            virtual   VARCHAR
        );

        DROP TABLE IF EXISTS dictionaryfields;
        CREATE TABLE dictionaryfields
        (
            id                  INT,
            dictionarytableid   INT,
            field               VARCHAR,
            label               VARCHAR,
            datatype            VARCHAR,
            datalength          INT,
            decimals            INT,
            isnullable          VARCHAR,
            sequence            INT,
            purpose             VARCHAR,
            foreigntableid      INT,
            detaileddescription VARCHAR,
            recordstatus        VARCHAR,
            updatecount         INT
        );

        DROP TABLE IF EXISTS dictionarytables;
        CREATE TABLE dictionarytables
        (
            id           INT,
            name         VARCHAR,
            shortcode    VARCHAR,
            singularname VARCHAR,
            translation  VARCHAR,
            purpose      VARCHAR,
            commandid    INT,
            recordstatus VARCHAR,
            updatecount  INT
        );

        DROP TABLE IF EXISTS employees;
        CREATE TABLE employees
        (
            id              INT,
            firstname       VARCHAR,
            middlename      VARCHAR,
            lastname        VARCHAR,
            reftypeid       INT,
            extension       VARCHAR,
            startdate       TIMESTAMP,
            birthdate       TIMESTAMP,
            sex             VARCHAR,
            signon          VARCHAR,
            download        VARCHAR,
            comments        VARCHAR,
            refemplstatusid INT,
            refuserpathid   INT,
            costcentreid    INT,
            termdate        TIMESTAMP,
            recordstatus    VARCHAR
        );

        DROP TABLE IF EXISTS fixedassetlocation;
        CREATE TABLE fixedassetlocation
        (
            id                   INT,
            seqno                INT,
            description          VARCHAR,
            val2                 VARCHAR,
            comments             VARCHAR,
            fixedassetlocationid INT,
            classify             VARCHAR,
            loc_id               VARCHAR,
            recordstatus         VARCHAR
        );

        DROP TABLE IF EXISTS fixedassetlocationhistory;
        CREATE TABLE fixedassetlocationhistory
        (
            id            INT,
            fixedassetid  INT,
            dbid          VARCHAR,
            dbcodeid      INT,
            effectivedate TIMESTAMP,
            confirm       VARCHAR,
            commandid     INT,
            active        VARCHAR,
            details       VARCHAR,
            contactid     INT,
            recordstatus  VARCHAR
        );

        DROP TABLE IF EXISTS fixedassettype;
        CREATE TABLE fixedassettype
        (
            id                             INT,
            seqno                          INT,
            description                    VARCHAR,
            fixedassettypeid               INT,
            template                       VARCHAR,
            fixedassetnumberused           VARCHAR,
            classify                       VARCHAR,
            deprlife                       VARCHAR,
            depreciationpercent            INT,
            glaccountidaccumdepr           INT,
            glaccountiddepreciationexpense INT,
            glaccountidasset               INT,
            subtype                        INT,
            replacementamount              NUMERIC(10, 2),
            treestring                     VARCHAR,
            treeseq                        INT,
            recordstatus                   VARCHAR
        );

        DROP TABLE IF EXISTS fixedassets;
        CREATE TABLE fixedassets
        (
            id                     INT,
            number                 VARCHAR,
            description            VARCHAR,
            fixedassettypeid       INT,
            serial                 VARCHAR,
            modelnum               VARCHAR,
            notes                  VARCHAR,
            refstatusid            INT,
            refownerid             INT,
            deprlife               VARCHAR,
            depriationsalvagevalue NUMERIC(10, 2),
            costcentreid           INT,
            purdate                TIMESTAMP,
            contactid              INT,
            recordstatus           VARCHAR
        );

        DROP TABLE IF EXISTS glaccounts;
        CREATE TABLE glaccounts
        (
            id                 INT,
            seqno              INT,
            description        VARCHAR,
            glaccountid        INT,
            narrative          VARCHAR,
            refsubclassifybyid INT,
            referencenumber    VARCHAR,
            classify           VARCHAR,
            reftypeid          INT,
            treestring         VARCHAR,
            treeseq            INT,
            recordstatus       VARCHAR
        );

        DROP TABLE IF EXISTS glaccountscostcentres;
        CREATE TABLE glaccountscostcentres
        (
            glaccountid  INT,
            costcentreid INT
        );

        DROP TABLE IF EXISTS glentries;
        CREATE TABLE glentries
        (
            id                 INT,
            glaccountid        INT,
            dbid               VARCHAR,
            dbcodeid           INT,
            refstatusid        INT,
            postingdate        TIMESTAMP,
            amount             NUMERIC(15, 2),
            description        VARCHAR,
            costcentreid       INT,
            rollupamount       NUMERIC(15, 2),
            transactionid      INT,
            reconciliationdate TIMESTAMP,
            referencenumber    VARCHAR,
            seqno              VARCHAR,
            glentryid          INT,
            comments           VARCHAR,
            type               VARCHAR,
            source             VARCHAR,
            recordstatus       VARCHAR
        );

        DROP TABLE IF EXISTS organizationalpositions;
        CREATE TABLE organizationalpositions
        (
            id                       INT,
            number                   VARCHAR,
            seqno                    INT,
            location                 VARCHAR,
            organizationunit         VARCHAR,
            wtitle                   VARCHAR,
            refclassificationid      INT,
            reftypeid                INT,
            organizationalpositionid INT,
            narrative                VARCHAR,
            recordstatus             VARCHAR
        );

        DROP TABLE IF EXISTS peopledatabase;
        CREATE TABLE peopledatabase
        (
            id           INT,
            firstname    VARCHAR,
            middlename   VARCHAR,
            lastname     VARCHAR,
            birthdate    TIMESTAMP,
            sex          VARCHAR,
            sinnum       VARCHAR,
            givenname    VARCHAR,
            wtitle       VARCHAR,
            extension    VARCHAR,
            salutation   VARCHAR,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS personalcomments;
        CREATE TABLE personalcomments
        (
            id           INT,
            dbid         VARCHAR,
            dbcodeid     INT,
            employeeid   INT,
            comments     VARCHAR,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS positionhistory;
        CREATE TABLE positionhistory
        (
            id                     INT,
            employeeid             INT,
            organizationpositionid INT,
            commencementdate       TIMESTAMP,
            expirydate             TIMESTAMP,
            effectivedate          TIMESTAMP,
            salary                 NUMERIC(9, 2),
            grade                  VARCHAR,
            histstatus             VARCHAR,
            transtype              VARCHAR,
            comments               VARCHAR,
            recordstatus           VARCHAR
        );

        DROP TABLE IF EXISTS ratesbilling;
        CREATE TABLE ratesbilling
        (
            id                INT,
            contactidby       INT,
            activitytypeid    INT,
            rate              NUMERIC(10, 3),
            activityprojectid INT,
            recordstatus      VARCHAR
        );

        DROP TABLE IF EXISTS ratesexpense;
        CREATE TABLE ratesexpense
        (
            id                INT,
            contactidby       INT,
            activitytypeid    INT,
            rate              NUMERIC(10, 2),
            activityprojectid INT,
            date              TIMESTAMP,
            recordstatus      VARCHAR
        );

        DROP TABLE IF EXISTS ratesfactor;
        CREATE TABLE ratesfactor
        (
            id           INT,
            date         TIMESTAMP,
            adjustment   NUMERIC(6, 3),
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS referencefields;
        CREATE TABLE referencefields
        (
            id               INT,
            referencetableid INT,
            seqno            INT,
            shortcode        VARCHAR,
            description      VARCHAR,
            val2             VARCHAR,
            comments         VARCHAR,
            referencefieldid INT,
            classify         VARCHAR,
            recordstatus     VARCHAR
        );

        DROP TABLE IF EXISTS referencetables;
        CREATE TABLE referencetables
        (
            id           INT,
            reftypeid    VARCHAR,
            shortcode    VARCHAR,
            val1label    VARCHAR,
            val2label    VARCHAR,
            length       VARCHAR,
            description  VARCHAR,
            sysid        VARCHAR,
            recordstatus VARCHAR
        );

        DROP TABLE IF EXISTS relationships;
        CREATE TABLE relationships
        (
            id             INT,
            contactid1     INT,
            contactid2     INT,
            reftypeid      INT,
            comments       VARCHAR,
            relationshipid INT,
            recordstatus   VARCHAR
        );

        DROP TABLE IF EXISTS salaries;
        CREATE TABLE salaries
        (
            resource VARCHAR,
            salary   BIGINT,
            year     BIGINT,
            totalhrs NUMERIC(10, 2)
        );

        DROP TABLE IF EXISTS transactions;
        CREATE TABLE transactions
        (
            id              INT,
            batchid         INT,
            parentid        INT,
            rectype         VARCHAR,
            description     VARCHAR,
            date            TIMESTAMP,
            refstatusid     INT,
            createdate      TIMESTAMP,
            referencenumber VARCHAR,
            glentryidmain   INT,
            recordstatus    VARCHAR
        );

        DROP TABLE IF EXISTS transactionscheques;
        CREATE TABLE transactionscheques
        (
            transactionid    INT,
            type             VARCHAR,
            name             VARCHAR,
            address1         VARCHAR,
            address2         VARCHAR,
            city             VARCHAR,
            province         VARCHAR,
            postal           VARCHAR,
            printcnt         INT,
            billingaccountid INT,
            recordstatus     VARCHAR
        );

        DROP TABLE IF EXISTS transactionsinvoices;
        CREATE TABLE transactionsinvoices
        (
            id                INT,
            activityprojectid INT,
            billingaccountid  INT,
            type              VARCHAR,
            gstover           VARCHAR,
            recordstatus      VARCHAR
        );

        DROP TABLE IF EXISTS aaReferenceFields;
        CREATE TABLE aaReferenceFields
        (
            id                 INT,
            type               VARCHAR,
            description        VARCHAR,
            sequence           INT,
            recordstatus       VARCHAR,
            shortcode          VARCHAR,
            sysChangeHistoryID INT,
            Source             VARCHAR,
            oldid              INT
        );

        COPY aaReferenceFields FROM 'c:\temp\bmsdata\aaReferenceFIelds.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        DROP TABLE IF EXISTS aaProvince;
        CREATE TABLE aaProvince
        (
            ID        NUMERIC(10),
            ShortCode VARCHAR,
            Countryid NUMERIC(10),
            Name      VARCHAR
        );

        COPY aaProvince FROM 'c:\temp\bmsdata\aaProvince.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        DROP TABLE IF EXISTS aaCountry;
        CREATE TABLE aaCountry
        (
            id        NUMERIC(10),
            name      VARCHAR,
            shortcode VARCHAR
        );

        COPY aaCountry FROM 'c:\temp\bmsdata\aaCountry.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        DROP TABLE IF EXISTS aaCity;
        CREATE TABLE aaCity
        (
            ID         NUMERIC(10),
            CountryID  NUMERIC(10),
            ProvinceID NUMERIC(10),
            Name       VARCHAR
        );

        COPY aaCity FROM 'c:\temp\bmsdata\aaCity.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        COPY CX FROM 'c:\temp\bmsdata\CX.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY CM FROM 'c:\temp\bmsdata\CM.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY DB FROM 'c:\temp\bmsdata\DB.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY DF FROM 'c:\temp\bmsdata\DF.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY AccountBalances FROM 'c:\temp\bmsdata\AS.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Activities FROM 'c:\temp\bmsdata\AC.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY ActivityAllocations FROM 'c:\temp\bmsdata\AA.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY ActivityProjects FROM 'c:\temp\bmsdata\AP.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY ActivityTypes FROM 'c:\temp\bmsdata\FM.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Attributelinkstoclient FROM 'c:\temp\bmsdata\AL.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Attributes FROM 'c:\temp\bmsdata\AB.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY AuditLogs FROM 'c:\temp\bmsdata\CH.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Batches FROM 'c:\temp\bmsdata\BA.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY BatchTypes FROM 'c:\temp\bmsdata\BT.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY BillingAccountBalances FROM 'c:\temp\bmsdata\BC.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY billingaccounts FROM 'c:\temp\bmsdata\BL.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Commands FROM 'c:\temp\bmsdata\CM.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY CommandJoins FROM 'c:\temp\bmsdata\CJ.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Contacts FROM 'c:\temp\bmsdata\CL.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY ContactSetup FROM 'c:\temp\bmsdata\FS.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY CostCentres FROM 'c:\temp\bmsdata\CC.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY CrossReference FROM 'c:\temp\bmsdata\CR.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Employees FROM 'c:\temp\bmsdata\IR.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY FixedAssetLocation FROM 'c:\temp\bmsdata\LO.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY FixedAssetLocationHistory FROM 'c:\temp\bmsdata\FL.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY FixedAssets FROM 'c:\temp\bmsdata\FA.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY FixedAssetType FROM 'c:\temp\bmsdata\TF.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY GLAccounts FROM 'c:\temp\bmsdata\CA.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY GLAccountsCostCentres FROM 'c:\temp\bmsdata\CB.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY GLEntries FROM 'c:\temp\bmsdata\PS.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY OrganizationalPositions FROM 'c:\temp\bmsdata\OP.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY PeopleDatabase FROM 'c:\temp\bmsdata\PE.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY PersonalComments FROM 'c:\temp\bmsdata\PC.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY RatesBilling FROM 'c:\temp\bmsdata\RT.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY RatesExpense FROM 'c:\temp\bmsdata\RX.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY RatesFactor FROM 'c:\temp\bmsdata\RY.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY ReferenceFields FROM 'c:\temp\bmsdata\SI.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY ReferenceTables FROM 'c:\temp\bmsdata\SD.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Relationships FROM 'c:\temp\bmsdata\RL.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY Transactions FROM 'c:\temp\bmsdata\TX.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY TransactionsCheques FROM 'c:\temp\bmsdata\T4.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';
        COPY TransactionsInvoices FROM 'c:\temp\bmsdata\T8.CSV' DELIMITER ',' CSV ENCODING 'windows-1251';

        ALTER TABLE cx
            ADD ScreenCode VARCHAR;
        ALTER TABLE cx
            ADD ValidationCode VARCHAR;

        UPDATE FixedAssetLocationHistory SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE FixedAssetLocationHistory SET FixedAssetID = NULL WHERE FixedAssetID IN (0, 94313);
        UPDATE FixedAssetLocationHistory SET dbid = REPLACE(dbid, '^', ',');
        UPDATE FixedAssetLocationHistory SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE FixedAssetLocationHistory SET Confirm = REPLACE(Confirm, '^', ',');
        UPDATE FixedAssetLocationHistory SET CommandID = NULL WHERE CommandID IN (0, 94313);
        UPDATE FixedAssetLocationHistory SET Active = REPLACE(Active, '^', ',');
        UPDATE FixedAssetLocationHistory SET Details = REPLACE(Details, '^', ',');
        UPDATE FixedAssetLocationHistory SET ContactID = NULL WHERE ContactID IN (0, 94313);
        UPDATE FixedAssetLocationHistory SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE DictionaryTables SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE DictionaryTables SET Name = REPLACE(Name, '^', ',');
        UPDATE DictionaryTables SET ShortCode = REPLACE(ShortCode, '^', ',');
        UPDATE DictionaryTables SET Purpose = REPLACE(Purpose, '^', ',');
        UPDATE DictionaryTables SET CommandID = NULL WHERE CommandID IN (0, 94313);
        UPDATE DictionaryTables SET CommandID = NULL WHERE CommandID IN (0, 94313);
        UPDATE DictionaryTables SET SingularName = REPLACE(SingularName, '^', ',');
        UPDATE DictionaryTables SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE DictionaryTables SET Translation = REPLACE(Translation, '^', ',');
        UPDATE DictionaryTables SET UpdateCount = NULL WHERE UpdateCount IN (0, 94313);
        UPDATE ReferenceFields SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE ReferenceFields SET ReferenceTableID = NULL WHERE ReferenceTableID IN (0, 94313);
        UPDATE ReferenceFields SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE ReferenceFields SET ShortCode = REPLACE(ShortCode, '^', ',');
        UPDATE ReferenceFields SET Description = REPLACE(Description, '^', ',');
        UPDATE ReferenceFields SET Val2 = REPLACE(Val2, '^', ',');
        UPDATE ReferenceFields SET Comments = REPLACE(Comments, '^', ',');
        UPDATE ReferenceFields SET ReferenceFieldID = NULL WHERE ReferenceFieldID IN (0, 94313);
        UPDATE ReferenceFields SET Classify = REPLACE(Classify, '^', ',');
        UPDATE ReferenceFields SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE ReferenceTables SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE ReferenceTables SET RefTypeID = REPLACE(RefTypeID, '^', ',');
        UPDATE ReferenceTables SET ShortCode = REPLACE(ShortCode, '^', ',');
        UPDATE ReferenceTables SET Val1Label = REPLACE(Val1Label, '^', ',');
        UPDATE ReferenceTables SET Val2Label = REPLACE(Val2Label, '^', ',');
        UPDATE ReferenceTables SET Length = REPLACE(Length, '^', ',');
        UPDATE ReferenceTables SET Description = REPLACE(Description, '^', ',');
        UPDATE ReferenceTables SET SysId = REPLACE(SysId, '^', ',');
        UPDATE ReferenceTables SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Contacts SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Contacts SET Name = REPLACE(Name, '^', ',') WHERE name LIKE '%^%';
        UPDATE Contacts SET Phone1 = REPLACE(Phone1, '^', ',');
        UPDATE Contacts SET Phone2 = REPLACE(Phone2, '^', ',');
        UPDATE Contacts SET Address1 = REPLACE(Address1, '^', ',');
        UPDATE Contacts SET Address2 = REPLACE(Address2, '^', ',');
        UPDATE Contacts SET City = REPLACE(City, '^', ',');
        UPDATE Contacts SET Province = REPLACE(Province, '^', ',');
        UPDATE Contacts SET MailingAddress1 = REPLACE(MailingAddress1, '^', ',');
        UPDATE Contacts SET MailingAddress2 = REPLACE(MailingAddress2, '^', ',');
        UPDATE Contacts SET MailingCity = REPLACE(MailingCity, '^', ',');
        UPDATE Contacts SET MaillingProvince = REPLACE(MaillingProvince, '^', ',');
        UPDATE Contacts SET Postal = REPLACE(Postal, '^', ',');
        UPDATE Contacts SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE Contacts SET Comments = REPLACE(Comments, '^', ',');
        UPDATE Contacts SET RecType = REPLACE(RecType, '^', ',');
        UPDATE Contacts SET ContactID = NULL WHERE ContactID IN (0, 94313);
        UPDATE Contacts SET Contact = REPLACE(Contact, '^', ',');
        UPDATE Contacts SET Date = NULL WHERE Date IN (0, 94313);
        UPDATE Contacts SET Number = REPLACE(Number, '^', ',');
        UPDATE Contacts SET BillingMethod = REPLACE(BillingMethod, '^', ',');
        UPDATE Contacts SET RefIndustryID = NULL WHERE RefIndustryID IN (0, 94313);
        UPDATE Contacts SET RefLocationID = NULL WHERE RefLocationID IN (0, 94313);
        UPDATE Contacts SET EMail = REPLACE(EMail, '^', ',');
        UPDATE Contacts SET Kms = NULL WHERE Kms IN (0, 94313);
        UPDATE Contacts SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE PeopleDatabase SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE PeopleDatabase SET FirstName = REPLACE(FirstName, '^', ',');
        UPDATE PeopleDatabase SET MiddleName = REPLACE(MiddleName, '^', ',');
        UPDATE PeopleDatabase SET LastName = REPLACE(LastName, '^', ',');
        UPDATE PeopleDatabase SET Sex = REPLACE(Sex, '^', ',');
        UPDATE PeopleDatabase SET SinNum = REPLACE(SinNum, '^', ',');
        UPDATE PeopleDatabase SET GivenName = REPLACE(GivenName, '^', ',');
        UPDATE PeopleDatabase SET WTitle = REPLACE(WTitle, '^', ',');
        UPDATE PeopleDatabase SET Extension = REPLACE(Extension, '^', ',');
        UPDATE PeopleDatabase SET Salutation = REPLACE(Salutation, '^', ',');
        UPDATE PeopleDatabase SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Relationships SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Relationships SET ContactID1 = NULL WHERE ContactID1 IN (0, 94313);
        UPDATE Relationships SET ContactID2 = NULL WHERE ContactID2 IN (0, 94313);
        UPDATE Relationships SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE Relationships SET Comments = REPLACE(Comments, '^', ',');
        UPDATE Relationships SET RelationshipID = NULL WHERE RelationshipID IN (0, 94313);
        UPDATE Relationships SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE ActivityTypes SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE ActivityTypes SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE ActivityTypes SET Description = REPLACE(Description, '^', ',');
        UPDATE ActivityTypes SET ActivityTypeID = NULL WHERE ActivityTypeID IN (0, 94313);
        UPDATE ActivityTypes SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE ActivityTypes SET Narrative = REPLACE(Narrative, '^', ',');
        UPDATE ActivityTypes SET RefSubClassifyByID = NULL WHERE RefSubClassifyByID IN (0, 94313);
        UPDATE ActivityTypes SET Template = REPLACE(Template, '^', ',');
        UPDATE ActivityTypes SET SubCnt = NULL WHERE SubCnt IN (0, 94313);
        UPDATE ActivityTypes SET RefCostUnitID = NULL WHERE RefCostUnitID IN (0, 94313);
        UPDATE ActivityTypes SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE ActivityTypes SET TreeString = REPLACE(TreeString, '^', ',');
        UPDATE ActivityTypes SET TreeSeq = NULL WHERE TreeSeq IN (0, 94313);
        UPDATE ActivityTypes SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Activities SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Activities SET Description = REPLACE(Description, '^', ',');
        UPDATE Activities SET ContactIDPerformedBY = NULL WHERE ContactIDPerformedBY IN (0, 94313);
        UPDATE Activities SET ActivityTypeID = NULL WHERE ActivityTypeID IN (0, 94313);
        UPDATE Activities SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE Activities SET RefBillingStatusID = NULL WHERE RefBillingStatusID IN (0, 94313);
        UPDATE Activities SET dbid = REPLACE(dbid, '^', ',');
        UPDATE Activities SET EmployeeID = NULL WHERE EmployeeID IN (0, 94313);
        UPDATE Activities SET TransactionID = NULL WHERE TransactionID IN (0, 94313);
        UPDATE Activities SET OverrideHours = REPLACE(OverrideHours, '^', ',');
        UPDATE Activities SET ContactIDInvoiceThrough = NULL WHERE ContactIDInvoiceThrough IN (0, 94313);
        UPDATE Activities SET Override = REPLACE(Override, '^', ',');
        UPDATE Activities SET RefSubClassifyByID = NULL WHERE RefSubClassifyByID IN (0, 94313);
        UPDATE Activities SET RefPriortyID = NULL WHERE RefPriortyID IN (0, 94313);
        UPDATE Activities SET RefStatusID = NULL WHERE RefStatusID IN (0, 94313);
        UPDATE Activities SET SubCnt = NULL WHERE SubCnt IN (0, 94313);
        UPDATE Activities SET ActivityProjectID = NULL WHERE ActivityProjectID IN (0, 94313);
        UPDATE Activities SET Notes = REPLACE(Notes, '^', ',');
        UPDATE Activities SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Employees SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Employees SET FirstName = REPLACE(FirstName, '^', ',');
        UPDATE Employees SET MiddleName = REPLACE(MiddleName, '^', ',');
        UPDATE Employees SET LastName = REPLACE(LastName, '^', ',');
        UPDATE Employees SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE Employees SET Extension = REPLACE(Extension, '^', ',');
        UPDATE Employees SET Sex = REPLACE(Sex, '^', ',');
        UPDATE Employees SET SignOn = REPLACE(SignOn, '^', ',');
        UPDATE Employees SET Download = REPLACE(Download, '^', ',');
        UPDATE Employees SET Comments = REPLACE(Comments, '^', ',');
        UPDATE Employees SET RefEmplStatusID = NULL WHERE RefEmplStatusID IN (0, 94313);
        UPDATE Employees SET RefUserPathID = NULL WHERE RefUserPathID IN (0, 94313);
        UPDATE Employees SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE Employees SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Commands SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Commands SET Description = REPLACE(Description, '^', ',');
        UPDATE Commands SET ProcedureName = REPLACE(ProcedureName, '^', ',');
        UPDATE Commands SET MenuPrmpt = REPLACE(MenuPrmpt, '^', ',');
        UPDATE Commands SET MenuMsg = REPLACE(MenuMsg, '^', ',');
        UPDATE Commands SET MenuType = REPLACE(MenuType, '^', ',');
        UPDATE Commands SET LogExpression = REPLACE(LogExpression, '^', ',');
        UPDATE Commands SET HelpText = REPLACE(HelpText, '^', ',');
        UPDATE Commands SET LogCommand = REPLACE(LogCommand, '^', ',');
        UPDATE Commands SET DisplayId = REPLACE(DisplayId, '^', ',');
        UPDATE Commands SET LogDb = REPLACE(LogDb, '^', ',');
        UPDATE Commands SET SysId = REPLACE(SysId, '^', ',');
        UPDATE Commands SET LogCondition = REPLACE(LogCondition, '^', ',');
        UPDATE Commands SET LogType = REPLACE(LogType, '^', ',');
        UPDATE Commands SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE CommandJoins SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE CommandJoins SET CommandIDParent = NULL WHERE CommandIDParent IN (0, 94313);
        UPDATE CommandJoins SET CommandIDChild = NULL WHERE CommandIDChild IN (0, 94313);
        UPDATE CommandJoins SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE CommandJoins SET Condition = REPLACE(Condition, '^', ',');
        UPDATE CommandJoins SET CheckJoin = REPLACE(CheckJoin, '^', ',');
        UPDATE CommandJoins SET TreeString = REPLACE(TreeString, '^', ',');
        UPDATE CommandJoins SET TreeSeq = NULL WHERE TreeSeq IN (0, 94313);
        UPDATE CommandJoins SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE FixedAssetType SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE FixedAssetType SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE FixedAssetType SET Description = REPLACE(Description, '^', ',');
        UPDATE FixedAssetType SET FixedAssetTypeID = NULL WHERE FixedAssetTypeID IN (0, 94313);
        UPDATE FixedAssetType SET Template = REPLACE(Template, '^', ',');
        UPDATE FixedAssetType SET FixedAssetNumberUsed = REPLACE(FixedAssetNumberUsed, '^', ',');
        UPDATE FixedAssetType SET Classify = REPLACE(Classify, '^', ',');
        UPDATE FixedAssetType SET DeprLife = REPLACE(DeprLife, '^', ',');
        UPDATE FixedAssetType SET DepreciationPercent = NULL WHERE DepreciationPercent IN (0, 94313);
        UPDATE FixedAssetType SET GLAccountIDAccumDepr = NULL WHERE GLAccountIDAccumDepr IN (0, 94313);
        UPDATE FixedAssetType
        SET GLAccountIDDepreciationExpense = NULL
        WHERE GLAccountIDDepreciationExpense IN (0, 94313);
        UPDATE FixedAssetType SET GLAccountIDAsset = NULL WHERE GLAccountIDAsset IN (0, 94313);
        UPDATE FixedAssetType SET SubType = NULL WHERE SubType IN (0, 94313);
        UPDATE FixedAssetType SET TreeString = REPLACE(TreeString, '^', ',');
        UPDATE FixedAssetType SET TreeSeq = NULL WHERE TreeSeq IN (0, 94313);
        UPDATE FixedAssetType SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE FixedAssets SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE FixedAssets SET Number = REPLACE(Number, '^', ',');
        UPDATE FixedAssets SET Description = REPLACE(Description, '^', ',');
        UPDATE FixedAssets SET FixedAssetTypeID = NULL WHERE FixedAssetTypeID IN (0, 94313);
        UPDATE FixedAssets SET Serial = REPLACE(Serial, '^', ',');
        UPDATE FixedAssets SET ModelNum = REPLACE(ModelNum, '^', ',');
        UPDATE FixedAssets SET Notes = REPLACE(Notes, '^', ',');
        UPDATE FixedAssets SET RefStatusID = NULL WHERE RefStatusID IN (0, 94313);
        UPDATE FixedAssets SET RefOwnerID = NULL WHERE RefOwnerID IN (0, 94313);
        UPDATE FixedAssets SET DeprLife = REPLACE(DeprLife, '^', ',');
        UPDATE FixedAssets SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE FixedAssets SET ContactID = NULL WHERE ContactID IN (0, 94313);
        UPDATE FixedAssets SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE OrganizationalPositions SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE OrganizationalPositions SET Number = REPLACE(Number, '^', ',');
        UPDATE OrganizationalPositions SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE OrganizationalPositions SET Location = REPLACE(Location, '^', ',');
        UPDATE OrganizationalPositions SET OrganizationUnit = REPLACE(OrganizationUnit, '^', ',');
        UPDATE OrganizationalPositions SET WTitle = REPLACE(WTitle, '^', ',');
        UPDATE OrganizationalPositions SET RefClassificationID = NULL WHERE RefClassificationID IN (0, 94313);
        UPDATE OrganizationalPositions SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE OrganizationalPositions SET OrganizationalPositionID = NULL WHERE OrganizationalPositionID IN (0, 94313);
        UPDATE OrganizationalPositions SET Narrative = REPLACE(Narrative, '^', ',');
        UPDATE OrganizationalPositions SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE FixedAssetLocation SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE FixedAssetLocation SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE FixedAssetLocation SET Description = REPLACE(Description, '^', ',');
        UPDATE FixedAssetLocation SET Val2 = REPLACE(Val2, '^', ',');
        UPDATE FixedAssetLocation SET Comments = REPLACE(Comments, '^', ',');
        UPDATE FixedAssetLocation SET FixedAssetLocationID = NULL WHERE FixedAssetLocationID IN (0, 94313);
        UPDATE FixedAssetLocation SET Classify = REPLACE(Classify, '^', ',');
        UPDATE FixedAssetLocation SET Loc_id = REPLACE(Loc_id, '^', ',');
        UPDATE FixedAssetLocation SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE AuditLogs SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE AuditLogs SET CommandID = NULL WHERE CommandID IN (0, 94313);
        UPDATE AuditLogs SET dbid = REPLACE(dbid, '^', ',');
        UPDATE AuditLogs SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE AuditLogs SET EmployeeID = NULL WHERE EmployeeID IN (0, 94313);
        UPDATE AuditLogs SET LogTime = REPLACE(LogTime, '^', ',');
        UPDATE AuditLogs SET LogNote = REPLACE(LogNote, '^', ',');
        UPDATE AuditLogs SET Comments = REPLACE(Comments, '^', ',');
        UPDATE AuditLogs SET Xlog = REPLACE(Xlog, '^', ',');
        UPDATE AuditLogs SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE PositionHistory SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE PositionHistory SET EmployeeID = NULL WHERE EmployeeID IN (0, 94313);
        UPDATE PositionHistory SET OrganizationPositionID = NULL WHERE OrganizationPositionID IN (0, 94313);
        UPDATE PositionHistory SET Grade = REPLACE(Grade, '^', ',');
        UPDATE PositionHistory SET HistStatus = REPLACE(HistStatus, '^', ',');
        UPDATE PositionHistory SET TransType = REPLACE(TransType, '^', ',');
        UPDATE PositionHistory SET Comments = REPLACE(Comments, '^', ',');
        UPDATE PositionHistory SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE PersonalComments SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE PersonalComments SET dbid = REPLACE(dbid, '^', ',');
        UPDATE PersonalComments SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE PersonalComments SET EmployeeID = NULL WHERE EmployeeID IN (0, 94313);
        UPDATE PersonalComments SET Comments = REPLACE(Comments, '^', ',');
        UPDATE PersonalComments SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE CrossReference SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE CrossReference SET dbid = REPLACE(dbid, '^', ',');
        UPDATE CrossReference SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE CrossReference SET Number = REPLACE(Number, '^', ',');
        UPDATE CrossReference SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE CrossReference SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE GLEntries SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE GLEntries SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE GLEntries SET dbid = REPLACE(dbid, '^', ',');
        UPDATE GLEntries SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE GLEntries SET RefStatusID = NULL WHERE RefStatusID IN (0, 94313);
        UPDATE GLEntries SET Description = REPLACE(Description, '^', ',');
        UPDATE GLEntries SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE GLEntries SET TransactionID = NULL WHERE TransactionID IN (0, 94313);
        UPDATE GLEntries SET ReferenceNumber = REPLACE(ReferenceNumber, '^', ',');
        UPDATE GLEntries SET SeqNo = REPLACE(SeqNo, '^', ',');
        UPDATE GLEntries SET GLEntryID = NULL WHERE GLEntryID IN (0, 94313);
        UPDATE GLEntries SET Comments = REPLACE(Comments, '^', ',');
        UPDATE GLEntries SET Type = REPLACE(Type, '^', ',');
        UPDATE GLEntries SET Source = REPLACE(Source, '^', ',');
        UPDATE GLEntries SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE GLAccounts SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE GLAccounts SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE GLAccounts SET Description = REPLACE(Description, '^', ',');
        UPDATE GLAccounts SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE GLAccounts SET Narrative = REPLACE(Narrative, '^', ',');
        UPDATE GLAccounts SET RefSubClassifyByID = NULL WHERE RefSubClassifyByID IN (0, 94313);
        UPDATE GLAccounts SET ReferenceNumber = REPLACE(ReferenceNumber, '^', ',');
        UPDATE GLAccounts SET Classify = REPLACE(Classify, '^', ',');
        UPDATE GLAccounts SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE GLAccounts SET TreeString = REPLACE(TreeString, '^', ',');
        UPDATE GLAccounts SET TreeSeq = NULL WHERE TreeSeq IN (0, 94313);
        UPDATE GLAccounts SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE BatchTypes SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE BatchTypes SET Description = REPLACE(Description, '^', ',');
        UPDATE BatchTypes SET Shortcode = REPLACE(Shortcode, '^', ',');
        UPDATE BatchTypes SET GenBlk = REPLACE(GenBlk, '^', ',');
        UPDATE BatchTypes SET IncrementDate = REPLACE(IncrementDate, '^', ',');
        UPDATE BatchTypes SET Comments = REPLACE(Comments, '^', ',');
        UPDATE BatchTypes SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Batches SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Batches SET BatchTypeID = NULL WHERE BatchTypeID IN (0, 94313);
        UPDATE Batches SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE Batches SET BatchError = REPLACE(BatchError, '^', ',');
        UPDATE Batches SET ApprovalYear = REPLACE(ApprovalYear, '^', ',');
        UPDATE Batches SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE AccountBalances SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE AccountBalances SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE AccountBalances SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE AccountBalances SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE BillingAccounts SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE BillingAccounts SET ContactID = NULL WHERE ContactID IN (0, 94313);
        UPDATE BillingAccounts SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE BillingAccounts SET Name = REPLACE(Name, '^', ',');
        UPDATE BillingAccounts SET dbid = REPLACE(dbid, '^', ',');
        UPDATE BillingAccounts SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE BillingAccounts SET RefStatusID = NULL WHERE RefStatusID IN (0, 94313);
        UPDATE BillingAccounts SET RecType = REPLACE(RecType, '^', ',');
        UPDATE BillingAccounts SET GLAccountIDDefault = NULL WHERE GLAccountIDDefault IN (0, 94313);
        UPDATE BillingAccounts SET GstExempt = REPLACE(GstExempt, '^', ',');
        UPDATE BillingAccounts SET PayTerms = NULL WHERE PayTerms IN (0, 94313);
        UPDATE BillingAccounts SET Ledger = REPLACE(Ledger, '^', ',');
        UPDATE BillingAccounts SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE RatesBilling SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE RatesBilling SET ContactIDBY = NULL WHERE ContactIDBY IN (0, 94313);
        UPDATE RatesBilling SET ActivityTypeID = NULL WHERE ActivityTypeID IN (0, 94313);
        UPDATE RatesBilling SET ActivityProjectID = NULL WHERE ActivityProjectID IN (0, 94313);
        UPDATE RatesBilling SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE TransactionsInvoices SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE TransactionsInvoices SET ActivityProjectID = NULL WHERE ActivityProjectID IN (0, 94313);
        UPDATE TransactionsInvoices SET BillingAccountID = NULL WHERE BillingAccountID IN (0, 94313);
        UPDATE TransactionsInvoices SET Type = REPLACE(Type, '^', ',');
        UPDATE TransactionsInvoices SET GstOver = REPLACE(GstOver, '^', ',');
        UPDATE TransactionsInvoices SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE ContactSetup SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE ContactSetup SET AccountLabel = REPLACE(AccountLabel, '^', ',');
        UPDATE ContactSetup SET CostCentreLabel = REPLACE(CostCentreLabel, '^', ',');
        UPDATE ContactSetup SET FiscalYearIncrement = NULL WHERE FiscalYearIncrement IN (0, 94313);
        UPDATE ContactSetup SET ForceBalancing = REPLACE(ForceBalancing, '^', ',');
        UPDATE ContactSetup SET ChequeNumber = NULL WHERE ChequeNumber IN (0, 94313);
        UPDATE ContactSetup SET ContactID = NULL WHERE ContactID IN (0, 94313);
        UPDATE ContactSetup SET DeprInc = REPLACE(DeprInc, '^', ',');
        UPDATE ContactSetup SET RefDepreciationTypeID = NULL WHERE RefDepreciationTypeID IN (0, 94313);
        UPDATE ContactSetup SET T0Refer = REPLACE(T0Refer, '^', ',');
        UPDATE ContactSetup SET T2Refer = REPLACE(T2Refer, '^', ',');
        UPDATE ContactSetup SET ReferenceNumber = REPLACE(ReferenceNumber, '^', ',');
        UPDATE ContactSetup SET WIPBilling = REPLACE(WIPBilling, '^', ',');
        UPDATE ContactSetup SET UseCc = REPLACE(UseCc, '^', ',');
        UPDATE ContactSetup SET Gstnum = REPLACE(Gstnum, '^', ',');
        UPDATE ContactSetup SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Budgets SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Budgets SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE Budgets SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE Budgets SET Notes = REPLACE(Notes, '^', ',');
        UPDATE Budgets SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Attributes SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Attributes SET AttributeID = NULL WHERE AttributeID IN (0, 94313);
        UPDATE Attributes SET Description = REPLACE(Description, '^', ',');
        UPDATE Attributes SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE Attributes SET ExtraData = REPLACE(ExtraData, '^', ',');
        UPDATE Attributes SET RefTypeID = NULL WHERE RefTypeID IN (0, 94313);
        UPDATE Attributes SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Attributelinkstoclient SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Attributelinkstoclient SET dbid = REPLACE(dbid, '^', ',');
        UPDATE Attributelinkstoclient SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE Attributelinkstoclient SET AttributeID = NULL WHERE AttributeID IN (0, 94313);
        UPDATE Attributelinkstoclient SET Notes = REPLACE(Notes, '^', ',');
        UPDATE Attributelinkstoclient SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE ActivityAllocations SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE ActivityAllocations SET ContactID = NULL WHERE ContactID IN (0, 94313);
        UPDATE ActivityAllocations SET ActivityProjectID = NULL WHERE ActivityProjectID IN (0, 94313);
        UPDATE ActivityAllocations SET ActivityTypeID = NULL WHERE ActivityTypeID IN (0, 94313);
        UPDATE ActivityAllocations SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE ActivityProjects SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE ActivityProjects SET ContactID = NULL WHERE ContactID IN (0, 94313);
        UPDATE ActivityProjects SET RefStatusID = NULL WHERE RefStatusID IN (0, 94313);
        UPDATE ActivityProjects SET Description = REPLACE(Description, '^', ',');
        UPDATE ActivityProjects SET EmployeeID = NULL WHERE EmployeeID IN (0, 94313);
        UPDATE ActivityProjects SET Notes = REPLACE(Notes, '^', ',');
        UPDATE ActivityProjects SET Acronym = REPLACE(Acronym, '^', ',');
        UPDATE ActivityProjects SET RefBillingMethodID = NULL WHERE RefBillingMethodID IN (0, 94313);
        UPDATE ActivityProjects SET Contact = REPLACE(Contact, '^', ',');
        UPDATE ActivityProjects SET RefContracteeID = NULL WHERE RefContracteeID IN (0, 94313);
        UPDATE ActivityProjects SET RefSourceID = NULL WHERE RefSourceID IN (0, 94313);
        UPDATE ActivityProjects SET ActivityProjectID = NULL WHERE ActivityProjectID IN (0, 94313);
        UPDATE ActivityProjects SET ReferenceNumber = REPLACE(ReferenceNumber, '^', ',');
        UPDATE ActivityProjects SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE ActivityProjects SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE RatesFactor SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE RatesFactor SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE RatesExpense SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE RatesExpense SET ContactIDBY = NULL WHERE ContactIDBY IN (0, 94313);
        UPDATE RatesExpense SET ActivityTypeID = NULL WHERE ActivityTypeID IN (0, 94313);
        UPDATE RatesExpense SET ActivityProjectID = NULL WHERE ActivityProjectID IN (0, 94313);
        UPDATE RatesExpense SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE TransactionsCheques SET TransactionID = NULL WHERE TransactionID IN (0, 94313);
        UPDATE TransactionsCheques SET Type = REPLACE(Type, '^', ',');
        UPDATE TransactionsCheques SET Name = REPLACE(Name, '^', ',');
        UPDATE TransactionsCheques SET Address1 = REPLACE(Address1, '^', ',');
        UPDATE TransactionsCheques SET Address2 = REPLACE(Address2, '^', ',');
        UPDATE TransactionsCheques SET City = REPLACE(City, '^', ',');
        UPDATE TransactionsCheques SET Province = REPLACE(Province, '^', ',');
        UPDATE TransactionsCheques SET Postal = REPLACE(Postal, '^', ',');
        UPDATE TransactionsCheques SET PrintCnt = NULL WHERE PrintCnt IN (0, 94313);
        UPDATE TransactionsCheques SET BillingAccountID = NULL WHERE BillingAccountID IN (0, 94313);
        UPDATE TransactionsCheques SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE Transactions SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE Transactions SET BatchID = NULL WHERE BatchID IN (0, 94313);
        UPDATE Transactions SET ParentID = NULL WHERE ParentID IN (0, 94313);
        UPDATE Transactions SET RecType = REPLACE(RecType, '^', ',');
        UPDATE Transactions SET Description = REPLACE(Description, '^', ',');
        UPDATE Transactions SET RefStatusID = NULL WHERE RefStatusID IN (0, 94313);
        UPDATE Transactions SET ReferenceNumber = REPLACE(ReferenceNumber, '^', ',');
        UPDATE Transactions SET GLEntryIDMain = NULL WHERE GLEntryIDMain IN (0, 94313);
        UPDATE Transactions SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE BillingAccountBalances SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE BillingAccountBalances SET dbid = REPLACE(dbid, '^', ',');
        UPDATE BillingAccountBalances SET DBCODEID = NULL WHERE DBCODEID IN (0, 94313);
        UPDATE BillingAccountBalances SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE BillingAccountBalances SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE CostCentres SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE CostCentres SET SeqNo = NULL WHERE SeqNo IN (0, 94313);
        UPDATE CostCentres SET Description = REPLACE(Description, '^', ',');
        UPDATE CostCentres SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE CostCentres SET Narrative = REPLACE(Narrative, '^', ',');
        UPDATE CostCentres SET ReferenceNumber = REPLACE(ReferenceNumber, '^', ',');
        UPDATE CostCentres SET Classify = REPLACE(Classify, '^', ',');
        UPDATE CostCentres SET TreeString = REPLACE(TreeString, '^', ',');
        UPDATE CostCentres SET TreeSeq = NULL WHERE TreeSeq IN (0, 94313);
        UPDATE CostCentres SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE GLAccountsCostCentres SET GLAccountID = NULL WHERE GLAccountID IN (0, 94313);
        UPDATE GLAccountsCostCentres SET CostCentreID = NULL WHERE CostCentreID IN (0, 94313);
        UPDATE DictionaryFields SET ID = NULL WHERE ID IN (0, 94313);
        UPDATE DictionaryFields SET DictionaryTableID = NULL WHERE DictionaryTableID IN (0, 94313);
        UPDATE DictionaryFields SET Field = REPLACE(Field, '^', ',');
        UPDATE DictionaryFields SET Label = REPLACE(Label, '^', ',');
        UPDATE DictionaryFields SET DataType = REPLACE(DataType, '^', ',');
        UPDATE DictionaryFields SET DataLength = NULL WHERE DataLength IN (0, 94313);
        UPDATE DictionaryFields SET Decimals = NULL WHERE Decimals IN (0, 94313);
        UPDATE DictionaryFields SET isnullable = REPLACE(isnullable, '^', ',');
        UPDATE DictionaryFields SET Sequence = NULL WHERE Sequence IN (0, 94313);
        UPDATE DictionaryFields SET Purpose = REPLACE(Purpose, '^', ',');
        UPDATE DictionaryFields SET ForeignTableID = NULL WHERE ForeignTableID IN (0, 94313);
        UPDATE DictionaryFields SET DetailedDescription = REPLACE(DetailedDescription, '^', ',');
        UPDATE DictionaryFields SET RecordStatus = REPLACE(RecordStatus, '^', ',');
        UPDATE DictionaryFields SET UpdateCount = NULL WHERE UpdateCount IN (0, 94313);
        UPDATE AccountBalances SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Activities SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE ActivityAllocations SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE ActivityProjects SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE ActivityTypes SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Attributelinkstoclient SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Attributes SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE AuditLogs SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Batches SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE BatchTypes SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE BillingAccountBalances SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE BillingAccounts SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Budgets SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE CommandJoins SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Commands SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Contacts SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE ContactSetup SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE CostCentres SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE CrossReference SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Employees SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE FixedAssetLocation SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE FixedAssetLocationHistory SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE FixedAssets SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE FixedAssetType SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE GLAccounts SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE GLEntries SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE OrganizationalPositions SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE PeopleDatabase SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE PersonalComments SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE PositionHistory SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE RatesBilling SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE RatesExpense SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE RatesFactor SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE ReferenceFields SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE ReferenceTables SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Relationships SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE Transactions SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE TransactionsCheques SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;
        UPDATE TransactionsInvoices SET RecordStatus=COALESCE(RecordStatus, 'a') WHERE recordstatus IS NULL;

        DELETE
        FROM Activities
        WHERE id IN (
                        SELECT id
                        FROM Activities
                        GROUP BY id
                        HAVING COUNT(*) > 1);

        DELETE
        FROM contacts
        WHERE id IN (
                        SELECT id
                        FROM contacts
                        GROUP BY id
                        HAVING COUNT(*) > 1)
          AND recordstatus = 'D';

        DELETE
        FROM Employees
        WHERE id IN (
                        SELECT id
                        FROM Employees
                        GROUP BY id
                        HAVING COUNT(*) > 1)
          AND (recordstatus = 'D' OR firstname IS NULL);


        DELETE
        FROM auditlogs
        WHERE id IN (
                        SELECT id
                        FROM Auditlogs
                        GROUP BY id
                        HAVING COUNT(*) > 1);

        IF EXISTS(
                SELECT id
                FROM billingaccounts
                GROUP BY id
                HAVING COUNT(*) > 1)
        THEN
            RAISE EXCEPTION 'You need to clean-up the dup ids for 6707,0,12245,6818';
        END IF;

        DELETE FROM contacts WHERE id IS NULL;

        DELETE FROM organizationalpositions WHERE id IS NULL;

        DELETE FROM transactionsinvoices WHERE id IS NULL;

        DELETE FROM billingaccounts WHERE id IS NULL;

        DELETE FROM TransactionsCheques WHERE TransactionID IS NULL;

        DELETE
        FROM TransactionsCheques
        WHERE TransactionID IN (
                                   SELECT TransactionID
                                   FROM TransactionsCheques
                                   GROUP BY TransactionID
                                   HAVING COUNT(*) > 1
        );

        UPDATE glentries
        SET id = (
                     SELECT MAX(id) + 1
                     FROM glentries)
        WHERE id IS NULL;

        --         DELETE from glentries
--         WHERE transactionid IN
--               (
--                   SELECT aa.transactionid
--                   FROM glentries aa
--                   WHERE aa.recordstatus = 'a'
--                   GROUP BY aa.transactionid
--                   HAVING SUM(aa.amount);

        DELETE
        FROM glentries
        WHERE transactionid IN (
                                   SELECT id
                                   FROM transactions
                                   WHERE recordstatus = 'D');

        DELETE FROM glentries WHERE recordstatus = 'D';

        DELETE FROM transactionscheques WHERE recordstatus = 'D';
        DELETE FROM transactionsinvoices WHERE recordstatus = 'D';
        DELETE FROM transactions WHERE recordstatus = 'D';

        DELETE
        FROM glaccounts a
        WHERE NOT EXISTS(SELECT 1 FROM GLEntries aa WHERE aa.GLAccountID = a.id LIMIT 1)
          AND NOT EXISTS(SELECT 1 FROM GLAccounts aa WHERE aa.GLAccountID = a.id LIMIT 1);

        DELETE
        FROM costcentres a
        WHERE NOT EXISTS(SELECT 1 FROM GLEntries aa WHERE aa.costcentreID = a.id LIMIT 1)
          AND NOT EXISTS(SELECT 1 FROM costcentres aa WHERE aa.costcentreID = a.id LIMIT 1);

        UPDATE activityprojects a
        SET costcentreid=NULL
        WHERE costcentreid IS NOT NULL
          AND NOT EXISTS
            (SELECT aa.id FROM costcentres aa WHERE aa.id = a.costcentreid);

        UPDATE fixedassets a
        SET costcentreid=NULL
        WHERE costcentreid IS NOT NULL
          AND NOT EXISTS
            (SELECT aa.id FROM costcentres aa WHERE aa.id = a.costcentreid);

        UPDATE ContactSetup SET id=1;

        DROP TABLE IF EXISTS aaMultilinkTableRule;
        CREATE TABLE aaMultilinkTableRule
        (
            ID                          INT,
            Key                         VARCHAR,
            sysDictionaryColumnIDSource INT,
            sysDictionaryColumnIDDest   INT,
            SeqNo                       INT,
            Description                 VARCHAR,
            sysDictionaryTableID        INT,
            WhereClause                 VARCHAR,
            RefTable                    VARCHAR,
            RefValue                    VARCHAR,
            RefID                       INT
        );

        COPY aaMultilinkTableRule FROM 'c:\temp\bmsdata\aaMultilinkTableRule.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        UPDATE aaMultilinkTableRule a
        SET refid=b.Refid
        FROM (
                 SELECT a.id RefID, b.shortcode RefTable, a.description RefValue
                 FROM referencefields a
                 JOIN referencetables b
                      ON b.id = a.referencetableid) b
        WHERE a.reftable = b.reftable
          AND a.refvalue = b.refvalue;

        UPDATE employees a
        SET startdate=b.completiondate
        FROM (
                 SELECT employeeid, MIN(completiondate) completiondate FROM activities aa GROUP BY employeeid) b
        WHERE a.id = b.employeeid
          AND a.startdate IS NULL;

        UPDATE employees a
        SET startdate=b.completiondate
        FROM (
                 SELECT employeeid, MIN(logdate) completiondate FROM auditlogs aa GROUP BY employeeid) b
        WHERE a.id = b.employeeid
          AND a.startdate IS NULL;

        UPDATE employees SET startdate=DATE '1990-01-01' WHERE startdate IS NULL;

        TRUNCATE TABLE organizationalpositions;

        DROP TABLE IF EXISTS aaGLAccountType;
        CREATE TABLE aaGLAccountType
        (
            ID                  NUMERIC(10),
            Description         VARCHAR,
            IsBalanceSheet      BOOLEAN,
            NormalizationFactor NUMERIC(10)
        );

        COPY aaGLAccountType FROM 'c:\temp\bmsdata\aaGLAccountType.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        DROP TABLE IF EXISTS aaGLAccount;
        CREATE TABLE aaGLAccount
        (
            id                         BIGINT,
            glaccountidparent          BIGINT,
            glaccounttypeid            BIGINT,
            sysmultilinktableruleid    BIGINT,
            bankaccountnumber          BIGINT,
            bottomuplevel              BIGINT,
            comments                   VARCHAR,
            description                VARCHAR,
            displaysequence            BIGINT,
            iscollapseonexportrequired BOOLEAN,
            isusedtoclassifyrecords    BOOLEAN,
            quickcode                  VARCHAR,
            referencenumber            BIGINT,
            topdownlevel               BIGINT,
            rowstatus                  VARCHAR,
            syschangehistoryid         BIGINT,
            OldID                      BIGINT,
            StageID                    BIGINT
        );

        COPY aaGLAccount FROM 'c:\temp\bmsdata\aaGLAccount.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

    END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _staging.TranslateDBID(p_dbid VARCHAR)
    RETURNS INTEGER AS
$BODY$

BEGIN
    RETURN CASE
        WHEN p_dbid = 'AC' THEN 420
        WHEN p_dbid = 'AP' THEN 401
        WHEN p_dbid = 'BA' THEN 310
        WHEN p_dbid = 'BL' THEN 321
        WHEN p_dbid = 'CL' THEN 100
        WHEN p_dbid = 'FA' THEN 200
        WHEN p_dbid = 'FL' THEN 503
        WHEN p_dbid = 'FS' THEN 320
        WHEN p_dbid = 'IR' THEN 101
        WHEN p_dbid = 'LO' THEN 503
        WHEN p_dbid = 'PS' THEN 316
        WHEN p_dbid = 'RL' THEN 120
        WHEN p_dbid = 'SI' THEN 207
        WHEN p_dbid = 'TX' THEN 311
        WHEN p_dbid = 'T4' THEN 313
        WHEN p_dbid = 'T8' THEN 314
        END;


END;
$BODY$
    LANGUAGE plpgsql
    IMMUTABLE
;
