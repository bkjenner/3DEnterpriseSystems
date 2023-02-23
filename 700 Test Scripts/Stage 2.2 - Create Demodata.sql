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
This script imports additional demo data for the proof of concept environment.
---------------------------------------
Instructions

 This script uses the data from s0001v0000 and imports from the demo data spreadsheet
 to populate the demo data schema which is used to prepare each of the systems.

 */

SELECT PG_TERMINATE_BACKEND(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname LIKE 'sn%'
  AND datname != 'sn000000'
  AND pid <> PG_BACKEND_PID();

-- Switch to DemoData
DROP SCHEMA IF EXISTS DemoData CASCADE;
CREATE SCHEMA DemoData;
SET SEARCH_PATH TO demodata, s0000v0000, public;

DROP TABLE IF EXISTS sysDictionaryColumn;
CREATE TABLE sysDictionaryColumn
(
    ID                          BIGINT,
    sysDictionaryTableID        BIGINT,
    Name                        VARCHAR,
    Label                       VARCHAR,
    ColumnSequence              INT,
    DefaultValue                VARCHAR,
    isnullable                  BOOLEAN,
    datatype                    VARCHAR,
    datalength                  INT,
    decimals                    INT,
    purpose                     VARCHAR,
    sysDictionaryTableIDForeign BIGINT,
    IsChangeHistoryUsed         BOOLEAN,
    RowStatus                   CHAR(1),
    sysChangeHistoryID          BIGINT
);

COPY sysDictionaryColumn FROM 'c:\temp\BMSData\sysDictionaryColumn.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

UPDATE sysDictionaryColumn set IsChangeHistoryUsed=false where ischangehistoryused is null;

DROP TABLE IF EXISTS sysDictionaryTable;
CREATE TABLE sysDictionaryTable
(
    ID                  BIGINT,
    Name                VARCHAR,
    Translation         VARCHAR,
    ChangeHistoryLevel  INT,
    ChangeHistoryScope  VARCHAR,
    Description         VARCHAR,
    IsChangeHistoryUsed BOOLEAN,
    IsTableTemporal     BOOLEAN,
    NormalizedName      VARCHAR,
    SingularName        VARCHAR,
    PluralName          VARCHAR,
    SystemModule        VARCHAR,
    TableType           VARCHAR,
    RowStatus           CHAR(1),
    sysChangeHistoryID  BIGINT
);

COPY sysDictionaryTable FROM 'c:\temp\BMSData\sysDictionaryTable.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS crmrelationshiptype;
CREATE TABLE crmrelationshiptype AS
SELECT *
FROM s0001v0000.crmrelationshiptype;

DROP TABLE IF EXISTS crmrelationship;
CREATE TABLE crmrelationship
(
    ID                    BIGINT,
    crmcontactid1         BIGINT,
    crmcontactid2         BIGINT,
    crmrelationshiptypeid BIGINT,
    temporalstartdate     DATE,
    temporalenddate       DATE,
    rowstatus             VARCHAR,
    syschangehistoryid    BIGINT
);

COPY crmrelationship FROM 'c:\temp\BMSData\crmrelationship.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS crmContact;
CREATE TABLE crmContact
(
    Id                 BIGINT,
    Name               VARCHAR,
    lastname           VARCHAR,
    middlename         VARCHAR,
    firstname          VARCHAR,
    crmgenderid        BIGINT,
    birthdate          DATE,
    ContactPerson      VARCHAR,
    RowStatus          CHAR(1),
    sysChangeHistoryID BIGINT
);

COPY crmContact FROM 'c:\temp\BMSData\crmContact.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS crmAddress;
CREATE TABLE crmAddress
(
    ID                 BIGINT,
    crmContactID       BIGINT,
    AddressType        VARCHAR,
    Address1           VARCHAR,
    Address2           VARCHAR,
    Address3           VARCHAR,
    City               VARCHAR,
    Province           VARCHAR,
    Country            VARCHAR,
    PostalCode         VARCHAR,
    RowStatus          CHAR(1),
    sysChangeHistoryID BIGINT
);

COPY crmAddress FROM 'c:\temp\BMSData\crmAddress.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS ogWellProd;
CREATE TABLE ogWellProd
(
    ID                 BIGINT,
    ogWellID           BIGINT,
    ProductionYear     BIGINT,
    ProductDescription VARCHAR,
    ProductionVolume   NUMERIC(10, 1),
    InletGatheredVOL   NUMERIC(10, 1),
    PortionOFProduced  NUMERIC(10, 5),
    RowStatus          CHAR(1),
    sysChangeHistoryID BIGINT
);

COPY ogWellProd FROM 'c:\temp\BMSData\ogWellProd.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS ogWell;
CREATE TABLE ogWell
(
    ID                   BIGINT,
    UWI                  VARCHAR,
    lrParcelID           BIGINT,
    WellStatus           VARCHAR,
    StatusDate           DATE,
    WellType             VARCHAR,
    crmcontactidLicensee BIGINT,
    crmcontactidOperator BIGINT,
    TemporalStartDate    DATE,
    TemporalEndDate      DATE,
    RowStatus            CHAR(1),
    sysChangeHistoryID   BIGINT
);

COPY ogWell FROM 'c:\temp\BMSData\ogWell.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS mtTaxroll;
CREATE TABLE mtTaxroll
(
    ID                 BIGINT,
    TaxrollNumber      VARCHAR,
    crmContactID       BIGINT,
    Status             VARCHAR,
    Type               VARCHAR,
    Disposition        VARCHAR,
    temporalstartdate  DATE,
    temporalenddate    DATE,
    RowStatus          CHAR(1),
    sysChangeHistoryID BIGINT
);

COPY mtTaxroll FROM 'c:\temp\BMSData\mtTaxroll.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS mtAssessment;
CREATE TABLE mtAssessment
(
    ID                 BIGINT,
    lrParcelID         BIGINT,
    Land               INT,
    Building           INT,
    AssessmentDate     DATE,
    Total              INT,
    LandUse            VARCHAR,
    TemporalStartDate  DATE,
    TemporalEndDate    DATE,
    RowStatus          CHAR(1),
    sysChangeHistoryID BIGINT
);

COPY mtAssessment FROM 'c:\temp\BMSData\mtAssessment.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS lrparcelinterest;
CREATE TABLE lrparcelinterest
(
    id                            BIGINT,
    sysdictionarytableidappliesto BIGINT,
    rowidappliesto                BIGINT,
    lrinterestid                  BIGINT,
    acres                         NUMERIC(10, 2),
    temporalstartdate             DATE,
    temporalenddate               DATE,
    rowstatus                     CHAR,
    syschangehistoryid            BIGINT
);

COPY lrparcelinterest FROM 'c:\temp\BMSData\lrparcelinterest.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS lrParcel;
CREATE TABLE lrParcel
(
    ID                 BIGINT,
    legaldescription   VARCHAR,
    atslocation        VARCHAR,
    mtTaxrollID        BIGINT,
    acresgross         NUMERIC(10, 2),
    surveystatus       VARCHAR,
    parceltype         VARCHAR,
    ownership          VARCHAR,
    acresunbroken      NUMERIC(10, 2),
    waterstatus        VARCHAR,
    purpose            VARCHAR,
    temporalstartdate  DATE,
    temporalenddate    DATE,
    rowstatus          CHAR,
    syschangehistoryid BIGINT
);

COPY lrParcel FROM 'c:\temp\BMSData\lrParcel.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS lrInterest;
CREATE TABLE lrInterest
(
    ID                 BIGINT,
    crmcontactid       BIGINT,
    interestnumber     VARCHAR,
    origin             VARCHAR,
    interesttype       VARCHAR,
    holderstatus       VARCHAR,
    purpose            VARCHAR,
    intereststatus     VARCHAR,
    TemporalStartDate  DATE,
    TemporalEndDate    DATE,
    RowStatus          CHAR,
    sysChangeHistoryID BIGINT
);

COPY lrInterest FROM 'c:\temp\BMSData\lrInterest.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS lrPlanParcel;
CREATE TABLE lrPlanParcel
(
    ID                 BIGINT,
    lrParcelId         BIGINT,
    lrPlanID           BIGINT,
    Disposition        VARCHAR,
    Rowstatus          CHAR,
    sysChangeHistoryID BIGINT
);

COPY lrPlanParcel FROM 'c:\temp\BMSData\lrPlanParcel.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS lrPlan;
CREATE TABLE lrPlan
(
    ID                 BIGINT,
    crmContactID       BIGINT,
    PlanNumber         VARCHAR,
    PlanStatus         VARCHAR,
    PlanType           VARCHAR,
    PlanMethod         VARCHAR,
    RegistrationDate   DATE,
    RowStatus          CHAR,
    sysChangeHistoryID BIGINT
);

COPY lrPlan FROM 'c:\temp\BMSData\lrPlan.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS hrPosition;
CREATE TABLE hrPosition
(
    ID                 BIGINT,
    hrPositionIDParent BIGINT,
    WorkingTitle       VARCHAR
);

COPY hrPosition FROM 'c:\temp\BMSData\hrPosition.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS comLocation;
CREATE TABLE comLocation
(
    id                      BIGINT,
    comlocationidparent     BIGINT,
    comments                VARCHAR,
    description             VARCHAR,
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    isusedtoclassifyrecords BOOLEAN,
    displaysequence         INT,
    topdownlevel            INT,
    bottomuplevel           INT,
    shortcode               VARCHAR,
    rowstatus               CHAR,
    syschangehistoryid      BIGINT
);

COPY comLocation FROM 'c:\temp\BMSData\comLocation.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS falocationhistory;
CREATE TABLE falocationhistory
(
    id                           BIGINT,
    crmcontactidassignedto       BIGINT,
    glCostCentreIDOwnedBy        BIGINT,
    fafixedassetid               BIGINT,
    sysdictionarytableidlocation BIGINT,
    rowidlocation                BIGINT,
    details                      VARCHAR,
    isactive                     BOOLEAN,
    isconfirmed                  BOOLEAN,
    temporalstartdate            DATE,
    temporalenddate              DATE,
    rowstatus                    CHAR,
    syschangehistoryid           BIGINT
);

COPY falocationhistory FROM 'c:\temp\BMSData\falocationhistory.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS faFixedAsset;
CREATE TABLE faFixedAsset
(
    ID                       BIGINT,
    faStatusID               BIGINT,
    faTypeID                 BIGINT,
    crmContactID             VARCHAR,
    Description              VARCHAR,
    PurchaseDate             DATE,
    FixedAssetNumber         VARCHAR,
    SerialNumber             VARCHAR,
    Make                     VARCHAR,
    ModelNumber              VARCHAR,
    ModelYear                INT,
    Comments                 VARCHAR,
    DepreciationLife         INT,
    DepreciationSalvageValue INT,
    WarrantyExpiryDate       DATE,
    RowStatus                CHAR,
    sysChangeHistoryID       BIGINT
);

COPY faFixedAsset FROM 'c:\temp\BMSData\faFixedAsset.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS faType;
CREATE TABLE faType
(
    id                                  BIGINT,
    fatypeidparent                      BIGINT,
    glaccountidaccummulateddepreciation BIGINT,
    glaccountidasset                    BIGINT,
    glaccountiddepreciationexpense      BIGINT,
    depreciationlife                    INT,
    depreciationpercent                 INT,
    description                         VARCHAR,
    isfixedassetnumberused              BOOLEAN,
    isusedtoclassifyrecords             BOOLEAN,
    replacementamount                   INT,
    displaysequence                     INT,
    topdownlevel                        INT,
    bottomuplevel                       INT,
    template                            VARCHAR,
    rowstatus                           CHAR,
    syschangehistoryid                  BIGINT
);

COPY faType FROM 'c:\temp\BMSData\faType.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS vrVehicle;
CREATE TABLE vrVehicle
(
    Id                 BIGINT,
    crmContactID       BIGINT,
    Make               VARCHAR,
    Model              VARCHAR,
    Year               INTEGER,
    Serialnumber       VARCHAR,
    Color              VARCHAR,
    Temporalstartdate  DATE,
    Temporalenddate    DATE,
    rowstatus          CHAR,
    Syschangehistoryid BIGINT
);

COPY vrVehicle FROM 'c:\temp\BMSData\vrVehicle.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

DROP TABLE IF EXISTS glcostcentre;
CREATE TABLE glcostcentre AS
SELECT *
FROM s0001v0000.glcostcentre
LIMIT 0;

INSERT INTO glcostcentre (id, glcostcentreidparent, description, isusedtoclassifyrecords, referencenumber, rowstatus)
VALUES (1000000000000000, NULL, 'Government of Doreney', FALSE, NULL, 'a'),
       (1000000000000001, 1000000000000000, 'Executive Office', TRUE, '100', 'a'),
       (1000000000000002, 1000000000000000, 'Citizen Registry', TRUE, '101', 'a'),
       (1000000000000003, 1000000000000000, 'Corporate Registry', TRUE, '102', 'a'),
       (1000000000000004, 1000000000000000, 'Land Titles', TRUE, '103', 'a'),
       (1000000000000005, 1000000000000000, 'Energy', TRUE, '104', 'a'),
       (1000000000000006, 1000000000000000, 'Municipal Affairs', FALSE, NULL, 'a'),
       (1000000000000007, 1000000000000006, 'Municipal Corporate', TRUE, '105', 'a'),
       (1000000000000008, 1000000000000006, 'Municipal District #1', TRUE, '106', 'a'),
       (1000000000000009, 1000000000000006, 'Municipal District #2', TRUE, '107', 'a'),
       (1000000000000010, 1000000000000000, 'Vehicle Registry', TRUE, '108', 'a'),
       (1000000000000011, 1000000000000000, 'Public Works', TRUE, '109', 'a');


DROP TABLE IF EXISTS glaccount;
CREATE TABLE glaccount AS
SELECT *
FROM s0001v0000.glaccount
LIMIT 0;

INSERT INTO glaccount (id, glaccountidparent, glaccounttypeid, sysmultilinktableruleid, bankaccountnumber, topdownlevel, bottomuplevel, comments, description, iscollapseonexportrequired, isusedtoclassifyrecords, quickcode, referencenumber, displaysequence, rowstatus, syschangehistoryid)
SELECT CASE WHEN fnsysidview(id, 's') != 0 THEN fnsysidget(100, fnsysidview(id, 'r'))
            ELSE id
            END id,
       CASE WHEN fnsysidview(glaccountidparent, 's') != 0
                THEN fnsysidget(100, fnsysidview(glaccountidparent, 'r'))
            ELSE glaccountidparent
            END glaccountidparent,
       glaccounttypeid,
       sysmultilinktableruleid,
       bankaccountnumber,
       topdownlevel,
       bottomuplevel,
       comments,
       description,
       iscollapseonexportrequired,
       isusedtoclassifyrecords,
       quickcode,
       referencenumber,
       displaysequence,
       rowstatus,
       syschangehistoryid
FROM s0001v0000.glaccount
WHERE id NOT IN (
                    SELECT id
                    FROM glaccount);

DROP TABLE IF EXISTS glentry;
CREATE TABLE glentry AS
SELECT *
FROM s0001v0000.glentry
LIMIT 0;

INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
SELECT a.id,
       CASE WHEN fnsysidview(a.glaccountid, 's') != 0
                THEN fnsysidget(100, fnsysidview(a.glaccountid, 'r'))
            ELSE a.glaccountid
            END glaccountid,
       a.glcostcentreid,
       a.glentrytypeid,
       a.gltransactionid,
       a.sysdictionarytableidchargedto,
       a.rowidchargedto,
       a.amount,
       a.bankreconciliationdate,
       a.comments,
       a.description,
       a.reconciliationbalance,
       a.referencenumber,
       a.reportingperioddate,
       a.rollupamount,
       1
FROM s0001v0000.glentry a
JOIN s0001v0000.gltransaction b
     ON b.id = a.gltransactionid
WHERE transactiondate >= '1996-12-01'
  AND transactiondate <= '2000-11-30';

INSERT INTO glentry (id, glaccountid, glcostcentreid, glentrytypeid, gltransactionid, sysdictionarytableidchargedto, rowidchargedto, amount, bankreconciliationdate, comments, description, reconciliationbalance, referencenumber, reportingperioddate, rollupamount, syschangehistoryid)
SELECT id,
       CASE WHEN fnsysidview(glaccountid, 's') != 0
                THEN fnsysidget(100, fnsysidview(glaccountid, 'r'))
            ELSE glaccountid
            END glaccountid,
       glcostcentreid,
       glentrytypeid,
       gltransactionid,
       sysdictionarytableidchargedto,
       rowidchargedto,
       amount,
       bankreconciliationdate,
       comments,
       description,
       reconciliationbalance,
       referencenumber,
       reportingperioddate,
       rollupamount,
       2
FROM s0001v0000.glentry
WHERE gltransactionid IN (
                             SELECT DISTINCT c.gltransactionid
                             FROM s0001v0000.fafixedasset a
                             JOIN LATERAL (SELECT MIN(transactiondate) transactiondate
                                           FROM s0001v0000.glentry aa
                                           JOIN s0001v0000.gltransaction bb
                                                ON bb.id = aa.gltransactionid
                                           WHERE aa.sysdictionarytableidchargedto = 200
                                             AND aa.rowidchargedto = a.id
                                             AND glaccountid IN (1700, 10000000000014, 10000000000020, 10000000000089)) b
                                  ON TRUE
                             JOIN LATERAL (SELECT MIN(gltransactionid) gltransactionid
                                           FROM s0001v0000.glentry aa
                                           JOIN s0001v0000.gltransaction bb
                                                ON bb.id = aa.gltransactionid
                                           WHERE aa.sysdictionarytableidchargedto = 200
                                             AND aa.rowidchargedto = a.id
                                             AND bb.transactiondate = b.transactiondate
                                             AND glaccountid IN (1700, 10000000000014, 10000000000020, 10000000000089)) c
                                  ON TRUE
                             WHERE b.transactiondate IS NOT NULL)
  AND id NOT IN (
                    SELECT id
                    FROM glentry)
ORDER BY gltransactionid;

UPDATE glentry
SET description='PAYRL ' || description = SUBSTR(description, 13, 999)
WHERE description LIKE 'PAYR%';

DROP TABLE IF EXISTS gltransaction;
CREATE TABLE gltransaction AS
SELECT *
FROM s0001v0000.gltransaction
LIMIT 0;

INSERT INTO gltransaction (id, glbatchid, glentryidmain, glpostingstatusid, gltransactionidreversed, gltransactiontypeid, sysdictionarytableidsubtype, createdate, description, duedate, referencenumber, transactiondate, syschangehistoryid)
SELECT a.id,
       a.glbatchid,
       a.glentryidmain,
       1 glpostingstatusid,
       a.gltransactionidreversed,
       a.gltransactiontypeid,
       a.sysdictionarytableidsubtype,
       dateadd('d', -2, d.transactiondate),
       c.description,
       a.duedate,
       a.referencenumber,
       d.transactiondate,
       a.syschangehistoryid
FROM s0001v0000.gltransaction a
JOIN LATERAL (SELECT gltransactionid, MIN(id) glentryid FROM glentry GROUP BY gltransactionid ) b
     ON b.gltransactionid = a.id
JOIN glentry c
     ON c.id = b.glentryid
JOIN LATERAL (SELECT CASE WHEN c.syschangehistoryid = 2 THEN '2018-12-31'
                          ELSE dateadd('m', 1, dateadd('y', 22, a.transactiondate))
                          END transactiondate ) d
     ON TRUE;

-- If the reversal id refers to a transaction previous to the demo data then null it
UPDATE gltransaction a
SET gltransactionidreversed=NULL
FROM (
         SELECT aa.id
         FROM gltransaction aa
         LEFT JOIN gltransaction bb
                   ON bb.id = aa.gltransactionidreversed
         WHERE aa.gltransactionidreversed IS NOT NULL
           AND bb.id IS NULL) b
WHERE b.id = a.id;

DROP TABLE IF EXISTS glbillingaccount;
CREATE TABLE glbillingaccount AS
SELECT *
FROM s0001v0000.glbillingaccount
LIMIT 0;

INSERT INTO glbillingaccount (id, crmcontactid, glaccountid, glbillingaccountstatusid, glbillingmethodid, glefttypeid, accountnumber, comments, currentbalance, duedate, hoursperday, isgstexempt, ishstexempt, ispremiumtaxexempt, ispstexempt, manualamount, name, paymentterms, transitnumber, typeid, rowstatus, syschangehistoryid)
SELECT a.id,
       a.crmcontactid,
       CASE WHEN fnsysidview(a.glaccountid, 's') != 0
                THEN fnsysidget(100, fnsysidview(a.glaccountid, 'r'))
            ELSE a.glaccountid
            END glaccountid,
       a.glbillingaccountstatusid,
       a.glbillingmethodid,
       a.glefttypeid,
       a.accountnumber,
       a.comments,
       a.currentbalance,
       a.duedate,
       a.hoursperday,
       a.isgstexempt,
       a.ishstexempt,
       a.ispremiumtaxexempt,
       a.ispstexempt,
       a.manualamount,
       a.name,
       a.paymentterms,
       a.transitnumber,
       a.typeid,
       a.rowstatus,
       a.syschangehistoryid
FROM s0001v0000.glbillingaccount a
WHERE a.id IN (
                  SELECT rowidchargedto FROM glentry aa WHERE aa.sysdictionarytableidchargedto = 321);

DROP TABLE IF EXISTS glbatch;
CREATE TABLE glbatch AS
SELECT *
FROM s0001v0000.glbatch
LIMIT 0;

INSERT INTO glbatch (id, glaccountid, glbatchstatusid, glbatchtypeid, glexportbatchid, approvaldate, createdate, description, fiscalyear, hasbatcherror, syschangehistoryid)
SELECT a.id,
       a.glaccountid,
       a.glbatchstatusid,
       a.glbatchtypeid,
       a.glexportbatchid,
       dateadd('m', 1, dateadd('y', 22, a.approvaldate)),
       dateadd('m', 1, dateadd('y', 22, a.createdate)),
       a.description,
       a.fiscalyear + 22,
       a.hasbatcherror,
       a.syschangehistoryid
FROM s0001v0000.glbatch A;

DROP TABLE IF EXISTS actproject;
CREATE TABLE actproject AS
SELECT *
FROM s0001v0000.actproject a
WHERE a.id IN (
                  SELECT aa.rowidchargedto FROM glentry aa WHERE aa.sysdictionarytableidchargedto = 401);

DROP TABLE IF EXISTS actactivity;
CREATE TABLE actactivity AS
SELECT *
FROM s0001v0000.actactivity
LIMIT 0;

INSERT INTO actactivity (id, actprojectid, actpriorityid, actstatusid, acttypeid, sysdictionarytableidperformedby, rowidperformedby, sysdictionarytableidperformedfor, rowidperformedfor, comments, completiondate, description, startdate, totalactual, rowstatus, syschangehistoryid)
SELECT a.id,
       a.actprojectid,
       a.actpriorityid,
       a.actstatusid,
       a.acttypeid,
       a.sysdictionarytableidperformedby,
       a.rowidperformedby,
       a.sysdictionarytableidperformedfor,
       a.rowidperformedfor,
       a.comments,
       dateadd('m', 1, dateadd('y', 22, a.completiondate)) completiondate,
       a.description,
       dateadd('m', 1, dateadd('y', 22, a.completiondate)) startdate,
       a.totalactual,
       a.rowstatus,
       a.id                                                syschangehistoryid
FROM s0001v0000.actactivity a
WHERE completiondate >= '1996-12-01'
  AND completiondate <= '2000-11-30'
UNION ALL
SELECT a.id,
       a.actprojectid,
       a.actpriorityid,
       a.actstatusid,
       a.acttypeid,
       a.sysdictionarytableidperformedby,
       a.rowidperformedby,
       a.sysdictionarytableidperformedfor,
       a.rowidperformedfor,
       a.comments,
       TO_DATE(((a.id % 3) + 2019)::VARCHAR || RIGHT('0' || month(a.completiondate)::VARCHAR, 2) ||
               RIGHT('0' || day(a.completiondate)::VARCHAR, 2), 'YYYYMMDD') completiondate,
       a.description,
       NULL                                                                 startdate,
       a.totalactual,
       a.rowstatus,
       a.id                                                                 syschangehistoryid
FROM s0001v0000.actactivity a
WHERE NOT (month(a.completiondate) = 2 AND day(completiondate) = 29)
  AND NOT (completiondate >= '1996-12-01' AND completiondate <= '2000-11-30')
  AND a.id % 5 = 0
  AND a.actprojectid IN (
                            SELECT id
                            FROM actproject);

DO
$$
    DECLARE
        l_updatecount INT;
        l_loopcount   INT := 0;
    BEGIN
        LOOP
            UPDATE actproject a
            SET crmcontactid=b.crmcontactid, syschangehistoryid= -1
            FROM actproject b
            WHERE a.actprojectidparent = b.id
              AND COALESCE(a.crmcontactid, -1) != COALESCE(b.crmcontactid, -1);
            GET DIAGNOSTICS l_updatecount = ROW_COUNT;
            IF l_updatecount = 0
            THEN
                RETURN;
            END IF;
            l_loopcount := l_loopcount + 1;
            IF l_loopcount > 100
            THEN
                RAISE EXCEPTION 'Recursive loop on updating crmcontactid on actproject';
            END IF;
        END LOOP;
    END
$$ LANGUAGE plpgsql;

INSERT INTO actproject
SELECT *
FROM s0001v0000.actproject a
WHERE a.id IN (
                  SELECT aa.actprojectid
                  FROM actactivity aa)
  AND a.id NOT IN (
                      SELECT id
                      FROM actproject);

INSERT INTO actproject
SELECT *
FROM s0001v0000.actproject a
WHERE a.id IN (
                  SELECT actprojectidparent
                  FROM actproject aa)
  AND a.id NOT IN (
                      SELECT id
                      FROM actproject);

DROP TABLE IF EXISTS actactivitysubtypebilling;
CREATE TABLE actactivitysubtypebilling AS
SELECT *
FROM s0001v0000.actactivitysubtypebilling a
WHERE a.actactivityid IN (
                             SELECT id
                             FROM actactivity);

UPDATE actactivitysubtypebilling
SET id=actactivityid, crmcontactidinvoicethrough=NULL, overridehours=NULL;

UPDATE actactivitysubtypebilling a
SET gltransactionid=NULL
WHERE id IN (
                SELECT aa.id
                FROM actactivitysubtypebilling aa
                LEFT JOIN gltransaction bb
                          ON bb.id = aa.gltransactionid
                WHERE aa.gltransactionid IS NOT NULL
                  AND bb.id IS NULL);

-- DELETE
-- FROM actactivitysubtypebilling
-- WHERE actactivityid NOT IN (
--                                SELECT id
--                                FROM actactivity);

UPDATE actproject a
SET baseamount=baseamount * 10, startdate=b.startdate, rowstatus='a', shortcode='', completiondate=NULL,
    description=TRIM(leftfind(b.description, ' ') || ' Support ' || rightfind(b.description, ' '))
FROM (
         SELECT actprojectid, MIN(completiondate) startdate, MAX(description) description
         FROM actactivity
         GROUP BY actprojectid) b
WHERE b.actprojectid = a.id;

DELETE
FROM actproject
WHERE id NOT IN (
                    SELECT actprojectid
                    FROM actactivity);

UPDATE glentry
SET rowidchargedto=NULL, sysdictionarytableidchargedto=NULL
WHERE sysdictionarytableidchargedto = 401
  AND rowidchargedto NOT IN (
                                SELECT id
                                FROM actproject);

DROP TABLE IF EXISTS actratebilling;
CREATE TABLE actratebilling AS
SELECT *
FROM s0001v0000.actratebilling a
WHERE a.actprojectid IN (
                            SELECT id
                            FROM actproject)
  AND (a.rowidchargedby IS NULL OR rowidchargedby IN (
                                                         SELECT rowidperformedby
                                                         FROM actactivity));

DROP TABLE IF EXISTS actrateexpense;
CREATE TABLE actrateexpense AS
SELECT *
FROM s0001v0000.actrateexpense a
WHERE a.rowidchargedby IN (
                              SELECT aa.rowidchargedby
                              FROM actratebilling aa
                              WHERE a.rowidchargedby IS NOT NULL);

DROP TABLE IF EXISTS actType;
CREATE TABLE actType
(
    id                                  BIGINT,
    actcostunitid                       BIGINT,
    acttypeidparent                     BIGINT,
    glaccountid                         BIGINT,
    glcostcentreid                      BIGINT,
    sysmultilinktableruleidperformedby  BIGINT,
    sysmultilinktableruleidperformedfor BIGINT,
    comments                            VARCHAR,
    description                         VARCHAR,
    displaysequence                     INTEGER,
    topdownlevel                        INTEGER,
    bottomuplevel                       INTEGER,
    rowstatus                           CHAR,
    syschangehistoryid                  BIGINT
);

COPY actType FROM 'c:\temp\BMSData\actType.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

SELECT fnsysHierarchyUpdate('acttype', 'description');

-- Convert old actType ids to new ones
UPDATE actactivity a
SET acttypeid=b.id, syschangehistoryid=acttypeid
FROM acttype b
WHERE a.acttypeid = b.syschangehistoryid;

UPDATE actratebilling a
SET acttypeid=b.id, syschangehistoryid=acttypeid
FROM acttype b
WHERE a.acttypeid = b.syschangehistoryid;

DELETE
FROM actratebilling a
WHERE a.id IN (
                  SELECT aa.id
                  FROM actratebilling aa
                  LEFT JOIN acttype bb
                            ON bb.id = aa.acttypeid
                  WHERE aa.acttypeid IS NOT NULL
                    AND bb.id IS NULL);

UPDATE actrateexpense a
SET acttypeid=b.id, syschangehistoryid=acttypeid
FROM acttype b
WHERE a.acttypeid = b.syschangehistoryid;

DELETE
FROM actrateexpense a
WHERE a.id IN (
                  SELECT aa.id
                  FROM actrateexpense aa
                  LEFT JOIN acttype bb
                            ON bb.id = aa.acttypeid
                  WHERE aa.acttypeid IS NOT NULL
                    AND bb.id IS NULL);

SET SEARCH_PATH TO demodata;

-- UPDATE crmaddress SET sysChangeHistoryID=NULL;
-- UPDATE crmcontact SET sysChangeHistoryID=NULL;
-- UPDATE crmrelationship SET sysChangeHistoryID=NULL;
-- UPDATE lrinterest SET sysChangeHistoryID=NULL;
-- UPDATE lrplan SET sysChangeHistoryID=NULL;
-- UPDATE lrplanparcel SET sysChangeHistoryID=NULL;
-- UPDATE lrparcel SET sysChangeHistoryID=NULL;
-- UPDATE lrparcelinterest SET sysChangeHistoryID=NULL;
-- UPDATE mtAssessment SET sysChangeHistoryID=NULL;
-- UPDATE mttaxroll SET sysChangeHistoryID=NULL;
-- UPDATE ogwell SET sysChangeHistoryID=NULL;
-- UPDATE ogwellprod SET sysChangeHistoryID=NULL;

DO
$$
    DECLARE
        l_testdatarecords INT := 999;
        l_count           INT := 0;
        l_updatecount     INT;
        l_loopcount       INT := 1;
    BEGIN
        SET client_min_messages = NOTICE;
        UPDATE mtTaxroll
        SET syschangehistoryid=1
            --select * from mttaxroll
        WHERE id < l_testdatarecords + 1
          AND syschangehistoryid IS NULL;

        UPDATE lrInterest
        SET syschangehistoryid=1
            --select * from lrInterest
        WHERE id < l_testdatarecords + 1
          AND syschangehistoryid IS NULL;

        UPDATE lrPlan
        SET syschangehistoryid=1
            --select * from lrInterest
        WHERE id < l_testdatarecords + 1
          AND syschangehistoryid IS NULL;

        UPDATE lrParcel
        SET syschangehistoryid=1
            --select * from lrParcel
        WHERE mtTaxRollID IN (
                                 SELECT id
                                 FROM mtTaxroll a
                                 WHERE a.sysChangeHistoryID = 1)
          AND syschangehistoryid IS NULL;

        UPDATE lrplanparcel
        SET syschangehistoryid=1
            --select * from lrparcelinterest
        WHERE lrplanid IN (
                              SELECT id
                              FROM lrplan aa
                              WHERE aa.syschangehistoryid = 1)
          AND syschangehistoryid IS NULL;

        UPDATE crmcontact a
        SET syschangehistoryid=1
            --The first 400 contacts are needed because
            --they are 20 employees in 20 different systems.
        WHERE (id < 401
            OR (id > 100000 AND id < 100000 + l_testdatarecords)
            OR (id > 104183 AND id < 104183 + l_testdatarecords))
          AND syschangehistoryid IS NULL;

        UPDATE crmrelationship
        SET syschangehistoryid=1
            --select * from crmrelationship
        WHERE (crmcontactid1 IN (
                                    SELECT id
                                    FROM crmContact
                                    WHERE sysChangeHistoryID = 1)
            OR crmcontactid2 IN (
                                    SELECT id
                                    FROM crmContact
                                    WHERE sysChangeHistoryID = 1))
          AND syschangehistoryid IS NULL;

        UPDATE lrparcelinterest
        SET syschangehistoryid=1
            --select * from lrparcelinterest
        WHERE RowIDAppliesTo IN (
                                    SELECT id
                                    FROM lrparcel aa
                                    WHERE aa.syschangehistoryid = 1)
          AND sysdictionarytableidappliesto = 1030000000000002
          AND syschangehistoryid IS NULL;

        UPDATE ogwell
        SET syschangehistoryid=1
            --select * from ogwell
        WHERE (id < l_testdatarecords + 1
            OR lrParcelid IN (
                                 SELECT id
                                 FROM lrparcel aa
                                 WHERE aa.syschangehistoryid = 1)
            OR crmcontactidLicensee IN (
                                           SELECT id
                                           FROM crmContact a
                                           WHERE a.sysChangeHistoryID = 1)
            OR crmcontactidOperator IN (
                                           SELECT id
                                           FROM crmContact a
                                           WHERE a.sysChangeHistoryID = 1)
            )
          AND syschangehistoryid IS NULL;

        LOOP
            UPDATE mtTaxroll
            SET syschangehistoryid=1
                --select * from mttaxroll
            WHERE (id IN (
                             SELECT mttaxrollid
                             FROM lrparcel
                             WHERE sysChangeHistoryID = 1))
              AND syschangehistoryid IS NULL;
            GET DIAGNOSTICS l_UpdateCount = ROW_COUNT; RAISE NOTICE 'mtTaxroll % ', l_updatecount;
            l_Count := l_UpdateCount + l_count;

            UPDATE ogwellprod
            SET syschangehistoryid=1
                --select * from ogwellprod
            WHERE (ogwellid IN (
                                   SELECT id
                                   FROM ogwell aa
                                   WHERE aa.syschangehistoryid = 1))
              AND syschangehistoryid IS NULL;
            GET DIAGNOSTICS l_UpdateCount = ROW_COUNT; RAISE NOTICE 'ogwellprod % ', l_updatecount;
            l_Count := l_UpdateCount + l_count;

            UPDATE lrInterest
            SET syschangehistoryid=1
                --select * from lrInterest
            WHERE (id IN (
                             SELECT lrInterestid
                             FROM lrparcelinterest aa
                             WHERE aa.syschangehistoryid = 1
                             UNION
                             SELECT RowIDAppliesTo
                             FROM lrparcelinterest aa
                             WHERE aa.syschangehistoryid = 1
                               AND aa.sysdictionarytableidappliesto = 1030000000000003))
              AND syschangehistoryid IS NULL;
            GET DIAGNOSTICS l_UpdateCount = ROW_COUNT; RAISE NOTICE 'lrInterest % ', l_updatecount;
            l_Count := l_UpdateCount + l_count;

            UPDATE crmcontact a
            SET syschangehistoryid=1
                --select * from crmcontact a
            WHERE (id IN (
                             SELECT crmcontactid1
                             FROM crmrelationship aa
                             WHERE aa.syschangehistoryid = 1
                             UNION
                             SELECT crmcontactid2
                             FROM crmrelationship aa
                             WHERE aa.syschangehistoryid = 1
                             UNION
                             SELECT crmcontactid
                             FROM mttaxroll aa
                             WHERE aa.syschangehistoryid = 1
                             UNION
                             SELECT bb.crmcontactid2
                             FROM mttaxroll aa
                             JOIN demodata.crmrelationship bb
                                  ON bb.crmcontactid1 = aa.crmcontactid AND bb.crmrelationshiptypeid = 50
                             WHERE aa.syschangehistoryid = 1
                             UNION
                             SELECT crmcontactid
                             FROM lrinterest aa
                             WHERE aa.syschangehistoryid = 1
                             UNION
                             SELECT crmcontactidLicensee
                             FROM ogwell aa
                             WHERE aa.syschangehistoryid = 1
                             UNION
                             SELECT crmcontactidOperator
                             FROM ogwell aa
                             WHERE aa.syschangehistoryid = 1))
              AND syschangehistoryid IS NULL;
            GET DIAGNOSTICS l_UpdateCount = ROW_COUNT; RAISE NOTICE 'crmcontact % ', l_updatecount;
            l_Count := l_UpdateCount + l_count;

            UPDATE lrParcel
            SET syschangehistoryid=1
                --select * from lrParcel
            WHERE (ID IN (
                             SELECT RowIDAppliesTo
                             FROM lrparcelinterest aa
                             WHERE aa.syschangehistoryid = 1
                               AND aa.sysdictionarytableidappliesto = 1030000000000002
                             UNION
                             SELECT lrParcelid
                             FROM ogwell aa
                             WHERE aa.syschangehistoryid = 1)
                OR mtTaxRollID IN (
                                      SELECT id
                                      FROM mtTaxroll a
                                      WHERE a.sysChangeHistoryID = 1)
                )
              AND syschangehistoryid IS NULL;
            GET DIAGNOSTICS l_UpdateCount = ROW_COUNT; RAISE NOTICE 'lrParcel % ', l_updatecount;
            l_Count := l_UpdateCount + l_count;

            UPDATE mtAssessment
            SET syschangehistoryid=1
                --select * from mtAssessment
            WHERE (lrParcelID IN (
                                     SELECT id
                                     FROM lrParcel aa
                                     WHERE aa.syschangehistoryid = 1))
              AND syschangehistoryid IS NULL;
            GET DIAGNOSTICS l_UpdateCount = ROW_COUNT; RAISE NOTICE 'mtAssessment % ', l_updatecount;
            l_Count := l_UpdateCount + l_count;

            UPDATE crmaddress
            SET syschangehistoryid=1
                --select * from crmaddress
            WHERE (crmcontactid IN (
                                       SELECT id
                                       FROM crmcontact aa
                                       WHERE aa.syschangehistoryid = 1))
              AND syschangehistoryid IS NULL;
            GET DIAGNOSTICS l_UpdateCount = ROW_COUNT; RAISE NOTICE 'crmaddress % ', l_updatecount;
            l_Count := l_UpdateCount + l_count;

            l_loopcount := l_loopcount + 1;
            IF l_count = 0 OR l_loopcount > 100
            THEN
                EXIT;
            ELSE
                l_count := 0;
            END IF;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;
/*
SET search_path TO demodata;
select * from (
select 'crmContact' Tablename, count(*) rcount from crmContact where syschangehistoryid=1 union
select 'crmRelationship', count(*) from crmRelationship where syschangehistoryid=1 union
select 'crmAddress', count(*) from crmAddress where syschangehistoryid=1 union
select 'lrinterest', count(*) from lrinterest where syschangehistoryid=1 union
select 'lrplan', count(*) from lrplan where syschangehistoryid=1 union
select 'lrplanparcel', count(*) from lrplanparcel where syschangehistoryid=1 union
select 'lrparcel', count(*) from lrparcel where syschangehistoryid=1 union
select 'lrparcelinterest', count(*) from lrparcel where syschangehistoryid=1 union
select 'mttaxroll', count(*) from mtassessment where syschangehistoryid=1 union
select 'mtassessment', count(*) from mtassessment where syschangehistoryid=1 union
select 'ogwell', count(*) from ogwell where syschangehistoryid=1 union
select 'ogwellprod', count(*) from ogwellprod where syschangehistoryid=1
) a order by tablename;
SET search_path TO DEFAULT;
 */

--select * from actactivity a