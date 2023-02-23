SET SEARCH_PATH TO s0111v0000, s0000v0000, public;
SET client_min_messages TO notice;

select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Penny', p_level:=1);
select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Penny', p_level:=1, p_querydate:='2022-12-03');

select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Jimmy', p_level:=1);
select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Jimmy', p_level:=1, p_querydate:='2022-12-03');

select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Penny', p_level:=4, p_querydate:='2022-01-01');
select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert, Jimmy', p_level:=1);

select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert,%', p_level:=3);
select fnsysMasterDataQuery (p_table:='crmcontact', p_queryfilter:='Aabert%', p_level:=3, p_foreigntable:='og%');


select fnsysMasterDataQuery (p_table:='crmcontact', p_whereclause:='a.lastname ilike ''aab%'' and year(a.birthdate) > 1970', p_level:=1);
select fnsysMasterDataQuery (p_table:='lrparcel', p_whereclause:='a.acresunbroken < 10', p_level:=1);
select fnsysMasterDataQuery (p_queryfilter:='SE-18-30-025-04-8', p_level:=3);


-- Check out parcel plans
select fnsysMasterDataQuery (p_queryfilter:='103-1-1', p_level:=3);
--copy paste the ids into a list
select fnsysMasterDataQuery (p_table:='lrparcel', p_whereclause:='a.id in (select lrparcelid from lrplanparcel aa where lrplanid=fnsysidget(103,1))', p_level:=3);
select fnsysMasterDataQuery (p_queryfilter:='103-001-01-01', p_level:=3);
-- grab data on encana
select fnsysMasterDataQuery (p_queryfilter:='Encana Infrastructure Resources%', p_level:=3);
select fnsysMasterDataQuery (p_table:='ogwell', p_whereclause:='a.id in (1040000000000881,1040000000000161,1040000000000150,1040000000000245,1040000000000049,1040000000008547,1040000000000164,1040000000000166,1040000000000159,1040000000000758,1040000000004955,1040000000000001,1040000000000246,1040000000000149)', p_level:=3);
select fnsysMasterDataQuery (p_table:='actproject', p_whereclause:='a.id=1090000000001199', p_level:=3);
select fnsysMasterDataQuery (p_table:='actproject', p_whereclause:='a.contact ilike ''Saif Hlovyak''', p_level:=3);

--Find the record with the most number of child tables
SELECT rowidmaster, COUNT(*)
FROM (
         SELECT DISTINCT rowidmaster, sysdictionarytableidforeign
         FROM sysmasterdataindex
         WHERE sysdictionarytableidmaster = 401) a
GROUP BY rowidmaster
ORDER BY COUNT(*) desc, ROWIDmaster

select * from sysdictionarytable where name ilike 'actproject'


select * from actproject where
select * from actproject where id = 1090000000001161

select actprojectidparent from actproject where actprojectidparent is not NULL order by actprojectidparent)





select rowidchargedto, count(*) from glentry where sysdictionarytableidchargedto=200 group by rowidchargedto order by count(*) DESC



INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                  sysDictionaryTableIDMaster,
                LOWER(dd.name)                         TableNameMaster,
                dd.pluralname                          TableFriendlyNameMaster,
                jl1.temporalcondition                  TemporalConditionMaster,
                cc.id                                  sysDictionaryTableIdForeign,
                LOWER(cc.name)                         TableNameForeign,
                '' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                  TemporalConditionForeign,
                LOWER(bb.name)                         ColumnNameForeign,
                LOWER(bb.purpose)                      PurposeForeign,
                aa.RowIdForeign                        RowIDForeign,
                aa.RowIdMaster                         RowidMaster,
                1                                      DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE
JOIN      vwcrmcontact a
          ON a.id = aa.RowIDMaster
WHERE ('crmcontact' IS NULL OR dd.name ILIKE 'crmcontact')
  AND ('Aabert%' IS NULL OR aa.foreignkeytranslation ILIKE 'Aabert%')
  AND (FALSE = FALSE OR aa.RowIdMaster IN (
                                              SELECT aaa.RowidForeign
                                              FROM t_foreignkeyreferences aaa
                                              WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                              sysDictionaryTableIDMaster,
                LOWER(dd.name)                                     TableNameMaster,
                dd.pluralname                                      TableFriendlyNameMaster,
                jl1.temporalcondition                              TemporalConditionMaster,
                cc.id                                              sysDictionaryTableIdForeign,
                LOWER(cc.name)                                     TableNameForeign,
                'Activities->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                              TemporalConditionForeign,
                LOWER(bb.name)                                     ColumnNameForeign,
                LOWER(bb.purpose)                                  PurposeForeign,
                aa.RowIdForeign                                    RowIDForeign,
                aa.RowIdMaster                                     RowidMaster,
                2                                                  DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('actactivity' IS NULL OR dd.name ILIKE 'actactivity')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                 sysDictionaryTableIDMaster,
                LOWER(dd.name)                                        TableNameMaster,
                dd.pluralname                                         TableFriendlyNameMaster,
                jl1.temporalcondition                                 TemporalConditionMaster,
                cc.id                                                 sysDictionaryTableIdForeign,
                LOWER(cc.name)                                        TableNameForeign,
                'Billing Rates->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                 TemporalConditionForeign,
                LOWER(bb.name)                                        ColumnNameForeign,
                LOWER(bb.purpose)                                     PurposeForeign,
                aa.RowIdForeign                                       RowIDForeign,
                aa.RowIdMaster                                        RowidMaster,
                2                                                     DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('actratebilling' IS NULL OR dd.name ILIKE 'actratebilling')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                             sysDictionaryTableIDMaster,
                LOWER(dd.name)                                    TableNameMaster,
                dd.pluralname                                     TableFriendlyNameMaster,
                jl1.temporalcondition                             TemporalConditionMaster,
                cc.id                                             sysDictionaryTableIdForeign,
                LOWER(cc.name)                                    TableNameForeign,
                'Addresses->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                             TemporalConditionForeign,
                LOWER(bb.name)                                    ColumnNameForeign,
                LOWER(bb.purpose)                                 PurposeForeign,
                aa.RowIdForeign                                   RowIDForeign,
                aa.RowIdMaster                                    RowidMaster,
                2                                                 DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('crmaddress' IS NULL OR dd.name ILIKE 'crmaddress')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                    sysDictionaryTableIDMaster,
                LOWER(dd.name)                                           TableNameMaster,
                dd.pluralname                                            TableFriendlyNameMaster,
                jl1.temporalcondition                                    TemporalConditionMaster,
                cc.id                                                    sysDictionaryTableIdForeign,
                LOWER(cc.name)                                           TableNameForeign,
                'Employee Details->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                    TemporalConditionForeign,
                LOWER(bb.name)                                           ColumnNameForeign,
                LOWER(bb.purpose)                                        PurposeForeign,
                aa.RowIdForeign                                          RowIDForeign,
                aa.RowIdMaster                                           RowidMaster,
                2                                                        DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('crmcontactsubtypeemployee' IS NULL OR dd.name ILIKE 'crmcontactsubtypeemployee')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                 sysDictionaryTableIDMaster,
                LOWER(dd.name)                                        TableNameMaster,
                dd.pluralname                                         TableFriendlyNameMaster,
                jl1.temporalcondition                                 TemporalConditionMaster,
                cc.id                                                 sysDictionaryTableIdForeign,
                LOWER(cc.name)                                        TableNameForeign,
                'Relationships->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                 TemporalConditionForeign,
                LOWER(bb.name)                                        ColumnNameForeign,
                LOWER(bb.purpose)                                     PurposeForeign,
                aa.RowIdForeign                                       RowIDForeign,
                aa.RowIdMaster                                        RowidMaster,
                2                                                     DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('crmrelationship' IS NULL OR dd.name ILIKE 'crmrelationship')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                    sysDictionaryTableIDMaster,
                LOWER(dd.name)                                           TableNameMaster,
                dd.pluralname                                            TableFriendlyNameMaster,
                jl1.temporalcondition                                    TemporalConditionMaster,
                cc.id                                                    sysDictionaryTableIdForeign,
                LOWER(cc.name)                                           TableNameForeign,
                'Location History->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                    TemporalConditionForeign,
                LOWER(bb.name)                                           ColumnNameForeign,
                LOWER(bb.purpose)                                        PurposeForeign,
                aa.RowIdForeign                                          RowIDForeign,
                aa.RowIdMaster                                           RowidMaster,
                2                                                        DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('falocationhistory' IS NULL OR dd.name ILIKE 'falocationhistory')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                    sysDictionaryTableIDMaster,
                LOWER(dd.name)                                           TableNameMaster,
                dd.pluralname                                            TableFriendlyNameMaster,
                jl1.temporalcondition                                    TemporalConditionMaster,
                cc.id                                                    sysDictionaryTableIdForeign,
                LOWER(cc.name)                                           TableNameForeign,
                'Billing Accounts->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                    TemporalConditionForeign,
                LOWER(bb.name)                                           ColumnNameForeign,
                LOWER(bb.purpose)                                        PurposeForeign,
                aa.RowIdForeign                                          RowIDForeign,
                aa.RowIdMaster                                           RowidMaster,
                2                                                        DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('glbillingaccount' IS NULL OR dd.name ILIKE 'glbillingaccount')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                           sysDictionaryTableIDMaster,
                LOWER(dd.name)                                  TableNameMaster,
                dd.pluralname                                   TableFriendlyNameMaster,
                jl1.temporalcondition                           TemporalConditionMaster,
                cc.id                                           sysDictionaryTableIdForeign,
                LOWER(cc.name)                                  TableNameForeign,
                'Entries->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                           TemporalConditionForeign,
                LOWER(bb.name)                                  ColumnNameForeign,
                LOWER(bb.purpose)                               PurposeForeign,
                aa.RowIdForeign                                 RowIDForeign,
                aa.RowIdMaster                                  RowidMaster,
                2                                               DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('glentry' IS NULL OR dd.name ILIKE 'glentry')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                        sysDictionaryTableIDMaster,
                LOWER(dd.name)                                               TableNameMaster,
                dd.pluralname                                                TableFriendlyNameMaster,
                jl1.temporalcondition                                        TemporalConditionMaster,
                cc.id                                                        sysDictionaryTableIdForeign,
                LOWER(cc.name)                                               TableNameForeign,
                'Employment Positions->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                        TemporalConditionForeign,
                LOWER(bb.name)                                               ColumnNameForeign,
                LOWER(bb.purpose)                                            PurposeForeign,
                aa.RowIdForeign                                              RowIDForeign,
                aa.RowIdMaster                                               RowidMaster,
                2                                                            DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('hrposition' IS NULL OR dd.name ILIKE 'hrposition')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                             sysDictionaryTableIDMaster,
                LOWER(dd.name)                                    TableNameMaster,
                dd.pluralname                                     TableFriendlyNameMaster,
                jl1.temporalcondition                             TemporalConditionMaster,
                cc.id                                             sysDictionaryTableIdForeign,
                LOWER(cc.name)                                    TableNameForeign,
                'Interests->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                             TemporalConditionForeign,
                LOWER(bb.name)                                    ColumnNameForeign,
                LOWER(bb.purpose)                                 PurposeForeign,
                aa.RowIdForeign                                   RowIDForeign,
                aa.RowIdMaster                                    RowidMaster,
                2                                                 DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('lrinterest' IS NULL OR dd.name ILIKE 'lrinterest')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                        sysDictionaryTableIDMaster,
                LOWER(dd.name)                                               TableNameMaster,
                dd.pluralname                                                TableFriendlyNameMaster,
                jl1.temporalcondition                                        TemporalConditionMaster,
                cc.id                                                        sysDictionaryTableIdForeign,
                LOWER(cc.name)                                               TableNameForeign,
                'Parcel Title Holders->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                        TemporalConditionForeign,
                LOWER(bb.name)                                               ColumnNameForeign,
                LOWER(bb.purpose)                                            PurposeForeign,
                aa.RowIdForeign                                              RowIDForeign,
                aa.RowIdMaster                                               RowidMaster,
                2                                                            DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('lrparceltitleholder' IS NULL OR dd.name ILIKE 'lrparceltitleholder')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                         sysDictionaryTableIDMaster,
                LOWER(dd.name)                                TableNameMaster,
                dd.pluralname                                 TableFriendlyNameMaster,
                jl1.temporalcondition                         TemporalConditionMaster,
                cc.id                                         sysDictionaryTableIdForeign,
                LOWER(cc.name)                                TableNameForeign,
                'Plans->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                         TemporalConditionForeign,
                LOWER(bb.name)                                ColumnNameForeign,
                LOWER(bb.purpose)                             PurposeForeign,
                aa.RowIdForeign                               RowIDForeign,
                aa.RowIdMaster                                RowidMaster,
                2                                             DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('lrplan' IS NULL OR dd.name ILIKE 'lrplan')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                            sysDictionaryTableIDMaster,
                LOWER(dd.name)                                   TableNameMaster,
                dd.pluralname                                    TableFriendlyNameMaster,
                jl1.temporalcondition                            TemporalConditionMaster,
                cc.id                                            sysDictionaryTableIdForeign,
                LOWER(cc.name)                                   TableNameForeign,
                'Taxrolls->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                            TemporalConditionForeign,
                LOWER(bb.name)                                   ColumnNameForeign,
                LOWER(bb.purpose)                                PurposeForeign,
                aa.RowIdForeign                                  RowIDForeign,
                aa.RowIdMaster                                   RowidMaster,
                2                                                DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('mttaxroll' IS NULL OR dd.name ILIKE 'mttaxroll')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                         sysDictionaryTableIDMaster,
                LOWER(dd.name)                                TableNameMaster,
                dd.pluralname                                 TableFriendlyNameMaster,
                jl1.temporalcondition                         TemporalConditionMaster,
                cc.id                                         sysDictionaryTableIdForeign,
                LOWER(cc.name)                                TableNameForeign,
                'Wells->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                         TemporalConditionForeign,
                LOWER(bb.name)                                ColumnNameForeign,
                LOWER(bb.purpose)                             PurposeForeign,
                aa.RowIdForeign                               RowIDForeign,
                aa.RowIdMaster                                RowidMaster,
                2                                             DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('ogwell' IS NULL OR dd.name ILIKE 'ogwell')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
INSERT INTO t_foreignkeyreferences (sysDictionaryTableIDMaster, TableNameMaster, TableFriendlyNameMaster, TemporalConditionMaster, sysDictionaryTableIdForeign, TableNameForeign, FriendlyNameForeign, TemporalConditionForeign, ColumnNameForeign, PurposeForeign, RowIDForeign, RowidMaster, datalevel)
SELECT DISTINCT dd.id                                                       sysDictionaryTableIDMaster,
                LOWER(dd.name)                                              TableNameMaster,
                dd.pluralname                                               TableFriendlyNameMaster,
                jl1.temporalcondition                                       TemporalConditionMaster,
                cc.id                                                       sysDictionaryTableIdForeign,
                LOWER(cc.name)                                              TableNameForeign,
                'Registered Vehicles->' || COALESCE(cc.pluralname, cc.name) FriendlyNameForeign,
                jl0.temporalcondition                                       TemporalConditionForeign,
                LOWER(bb.name)                                              ColumnNameForeign,
                LOWER(bb.purpose)                                           PurposeForeign,
                aa.RowIdForeign                                             RowIDForeign,
                aa.RowIdMaster                                              RowidMaster,
                2                                                           DataLevel
FROM sysmasterdataindex aa
LEFT JOIN sysdictionarycolumn bb
          ON bb.id = aa.sysdictionarycolumnidforeign
LEFT JOIN sysdictionarytable cc
          ON cc.id = bb.sysdictionarytableid
JOIN      sysdictionarytable dd
          ON dd.id = aa.sysdictionarytableidmaster
JOIN      LATERAL (SELECT CASE WHEN cc.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL0
          ON TRUE
JOIN      LATERAL (SELECT CASE WHEN dd.istabletemporal
                                   THEN ' AND temporalstartdate <= ''' || '2022-11-15'
                                            || '''::DATE AND temporalenddate >= '''
                                            || '2022-11-15' || '''::DATE'
                               ELSE ''
                               END temporalcondition) JL1
          ON TRUE

WHERE ('vrvehicle' IS NULL OR dd.name ILIKE 'vrvehicle')
  AND (NULL IS NULL OR aa.foreignkeytranslation ILIKE NULL)
  AND (FALSE = TRUE OR aa.RowIdMaster IN (
                                             SELECT aaa.RowidForeign
                                             FROM t_foreignkeyreferences aaa
                                             WHERE aaa.TableNameForeign = dd.name))
  AND (NULL IS NULL OR cc.name ILIKE NULL)
;
<NULL>