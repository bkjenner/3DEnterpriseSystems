DO
$$
    DECLARE
        l_NewChangeHistoryID INT;
        l_ID                 BIGINT;
        l_JSON               JSON;
        l_SQL                VARCHAR;
    BEGIN

        SELECT STRING_AGG('Drop function if exists ' || routine_name || ' cascade;','')
        INTO l_SQL
        FROM information_schema.routines
        WHERE routine_name ILIKE 'fnCH%CH';

        EXECUTE l_sql;

        DELETE FROM sysChangeHistoryColumn;
        DELETE FROM sysChangeHistoryRow;
        DELETE FROM syschangehistory;
        DELETE FROM glrate;

        call spsysChangeHistoryRefreshTriggers ();

        PERFORM SETVAL('sysChangeHistory_id_seq', 1);
        PERFORM SETVAL('sysChangeHistoryRow_id_seq', 1);
        PERFORM SETVAL('sysChangeHistoryColumn_id_seq', 1);
        PERFORM SETVAL('glRate_id_seq', 1);

--         INSERT INTO syschangehistory (
--             crmcontactiduser, changedate, comments, rowstatus, sysdictionarytableidappliesto, Rowidappliesto)
--         SELECT 1, NOW()::TIMESTAMP, 'Insert error - id not allowed', 'a', 396, 1
--         RETURNING ID INTO l_NewChangeHistoryID;

-- 		l_id=1;
--         l_JSON := '{"temporalstartdate":"1000-01-01","temporalenddate":"2021-08-31","comprovincestateid":null,"glratetypeid":1,"description":"Gst description","rate":0.0500}';
--         CALL spsysTemporalDataUpdate (p_Id := l_id,
--                                      p_EffectiveDate := '1000-01-01',
--                                      p_NewData := l_json,
--                                      p_Action := 'i',
--                                      p_sysDictionaryTableId := 396,
--                                      p_OverrideFutureSegments:=FALSE,
--                                      p_sysChangeHistoryID :=fnsysChangeHistoryCreate(''));

		l_id=null;
        l_JSON := '{"comprovincestateid":null,"glratetypeid":1,"description":"Gst description","rate":0.0500}';
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '1999-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Insert first glrate'));


        l_JSON := '{"comprovincestateid":30,"glratetypeid":1,"description":"Gst description","rate":0.0510}';
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '2000-01-05',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Update first glrate effective 2000-01-01'));

        l_JSON := '{"comprovincestateid":30,"glratetypeid":1,"description":"Gst description","rate":0.0560}';
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '1999-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=TRUE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Update existing segment causing future segments to be overridden'));

        l_JSON := '{"comprovincestateid":30,"glratetypeid":1,"description":"Gst description","rate":0.0510}';
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '2000-01-05',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Cause new segment to be created effective 2000-01-01'));

        l_JSON := '{"comprovincestateid":30,"glratetypeid":1,"description":"Gst description","rate":0.0530}';
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '2002-02-02',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Cause another new segment to be created as of 2002-02-02'));

        l_JSON := null;
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '1000-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'a',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Reactivate first segment'));

        l_JSON := '{"comprovincestateid":30,"glratetypeid":1,"description":"Gst description","rate":0.0560}';
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '1000-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Update first segment to same and second.   Causes first and second segment to be consolidated'));

        l_JSON := null;
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '2000-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Deactivate middle segment'));

        l_JSON := null;
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '2004-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Deactivate new segment'));

        l_JSON := null;
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '2003-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=FALSE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Deactivate causing existing segment to be back dated'));

        l_JSON := null;
        CALL spsysTemporalDataUpdate (p_Id := l_id,
                                     p_EffectiveDate := '2000-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments:=TRUE,
                                     p_sysChangeHistoryID :=fnsysChangeHistoryCreate('Deactivate causing last three segments to be merged'));


    END
$$
/*

update sysmessage set isexceptionraised=false where id>54

select * from syschangehistoryrow

select * from (
select 'db' source, * from glrate
union
select 'temp' source, * from t_currdata
) a
order by source, temporalstartdate

*/