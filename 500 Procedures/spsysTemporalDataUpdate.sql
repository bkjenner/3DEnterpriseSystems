DROP PROCEDURE IF EXISTS S0000V0000.spsysTemporalDataUpdate;
CREATE OR REPLACE PROCEDURE S0000V0000.spsysTemporalDataUpdate(INOUT p_Id BIGINT,
                                                               p_EffectiveDate DATE,
                                                               p_NewData json,
                                                               p_Action VARCHAR,
                                                               p_sysDictionaryTableId BIGINT,
                                                               p_OverrideFutureSegments BOOLEAN,
                                                               p_sysChangeHistoryID BIGINT,
                                                               p_debug BOOLEAN DEFAULT FALSE)
AS
$$
DECLARE
    e_Context           TEXT;
    e_Msg               TEXT;
    e_State             TEXT;
    l_Error             RECORD;
    l_sql               VARCHAR;
    l_TableName         VARCHAR;
    l_overridecondition VARCHAR;
    l_columnlist        VARCHAR;
    l_updatelist        VARCHAR;
BEGIN
    /*
    This procedure manages temporal segments based on the parameters that are passed.
    The procedure is based on the following assumptions for temporal data.
    -that referential integrity will be maintained to child records (i.e. why we have a record status)
    -that series will always begin with a date of 1000-01-01 and end with a date of 9999-12-31.
    -if we need a series to be logically not in existence until a particular date then a segment
    will be created at the beginning of the series that replicates the first active segment
    but has a record status of 'd'
    -if a series ends, the last segment in the series will replicate the last active segment
    but will have a record status of 'd'
    -we can always get the most current segment in a series by querying with a temporal end date of
    9999-12-31.  This allows us to maintain referential integrity.
    -it will be allowed to have multiple segments in the middle of a series that have a record status
    of 'd'
    -that we will not have two segments with the same enddate
    -that all segments in the series are contiguous (end date of one segment is one day earlier than
    the start date of the next.
    -if a segment is updated and a future segment exists, it is assumed that the future segment has
    the correct data but the user will be warned this situation exists.
    -that temporal data has different resolutions.  This procedure will support resolutions of a
    year, month and day.

    20210909    Blair Kjenner   Initial Code

    Parameters
    - p_Id INOUT - Gets set to the new id for Inserts.
    - p_EffectiveDate - Effective Date of the change.  This will be rounded to the beginning of the temporal period.
    - p_NewData - New JSON Data (Columns temporalstartdate, temporalenddate, rowstatus, changehistoryid can be passed but will be ignored.)
     Must not be passed for deletes.
    - p_OverrideFutureSegments - TRUE cause info that is passed to override future data if it exists.  False will keep future data.
    - p_Action I - Insert, U - Update, D - Deactivate, A - Activate
    - p_sysChangeHistoryID

do
$temp$
DECLARE l_json json;
BEGIN
SELECT ROW_TO_JSON(a)
--INTO l_json
FROM ( SELECT null compartmentalisation, 1 glratetypeid, 'GST Description' description,  0.05 rate) AS a;

CALL spsysTemporalDataUpdate (p_Id := l_id,
                             p_EffectiveDate := '1999-01-01',
                             p_NewData := l_json,
                             p_Action := 'i',
                             p_sysDictionaryTableId := 396,
                             p_OverrideFutureSegments:=FALSE,
                             p_sysChangeHistoryID := -1);

end;
$temp$ language plpgsql
    */

    p_action := LOWER(p_action);
    SELECT name INTO l_tablename FROM sysdictionarytable WHERE id = p_sysdictionarytableid;

    IF p_action NOT IN ('u', 'd', 'a', 'i')
    THEN
        --Invalid action.  Must be one of the following - u-Update, d-Deactivate, a-Activate, i-Insert
        RAISE SQLSTATE '51054' USING MESSAGE = p_id;
    END IF;

    IF p_NewData IS NOT NULL AND p_Action IN ('d', 'a')
    THEN
        RAISE SQLSTATE '51056' ;
    END IF;

    IF p_action = 'i' AND p_id IS NOT NULL
    THEN
        -- The procedure will not accept p_id as a parameter.  If it is passed
        RAISE SQLSTATE '51057' USING MESSAGE = p_id;
    END IF;

    IF p_action IN ('u', 'd', 'a') AND p_id IS NULL
    THEN
        -- The procedure will not accept p_id as a parameter.  If it is passed
        RAISE SQLSTATE '51063';
    END IF;

    l_overridecondition := 'temporalstartdate ' || CASE WHEN p_OverrideFutureSegments = TRUE THEN '!='
                                                        ELSE '<'
                                                        END || ' l_temporalstartdate';

    IF p_action = 'i'
    THEN
        p_id := fnsysidcreate(p_table:=l_TableName);
    END IF;

    -- Get the columns names
    SELECT STRING_AGG(column_name, ', '), STRING_AGG(column_name || '=b.' || column_name, ', ')
    INTO l_columnlist, l_updatelist
    FROM (
             SELECT column_name
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME ILIKE l_tablename
               AND TABLE_SCHEMA = CURRENT_SCHEMA()
               AND column_name NOT IN ('id', 'syschangehistoryid', 'temporalstartdate', 'temporalenddate', 'rowstatus')
             ORDER BY ordinal_position) AS a;

    -- This temp table allows us to pass parameters from this procedure to the dynamic sql and then
    -- allows allows us to accomplish INOUT (i.e reading/writing to a parameter).  Because it is a temp
    -- we will not have trouble with many different processes creating a table with the same name
    -- The other advantage to doing it in a parameter table is we have a better chance of SQL
    -- creating a compiled query plan for the same tables.
    DROP TABLE IF EXISTS t_spParameters;
    CREATE TEMP TABLE t_spParameters
    AS
    SELECT p_Action,
           p_id,
           p_newData,
           p_EffectiveDate,
           p_OverrideFutureSegments,
           p_sysChangeHistoryID;

    l_sql :=
            REPLACE(
                    REPLACE(
                            REPLACE(
                                                    '
DO
$TEMP$
DECLARE
    e_Context            TEXT;
    e_Msg                TEXT;
    e_State              TEXT;
    l_Error              RECORD;
    l_TemporalStartdate  DATE;
    l_TemporalEnddate    DATE;
    l_TemporalResolution INT;
    l_deletecount        INT;
    l_updatecount        INT;
    l_insertcount        INT;
    l_newdata            JSON;
    p_Action             VARCHAR;
    p_Id                 BIGINT;
    p_NewData            json;
    p_EffectiveDate      DATE;
    p_OverrideFutureSegments BOOLEAN;
    p_sysChangeHistoryID BIGINT;
BEGIN

    Select  *
    INTO
            p_action,
            p_id,
            p_newData,
            p_EffectiveDate,
            p_OverrideFutureSegments,
            p_sysChangeHistoryId
    FROM t_spParameters LIMIT 1;

    l_newdata := CAST(''['' || p_newdata || '']'' AS json);

    l_TemporalResolution := COALESCE(fnsysglobal(''sys-temporalresolution''), 3); --Day
    IF l_TemporalResolution = 1 --Year
    THEN
        l_TemporalStartdate := DATE_TRUNC(''YEAR'', p_effectivedate)::DATE;
        l_TemporalEnddate := (DATE_TRUNC(''year'', p_effectivedate) +
                              INTERVAL ''1 year'' - INTERVAL ''1 day'')::DATE;
    ELSIF l_TemporalResolution = 2 --Month
    THEN
        l_TemporalStartdate := DATE_TRUNC(''MONTH'', p_effectivedate)::DATE;
        l_TemporalEnddate := (DATE_TRUNC(''month'', p_effectivedate) +
                              INTERVAL ''1 month'' - INTERVAL ''1 day'')::DATE;
    ELSIF l_TemporalResolution = 3 --Day
    THEN
        l_TemporalStartdate := p_effectivedate;
        l_TemporalEnddate := p_effectivedate;
    ELSE
        RAISE SQLSTATE ''51055'' USING MESSAGE = l_TemporalResolution;
    END IF;

    IF p_OverrideFutureSegments=TRUE
    THEN
        l_temporalenddate := ''9999-12-31'';
    END IF;

    IF p_action IN (''u'', ''d'', ''a'') AND NOT EXISTS(SELECT FROM l_tablename WHERE id = p_id)
    THEN
        RAISE SQLSTATE ''51051'' USING MESSAGE = p_id;
    END IF;

    IF p_action IN (''u'') AND
       EXISTS(SELECT FROM l_tablename WHERE id = p_id AND temporalstartdate = l_TemporalStartdate AND rowstatus = ''d'')
    THEN
        RAISE SQLSTATE ''51052'' USING MESSAGE = p_id;
    END IF;

    DROP TABLE IF EXISTS t_currdata;
    CREATE TEMP TABLE t_currdata AS
    SELECT *
    FROM l_tablename
    LIMIT 0;
                                                    ' ||
                                                    CASE WHEN p_action = 'i' THEN '
    PERFORM fnsysChangeHistorySetParameters(p_actiontype:=''add''||case when l_temporalstartdate != ''1000-01-01'' then '' effective ''
    ||l_TemporalStartdate::VARCHAR else '''' end, p_sysChangeHistoryIdForDelete:=p_sysChangeHistoryID);

    INSERT INTO t_currdata (
        id, temporalstartdate, temporalenddate, l_columnlist, rowstatus, syschangehistoryid)
    SELECT p_id, l_TemporalStartdate,''9999-12-31''::DATE, l_columnlist, ''a'', p_syschangehistoryid
    FROM JSON_POPULATE_RECORDSET(NULL::l_tablename, l_newdata)
    RETURNING id INTO p_id;
'
                                                         ELSE ''
                                                         END ||
                                                    CASE WHEN p_action = 'u' THEN '
    PERFORM fnsysChangeHistorySetParameters(p_actiontype:=''update effective '' ||l_TemporalStartdate::VARCHAR
     || case when p_OverrideFutureSegments = TRUE then '' (Override future data) '' else '''' end, p_sysChangeHistoryIdForDelete:=p_sysChangeHistoryID);

    -- insert all segments except the one that matches the segment we are updating
    INSERT INTO t_currdata (
        id, temporalstartdate, temporalenddate, l_columnlist, rowstatus, syschangehistoryid)
    SELECT id, temporalstartdate, temporalenddate, l_columnlist, rowstatus, syschangehistoryid
    FROM l_tablename
    WHERE id = p_id
      AND ((p_OverrideFutureSegments=FALSE and temporalstartdate != l_temporalstartdate)
       OR
           (p_OverrideFutureSegments=TRUE  and temporalstartdate < l_temporalstartdate));

    INSERT INTO t_currdata (
        id, temporalstartdate, temporalenddate, l_columnlist, rowstatus, syschangehistoryid)
    SELECT p_id, l_TemporalStartdate, l_TemporalEnddate, l_columnlist, ''a'', p_syschangehistoryid
    FROM JSON_POPULATE_RECORDSET(NULL::l_tablename, l_newdata);
'
                                                         ELSE ''
                                                         END ||
                                                    CASE WHEN p_action IN ('d', 'a') THEN '
    PERFORM fnsysChangeHistorySetParameters(p_actiontype:=case when p_action=''d'' then ''deactivate'' else ''reactivate'' end
     ||'' effective '' ||l_TemporalStartdate::VARCHAR
     || case when p_OverrideFutureSegments = TRUE then '' (Override future data) '' else '''' end, p_sysChangeHistoryIdForDelete:=p_sysChangeHistoryID);

    -- insert all segments except the one that matches the segment we are deleting/deactivating
    INSERT INTO t_currdata (
        id, temporalstartdate, temporalenddate, l_columnlist, rowstatus, syschangehistoryid)
    SELECT id, temporalstartdate, case when temporalenddate = ''9999-12-31'' then dateadd(''d'',-1, l_temporalstartdate) else temporalenddate end, l_columnlist, rowstatus, syschangehistoryid
    FROM l_tablename
    WHERE id = p_id
      AND ((p_OverrideFutureSegments=FALSE and temporalstartdate != l_temporalstartdate)
       OR
           (p_OverrideFutureSegments=TRUE  and temporalstartdate < l_temporalstartdate));

    INSERT INTO t_currdata (
        id, temporalstartdate, temporalenddate, l_columnlist, rowstatus, syschangehistoryid)
    SELECT p_id, l_TemporalStartdate, ''9999-12-31'', l_columnlist, p_action, p_syschangehistoryid
    FROM l_tablename
    WHERE id = p_id
      AND temporalstartdate <= l_temporalstartdate
    ORDER BY temporalstartdate DESC
    LIMIT 1;
'
                                                         ELSE ''
                                                         END || '

--     RAISE NOTICE ''before %'', (
--                                 SELECT JSONB_AGG(ROW_TO_JSON(a))
--                                 FROM (select * from t_currdata order by temporalstartdate) AS a);

    CALL spsysTemporalDataNormalize(p_table := ''t_currdata'', p_syschangehistoryid := p_syschangehistoryid);

--     RAISE NOTICE ''after %'', (
--                                 SELECT JSONB_AGG(ROW_TO_JSON(a))
--                                FROM (select * from t_currdata order by temporalstartdate) AS a);

    DELETE
    FROM l_tablename a
    WHERE a.id = p_id
      AND a.temporalenddate NOT IN (SELECT temporalenddate
                                    FROM t_currdata);
    GET DIAGNOSTICS l_deletecount = ROW_COUNT;

    INSERT INTO l_tablename
    SELECT *
    FROM t_currdata A
    WHERE NOT EXISTS(SELECT FROM l_tablename aa WHERE aa.id = p_id AND aa.temporalenddate = a.temporalenddate);
    GET DIAGNOSTICS l_insertcount = ROW_COUNT;

    UPDATE l_tablename AS a
    SET l_updatelist, syschangehistoryid=p_syschangehistoryid, temporalstartdate = b.temporalstartdate, rowstatus=b.rowstatus
    FROM t_currdata AS b
    WHERE a.id = b.id
      AND a.temporalenddate = b.temporalenddate
      AND (
              SELECT COUNT(*) rcount
              FROM (
                       SELECT l_columnlist, temporalstartdate, rowstatus
                       FROM l_tablename aaa
                       WHERE id = a.id
                         AND aaa.temporalenddate = a.temporalenddate
                       UNION
                       SELECT l_columnlist, temporalstartdate, rowstatus
                       FROM t_currdata aaa
                       WHERE aaa.id = aaa.id
                         AND aaa.temporalenddate = a.temporalenddate
              ) aa) = 2;

    GET DIAGNOSTICS l_updatecount = ROW_COUNT;

    IF l_deletecount + l_updatecount + l_insertcount = 0
    THEN
        --No differences were detected
        RAISE SQLSTATE ''51053'' USING MESSAGE = coalesce(p_id,0);
    END IF;

    --RAISE NOTICE ''l_deletecount % l_updatecount % l_insertcount %'', l_deletecount, l_updatecount, l_insertcount;

	-- Causes parameter table to be dropped.
    PERFORM fnsysChangeHistorySetParameters();

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_state = RETURNED_SQLSTATE,
            e_msg = MESSAGE_TEXT,
            e_context = PG_EXCEPTION_CONTEXT;
        l_error := fnsysError(e_state, e_msg, e_context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION ''%'', l_Error.Message;
        ELSE
            RAISE NOTICE ''%'', l_Error.Message ;
        END IF;
END
$TEMP$
LANGUAGE plpgsql;'
                                , 'l_tablename', l_tablename)
                        , 'l_columnlist', l_columnlist)
                , 'l_updatelist', l_updatelist);

    IF p_debug = TRUE
    THEN
        RAISE NOTICE '%', l_SQL;
    ELSE
        EXECUTE (l_SQL);
    END IF;

    --

    --     DROP TABLE IF EXISTS t_spParameters;
--     DROP TABLE IF EXISTS t_currdata;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_state = RETURNED_SQLSTATE,
            e_msg = MESSAGE_TEXT,
            e_context = PG_EXCEPTION_CONTEXT;
        l_error := fnsysError(e_state, e_msg, e_context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', l_Error.Message;
        ELSE
            RAISE NOTICE '%', l_Error.Message ;
        END IF;
END
$$ LANGUAGE plpgsql;

