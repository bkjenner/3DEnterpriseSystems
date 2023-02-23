SET SEARCH_PATH TO DEFAULT;
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_crmContactID       BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN

        CALL spsysChangeHistoryRefreshTriggers(p_debug := FALSE);
        DELETE FROM exHistory;
        DELETE FROM sysChangeHistoryColumn;
        DELETE FROM sysChangeHistoryRow;
        DELETE FROM syschangehistory;
        DELETE FROM glrate;

        PERFORM SETVAL('exHistory_id_seq', 1);
        PERFORM SETVAL('sysChangeHistory_id_seq', 1);
        PERFORM SETVAL('sysChangeHistoryRow_id_seq', 1);
        PERFORM SETVAL('sysChangeHistoryColumn_id_seq', 1);
        PERFORM SETVAL('glRate_id_seq', 1);

        SELECT MIN(id) INTO l_crmContactID FROM crmcontact;

        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Update contact name and gender and insert a new rate record');
        UPDATE crmcontact
        SET name=name || 'x', crmgenderid=10, sysChangeHistoryID= l_NewChangeHistoryID
        WHERE id = l_crmContactID;

        -- Test Multilink column change
        UPDATE crmaddress
        SET crmcontactid=l_crmContactID, sysChangeHistoryID=l_NewChangeHistoryID
        WHERE ID = (
                       SELECT MAX(id)
                       FROM crmaddress
                       WHERE crmcontactid != l_crmContactID);

        l_id = NULL;
        l_JSON := '{
          "comprovincestateid": null,
          "glratetypeid": 1,
          "description": "Gst description",
          "rate": 0.0500
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '1999-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'i',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);

        CALL spsyschangehistorygenerate();
    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_crmContactID BIGINT;
    BEGIN
        INSERT INTO crmcontact (firstname, lastname, name, rowstatus, syschangehistoryid)
        VALUES ('System', 'Admin', 'System Admin', 'a', fnsysChangeHistoryCreate('Insert a new contact'))
        RETURNING ID INTO l_crmContactID;

        UPDATE crmcontact
        SET rowstatus='d', sysChangeHistoryID=fnsysChangeHistoryCreate('Deactivate a contact')
        WHERE id = l_crmContactID;

        UPDATE crmcontact
        SET rowstatus='a', sysChangeHistoryID=fnsysChangeHistoryCreate('Reactivate a contact')
        WHERE id = l_crmContactID;

        PERFORM fnsysChangeHistorySetParameters(p_sysChangeHistoryIDForDelete := fnsysChangeHistoryCreate('Delete a contact'));
        DELETE FROM crmcontact WHERE id = l_crmContactID;
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;
*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Update first glrate effective 2000-01-01');
        l_JSON := '{
          "comprovincestateid": 30,
          "glratetypeid": 1,
          "description": "Gst description",
          "rate": 0.0510
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2000-01-05',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Update existing segment causing future segments to be overridden');
        l_JSON := '{
          "comprovincestateid": 30,
          "glratetypeid": 1,
          "description": "Gst description",
          "rate": 0.0560
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '1999-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := TRUE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Cause new segment to be created effective 2000-01-01');
        l_JSON := '{
          "comprovincestateid": 30,
          "glratetypeid": 1,
          "description": "Gst description",
          "rate": 0.0510
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2000-01-05',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Cause another new segment to be created as of 2002-02-02');
        l_JSON := '{
          "comprovincestateid": 30,
          "glratetypeid": 1,
          "description": "Gst description",
          "rate": 0.0530
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2002-02-02',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Reactivate first segment');
        l_JSON := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '1000-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'a',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID :=
                fnsysChangeHistoryCreate('Update first segment to same and second.   Causes first and second segment to be consolidated');
        l_JSON := '{
          "comprovincestateid": 30,
          "glratetypeid": 1,
          "description": "Gst description",
          "rate": 0.0560
        }';
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '1000-01-01',
                                     p_NewData := l_json,
                                     p_Action := 'u',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Deactivate middle segment');
        l_JSON := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2000-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Deactivate new segment');
        l_JSON := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2004-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Deactivate causing existing segment to be back dated');
        l_JSON := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2003-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := FALSE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    DECLARE
        l_NewChangeHistoryID BIGINT;
        l_JSON               JSON;
        l_id                 BIGINT;
    BEGIN
        SELECT MAX(id) INTO l_id FROM glrate;
        l_NewChangeHistoryID := fnsysChangeHistoryCreate('Deactivate causing last three segments to be merged');
        l_JSON := NULL;
        CALL spsysTemporalDataUpdate(p_Id := l_id,
                                     p_EffectiveDate := '2000-01-01',
                                     p_NewData := l_JSON,
                                     p_Action := 'd',
                                     p_sysDictionaryTableId := 396,
                                     p_OverrideFutureSegments := TRUE,
                                     p_sysChangeHistoryID := l_NewChangeHistoryID);
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

*/
DO
$$
    BEGIN
        UPDATE crmContact
        SET name=name,
            sysChangeHistoryID=fnsysChangeHistoryCreate('Test Max Change History Records Ignored - All records');
        CALL spsyschangehistorygenerate();

    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

*/
DO
$$
    BEGIN
        UPDATE crmContact a
        SET name=name || 'x',
            sysChangeHistoryID=fnsysChangeHistoryCreate('Test Max Change History Records Overridden - 15 records')
        FROM (
                 SELECT id
                 FROM crmContact
                 LIMIT 15) b
        WHERE a.id = b.id;
        CALL spsyschangehistorygenerate();
    END;
$$ LANGUAGE plpgsql;
/*
select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;
*/
DO
$$
    DECLARE
        l_Rec                   RECORD;
        l_sysChangeHistoryIDNew BIGINT;
    BEGIN
        -- Undo all change history events that were done
        FOR l_Rec IN
            SELECT a.id sysChangeHistoryIDUndo, a.crmcontactiduser, a.sysdictionarytableidappliesto, a.Rowidappliesto
            FROM sysChangeHistory a
            LEFT JOIN sysChangeHistory b
                      ON b.sysChangeHistoryIDUndo = a.id
            WHERE a.sysChangeHistoryIDUndo IS NULL
              AND b.id IS NULL
              AND EXISTS(SELECT FROM syschangehistoryrow aa WHERE aa.syschangehistoryid = a.id)
            ORDER BY a.ID DESC
        LOOP
            INSERT INTO syschangehistory (
                crmcontactiduser,
                changedate,
                comments,
                rowstatus,
                sysdictionarytableidappliesto,
                Rowidappliesto,
                sysChangeHistoryIDUndo)
            SELECT a.crmcontactiduser,
                   NOW()::TIMESTAMP,
                   'Undo - ' || a.comments,
                   'a',
                   a.sysdictionarytableidappliesto,
                   a.Rowidappliesto,
                   a.id
            FROM sysChangeHistory A
            WHERE a.id = l_Rec.sysChangeHistoryIDUndo
            RETURNING ID INTO l_sysChangeHistoryIDNew;

            CALL spsysChangeHistoryUndo(p_sysChangeHistoryIDUndo := l_Rec.sysChangeHistoryIDUndo,
                                        p_sysChangeHistoryIDNew := l_sysChangeHistoryIDNew);
        END LOOP;
        CALL spsyschangehistorygenerate();

    END
$$
/*

call spsyschangehistorygenerate();

select e.id CHId, c.name ||'.'|| b.name as ColumnName, d.Rowidappliesto rowid, b.purpose, e.comments, d.actiontype, d.OperationType, a.TranslatedDataBefore, a.TranslatedDataAfter
from sysChangeHistoryColumn a
join sysdictionarycolumn b on b.id=a.sysdictionarycolumnid
join sysdictionarytable c on c.id=b.sysdictionarytableid
join sysChangeHistoryRow d on d.id=a.sysChangeHistoryRowid
join sysChangeHistory e on e.id=d.sysChangeHistoryid
order by e.id, d.id, b.ID;

select s.id, s.comments, aa.* from glrate aa
join syschangehistory s ON aa.syschangehistoryid = s.id
order by temporalstartdate;

 */