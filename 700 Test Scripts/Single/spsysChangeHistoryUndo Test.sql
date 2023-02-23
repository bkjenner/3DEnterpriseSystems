
DO
$$
    DECLARE
        l_sysChangeHistoryIDNew BIGINT;
        l_sysChangeHistoryIDUndo BIGINT;
    BEGIN
        INSERT INTO syschangehistory (crmcontactiduser, changedate, comments, rowstatus, sysdictionarytableidappliesto, Rowidappliesto, sysChangeHistoryIDUndo)
        SELECT a.crmcontactiduser, NOW()::TIMESTAMP, 'Undo the last change recorded that is not an Undo', 'a', a.sysdictionarytableidappliesto, a.Rowidappliesto, a.id
        FROM sysChangeHistory a
        LEFT JOIN sysChangeHistory b on b.sysChangeHistoryIDUndo=a.id
        WHERE a.sysChangeHistoryIDUndo is null
        AND b.id is null
        ORDER BY ID desc
        LIMIT 1
        RETURNING ID, sysChangeHistoryIDUndo INTO l_sysChangeHistoryIDNew, l_sysChangeHistoryIDUndo;

        call spsysChangeHistoryUndo(p_sysChangeHistoryIDUndo:=l_sysChangeHistoryIDUndo, p_sysChangeHistoryIDNew:=l_sysChangeHistoryIDNew);
    END
$$
