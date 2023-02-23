CREATE OR REPLACE PROCEDURE S0000V0000.spexSubscriptionRedact()
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

OVERVIEW
This procedure is called from the spexPackageExport.  It updates t_syschangehistoryrow and t_syschangehistorycolumn
and redacts any columns necessary

CHANGE LOG
20220828 Blair Kjenner	Initial Code

SAMPLE CALL

call spexSubscriptionRedact();

*/
DECLARE
    e_Context TEXT;
    e_Msg     TEXT;
    e_State   TEXT;
    l_Error   RECORD;
    l_Rec     RECORD;
    l_sql     VARCHAR := '';
BEGIN

    FOR l_Rec IN
        SELECT DISTINCT f.id                                                                    sysdictionarytableid,
                        LOWER(f.name)                                                           tablename,
                        e.id                                                                    sysdictionarycolumnid,
                        LOWER(e.name)                                                           columnname,
                        COALESCE('''' || d.redactedvalue || '''', 'null')                       redactedvalue,
                        COALESCE('''' || g.quote || d.redactedvalue || g.quote || '''', 'null') redactedvalueinquotes,
                        COALESCE('''' || d.redactedtranslation || '''', 'null')                 redactedtranslation
        FROM t_exhistory b
        JOIN exsubscriptiondetail c
             ON c.id = exsubscriptiondetailid
        JOIN exsubscriptionredaction d
             ON d.exsubscriptionid = c.exsubscriptionid
        JOIN sysdictionarycolumn e
             ON e.id = d.sysdictionarycolumnidredacted
        JOIN sysdictionarytable f
             ON f.id = e.sysdictionarytableid
        JOIN LATERAL (SELECT CASE WHEN LOWER(e.datatype) IN ('varchar', 'varchar2', 'nvarchar', 'char', 'nchar')
                                      THEN '"'
                                  ELSE ''
                                  END quote) g
             ON TRUE
    LOOP
        -- The following update statement will loop through all tables where new
        -- subscriptiondetail were created, and update the sysChangeHistoryID to
        -- the changehistoryid parameter.  This will cause the trigger to get
        -- fired which will create changehistoryrow records which we will be
        -- exporting.
        l_SQL := l_SQL || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('
        UPDATE t_syschangehistoryrow
        SET newdata = JSONB_SET(newdata, ''{l_columnname}'', l_redactedvalueinquotes, FALSE)
        WHERE sysdictionarytableidappliesto = l_sysdictionarytableid
          AND newdata IS NOT NULL;

        UPDATE t_syschangehistoryrow
        SET olddata = JSONB_SET(olddata, ''{l_columnname}'', l_redactedvalueinquotes, FALSE)
        WHERE sysdictionarytableidappliesto = l_sysdictionarytableid
          AND olddata IS NOT NULL;

        UPDATE t_syschangehistorycolumn
        SET rawdatabefore=l_redactedvalue, translateddatabefore=l_redactedtranslation
        WHERE sysdictionarycolumnid = l_sysdictionarycolumnid
          AND rawdatabefore IS NOT NULL;

        UPDATE t_syschangehistorycolumn
        SET rawdataafter=l_redactedvalue, translateddataafter=l_redactedtranslation
        WHERE sysdictionarycolumnid = l_sysdictionarycolumnid
          AND rawdataafter IS NOT NULL;
          '
                                                                      , 'l_columnname', l_rec.columnname)
                                                              , 'l_redactedvalueinquotes', l_rec.redactedvalueinquotes)
                                                      , 'l_redactedvalue', l_rec.redactedvalue)
                                              , 'l_redactedtranslation', l_rec.redactedtranslation)
                                      , 'l_sysdictionarytableid', l_rec.sysdictionarytableid::VARCHAR)
            , 'l_sysdictionarycolumnid', l_rec.sysdictionarycolumnid::VARCHAR);

    END LOOP;

    --RAISE NOTICE 'SQL %', l_SQL;

    IF l_sql IS NOT NULL
    THEN
        EXECUTE (l_SQL);
    END IF;

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

END ;
$$ LANGUAGE plpgsql

