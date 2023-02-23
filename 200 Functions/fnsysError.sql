DROP FUNCTION IF EXISTS S0000V0000.fnsysError (p_state TEXT, p_msg TEXT, p_context TEXT) CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnsysError(p_state TEXT, p_msg TEXT DEFAULT '', p_context TEXT DEFAULT '') RETURNS RECORD

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
This function is passed error information and formulates an error message
based on information in the sysMessage table.

20210302    Blair Kjenner   Initial Code

l_Error := fnsysError(e_State, e_Msg, e_Context);

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_State = RETURNED_SQLSTATE,
            e_Msg = MESSAGE_TEXT,
            e_Context = PG_EXCEPTION_CONTEXT;
        l_Error := fnsysError(e_State, e_Msg, e_Context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', l_Error.Message;
        ELSE
            RAISE NOTICE '%', l_Error.Message;
        END IF;

*/
DECLARE
    row       RECORD;
    l_message TEXT;
    l_return  RECORD;
    l_isExceptionRaised BOOLEAN = TRUE;
BEGIN
    IF p_state IN (
                      SELECT STATE
                      FROM sysmessage)
    THEN
        SELECT * INTO row FROM sysmessage WHERE state = p_state;
        l_isExceptionRaised := row.isexceptionraised;
        p_msg := COALESCE(p_msg, '');
        l_message := '
            Validation:
            state  : ' || row.state || '
            message: ' || REPLACE(row.description, '%', p_msg) || '
            detail : ' || COALESCE(row.detail, '') || '
            hint   : ' || COALESCE(row.hint, '') || '
            context: ' || COALESCE(p_context, '');
        IF row.ismessagelogged = TRUE
        THEN
            INSERT INTO syserror (context, message, timestamp, state)
            VALUES (p_context, row.description, NOW()::TIMESTAMP, row.state);
        END IF;
    ELSE
        l_message := '
            Got exception:
            state  : ' || p_state || '
            message: ' || p_msg || '
            context: ' || p_context;
        --TODO Writing to syserror will have no effect when we raise the exception when we return
        INSERT INTO syserror (context, message, timestamp, state)
        VALUES (p_context, p_msg, NOW()::TIMESTAMP, p_state);
    END IF;
    --      I can write errors to table or notify someone if I wish
--         insert into errors (state) values (l_return);
    SELECT l_IsExceptionRaised as IsExceptionRaised, l_message as Message  INTO l_return;
    RETURN l_return;
END;
$$ LANGUAGE plpgsql;

