CREATE OR REPLACE FUNCTION S0000V0000.fnsysMDSExecute(p_command TEXT)
    RETURNS TEXT
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
This procedure executes a command on the AWS server

CHANGE LOG
20211024 Blair Kjenner	Initial Code

PARAMETERS
p_command - text

SAMPLE CALL

select fnsysMDSExecute ('update s0000v0000.exsystem set name=name;');

*/
DECLARE
    e_Context          TEXT;
    e_Msg              TEXT;
    e_State            TEXT;
    l_Error            RECORD;
    l_return           TEXT;
BEGIN
    PERFORM fnsysMDSconnect();
    SELECT DBLINK_EXEC('aws', p_command) INTO l_return;
    IF l_return is NULL
    THEN
         RAISE SQLSTATE '51068';
    END IF;

    RETURN l_RETURN;

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
        RETURN NULL;
END ;
$$ LANGUAGE plpgsql


