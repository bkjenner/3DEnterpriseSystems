DROP FUNCTION IF EXISTS S0000V0000.fnsysExecSQL (p_sql TEXT) CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.fnsysExecSQL(p_sql TEXT) RETURNS TEXT
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

select fnsysExecSQL('update crmaddress set crmaddresstypeid=99 where id=10000000149656 ;');

*/
DECLARE
    e_Context TEXT;
    e_Msg     TEXT;
    e_State   TEXT;
BEGIN
    EXECUTE (p_SQL);
    RETURN NULL;
EXCEPTION

    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_msg = MESSAGE_TEXT, e_state = RETURNED_SQLSTATE;
        RETURN FORMAT('%s (%s)', e_msg, e_state);
END;
$$ LANGUAGE plpgsql;

