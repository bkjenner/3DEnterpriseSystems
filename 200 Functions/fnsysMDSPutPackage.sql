CREATE OR REPLACE FUNCTION S0000V0000.fnsysMDSPutPackage(p_exSubnetServerID BIGINT, p_package TEXT)
    RETURNS BIGINT
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
This procedure communicates with the Master subnet server to get the next package for a given subnet server

CHANGE LOG
20211023 Blair Kjenner	Initial Code

PARAMETERS
p_exSubnetServerid - subnet server to send data to
p_package - Text Package to be send

SAMPLE CALL

select fnsysMDSPutPackage (1, 'sample text');

*/
DECLARE
    e_Context TEXT;
    e_Msg     TEXT;
    e_State   TEXT;
    l_Error   RECORD;
    l_sql     TEXT;
    l_return  BIGINT;
BEGIN
    l_sql := FORMAT('
        INSERT INTO s0000v0000.exPackage(exSubnetServerid, package, readdate, createdate)
        VALUES (%s::bigint, ''%s''::text, null, now()::timestamp);
        ', p_exSubnetServerID, fixquote(p_Package));
    PERFORM fnsysMDSExecute(l_sql);

    SELECT MAX(id)
    INTO l_return
    FROM vwexPackageAWS;
    RETURN l_return;

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


