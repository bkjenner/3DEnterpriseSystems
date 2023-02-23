CREATE OR REPLACE FUNCTION S0000V0000.fnsysMDSConnect()
    RETURNS VOID
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
This procedure connects to the Master subnet server

CHANGE LOG
20211118 Blair Kjenner	Initial Code

SAMPLE CALL

select fnsysMDSConnect();

select dblink_disconnect('aws');

*/
DECLARE
    e_Context TEXT;
    e_Msg     TEXT;
    e_State   TEXT;
    l_Error   RECORD;
BEGIN
    IF DBLINK_GET_CONNECTIONS() IS NULL
    THEN
        PERFORM DBLINK_CONNECT('aws',
        -- WHEN YOU CHANGE THE SERVER YOU ALSO NEED TO CHANGE THE DATABASE YOU ARE CONNECTING TO FOR SETUP MASTER DATA SERVER
        --'host=awsfree.cbi3wlrbldxy.us-west-2.rds.amazonaws.com port=5432 dbname=sn000000 connect_timeout=9999 user=enternetadmin password=Scanner-Seduce8-Kabob-Contact-Process options=-csearch_path=');
        'dbname=sn000000 user=postgres password=Chabanack1 options=-csearch_path=');
    END IF;

    RETURN;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_state = RETURNED_SQLSTATE,
            e_msg = MESSAGE_TEXT,
            e_context = PG_EXCEPTION_CONTEXT;
        l_error := fnsysError(e_state, e_msg, e_context);
        IF DBLINK_GET_CONNECTIONS() IS NOT NULL
        THEN
            PERFORM DBLINK_DISCONNECT('aws');
        END IF;
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', l_Error.Message;
        ELSE
            RAISE NOTICE '%', l_Error.Message ;
        END IF;
        RETURN;
END ;
$$ LANGUAGE plpgsql


