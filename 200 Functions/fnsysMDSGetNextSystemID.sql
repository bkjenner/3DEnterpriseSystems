CREATE OR REPLACE FUNCTION S0000V0000.fnsysMDSGetNextSystemID(p_name VARCHAR, p_isSubnetServer BOOLEAN DEFAULT FALSE,
                                                                   p_exsystemid INT DEFAULT NULL)
    RETURNS INT
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
This procedure communicates with the Master subnet server to get the next system id

CHANGE LOG
20211023 Blair Kjenner	Initial Code

PARAMETERS
l_exSubnetServerID - subnet server to send data to
p_name - Name of system

SAMPLE CALL

select fnsysMDSGetNextSystemID ('test system2', TRUE);

select * from vwexSystemAWS;

*/
DECLARE
    e_Context           TEXT;
    e_Msg               TEXT;
    e_State             TEXT;
    l_Error             RECORD;
    l_sql               TEXT;
    l_schema            VARCHAR;
    l_exSystemIDSubnetServer      INT;
    l_exSystemIDNew     INT;
    l_currentsearchpath VARCHAR;
    l_SubnetServerschema  VARCHAR;
    l_exSubnetServerID    INT;
BEGIN
    l_currentSearchPath := CURRENT_SETTING('search_path');
    l_exSubnetServerid := COALESCE(l_exSubnetServerid, fnsyscurrentSubnetServerid());

    l_exSystemIDNew := coalesce(p_exsystemid, (select id from vwexSystemNextValAWS), 0);

    IF l_exSystemIDNew=0
    THEN
        -- Error creating next system id.  Zero means error accessing AWS. System ID %
        RAISE SQLSTATE '51069' USING MESSAGE = l_exSystemIDNew;
    END IF;

    -- Insert a new system record into the exSystem table on the Master subnet server
    -- and get the assigned id

    IF NOT EXISTS(SELECT * FROM vwexsystemaws WHERE id = p_exsystemid)
    THEN
        l_sql := FORMAT('
        INSERT INTO s0000v0000.exSystem(id, exSubnetServerid, name, rowstatus)
        SELECT %s, %s, ''%s'', ''a'';
        ', p_exsystemid, l_exSubnetServerID, fixquote(p_name));

        PERFORM fnsysMDSExecute(l_sql);
    END IF;

    IF p_isSubnetServer
    THEN
        SELECT systemidSubnetServer
        INTO l_exSystemIDSubnetServer
        FROM vwexSubnetServeraws
        WHERE id = l_exSubnetServerID;
        -- If the subnet server system is not null or it is not equal to the next system id then error
        -- Later condition is because test data is pre-setup with the correct system ids
        IF NOT (l_exSystemIDSubnetServer IS NULL OR l_exSystemIDSubnetServer = l_exSystemIDNew)
        THEN
            -- Since we cant rollback we are just going to delete it and reset then next seq number
            PERFORM fnsysMDSExecute(FORMAT('
            delete from s0000v0000.exsystem where id = SETVAL(''s0000v0000.exsystem_id_seq'', %s, FALSE);
            ', l_exSystemIDNew));

            -- subnet server already exists for this database.  Current subnet server is %.
            RAISE SQLSTATE '51070' USING MESSAGE = l_exSystemIDSubnetServer;
        END IF;
    END IF;

    l_schema := fnsysCurrentSchema(l_exSystemIDNew, 0);

    l_sql := FORMAT('
        UPDATE s0000v0000.exSystem set schemaname = ''%s'' where id=%s;
        ', l_schema, l_exSystemIDNew);
    PERFORM fnsysMDSExecute(l_sql);

    IF p_isSubnetServer
    THEN
        -- update the subnet server record to indicate the new subnet server schema id
        l_sql := FORMAT('
        UPDATE s0000v0000.exSubnetServer set systemidSubnetServer = ''%s'' where id=%s;
        ', l_exSystemIDNew, l_exSubnetServerID);
        PERFORM fnsysMDSExecute(l_sql);
    END IF;

    DROP TABLE IF EXISTS t_exsystem;
    CREATE TEMP TABLE t_exsystem
    AS
    SELECT *
    FROM vwexsystemaws
    WHERE id = l_exSystemIDNew;

    SELECT exSubnetServerschema INTO l_SubnetServerschema FROM t_exsystem;

    PERFORM SET_CONFIG('search_path', l_SubnetServerschema || ',' || CURRENT_SETTING('search_path'), TRUE);

    -- Insert into the subnet server system table.  This is done because every exsystem table
    -- for every schema should include the system entries for the systems a given schema
    -- interacts with.
    INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, rowstatus, syschangehistoryid)
    SELECT id, exSubnetServerid, name, schemaname, rowstatus, syschangehistoryid
    FROM vwexsystemaws
    WHERE id = l_exSystemIDNew
      AND id NOT IN (
                        SELECT id
                        FROM exsystem);

    PERFORM SET_CONFIG('search_path', l_currentSearchPath, TRUE);

    RETURN l_exSystemIDNew;

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
            IF l_error.IsExceptionRaised = TRUE
            THEN
                RAISE EXCEPTION '%', l_Error.Message;
            ELSE
                RAISE NOTICE '%', l_Error.Message ;
            END IF;
        END IF;
        RETURN NULL;
END ;
$$ LANGUAGE plpgsql


