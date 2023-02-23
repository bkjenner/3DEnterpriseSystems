CREATE OR REPLACE FUNCTION S0000V0000.fnsysMDSGetSystem(p_exSystemID BIGINT,
                                                             p_subscriptionkey VARCHAR DEFAULT NULL,
                                                             p_syschangehistoryid BIGINT DEFAULT NULL)
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
This procedure communicates with the Master subnet server to setup a local system.  The system must be on the
server with the id specified and the subscription key must match.  It will setup a subscriber record and
return that id

CHANGE LOG
20211023 Blair Kjenner	Initial Code

PARAMETERS
p_exSystemID - Name of system to setup
p_SubscriptionKey - Subscripton Key to access the system

SAMPLE CALL

select fnsysMDSGetSystem(4);

*/
DECLARE
    e_Context           TEXT;
    e_Msg               TEXT;
    e_State             TEXT;
    l_Error             RECORD;
    l_currentsearchpath VARCHAR;
    l_exsubscriberid    BIGINT;
BEGIN

    DROP TABLE IF EXISTS t_exsystem;
    CREATE TEMP TABLE t_exSystem
    AS
    SELECT id,
           exSubnetServerid,
           name,
           schemaname,
           productionversion,
           testversion,
           subscriptionkey,
           rowstatus,
           p_syschangehistoryid sysChangeHistoryID
    FROM vwexSystemaws
    WHERE ID = p_exSystemID
      AND COALESCE(subscriptionkey, '') = COALESCE(p_subscriptionkey, '');

    IF NOT EXISTS(SELECT FROM t_exSystem)
    THEN
        RETURN NULL;
    END IF;

    INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, subscriptionkey, rowstatus, syschangehistoryid)
    SELECT *
    FROM t_exSystem
    WHERE id NOT IN (
                        SELECT id
                        FROM exsystem);

    INSERT INTO exsubscriber (exsystemid, name, syschangehistoryid)
    SELECT id, name, syschangehistoryid
    FROM t_exsystem
    WHERE id NOT IN (
                        SELECT exsystemid
                        FROM exsubscriber
                        WHERE exsystemid IS NOT NULL)
    RETURNING id INTO l_exsubscriberid;

    IF l_exsubscriberid IS NULL
    THEN
        SELECT id INTO l_exsubscriberid FROM exsubscriber a WHERE exsystemid = a.exsystemid;
    END IF;

    l_currentsearchpath := CURRENT_SETTING('search_path');

    PERFORM SET_CONFIG('search_path', fnsysCurrentSubnetServerSchema() || ',' || l_currentsearchpath, FALSE);
    -- Anytime a new system is added locally it must also be added to the SubnetServer
    INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, subscriptionkey, rowstatus, syschangehistoryid)
    SELECT *
    FROM t_exSystem
    WHERE ID NOT IN (
                        SELECT id
                        FROM exSystem);

    PERFORM SET_CONFIG('search_path', l_currentsearchpath, FALSE);

    RETURN l_exsubscriberid;

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
