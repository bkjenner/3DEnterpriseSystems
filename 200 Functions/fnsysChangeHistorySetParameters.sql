CREATE OR REPLACE FUNCTION S0000V0000.fnsysChangeHistorySetParameters(p_ActionType VARCHAR DEFAULT NULL,
                                                                  p_sysChangeHistoryIDForDelete BIGINT DEFAULT NULL)
    RETURNS BOOLEAN AS
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
This procedure creates the t_sysChangeHistoryParm table which allows procedures to communicate with the
change history trigger functions.  If the fnsysChangeHistorySetParameter is called with no parameters
it will drop the t_sysChangeHistoryParm if it exists.

20210326    Blair Kjenner   Initial Code

select fnsysChangeHistorySetParameters(p_sysChangeHistoryIDForDelete:=fnsysChangeHistoryCreate('Test Delete'));
delete from glrate where id=2;

-- Eliminates the sysChangeHistory Parameter table
select fnsysChangeHistorySetParameters();

*/
BEGIN
    IF p_ActionType IS NULL AND p_sysChangeHistoryIDForDelete IS NULL
    THEN
        DROP TABLE IF EXISTS t_sysChangeHistoryParm;
    ELSE
        IF fnIfTableExists('t_sysChangeHistoryParm')
        THEN
            UPDATE t_sysChangeHistoryParm
            SET ActionType=p_ActionType, sysChangeHistoryIdForDelete = p_sysChangeHistoryIdForDelete;
        ELSE
            CREATE TEMP TABLE t_sysChangeHistoryParm
            (
                ActionType                  VARCHAR,
                sysChangeHistoryIdForDelete BIGINT
            );

            INSERT INTO t_sysChangeHistoryParm
            VALUES (p_ActionType, p_sysChangeHistoryIDForDelete);

        END IF;
    END IF;

    RETURN TRUE;

END;
$$ LANGUAGE plpgsql

