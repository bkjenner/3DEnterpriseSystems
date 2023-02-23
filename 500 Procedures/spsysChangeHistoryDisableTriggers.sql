DROP PROCEDURE IF EXISTS S0000V0000.spsysChangeHistoryDisableTriggers;
CREATE OR REPLACE PROCEDURE S0000V0000.spsysChangeHistoryDisableTriggers(p_debug BOOLEAN DEFAULT FALSE)
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

This procedure disables change history.

20210325    Blair Kjenner   Initial Code

call spsysChangeHistoryDisableTriggers(p_debug:=false)
*/
DECLARE
    l_SQL VARCHAR := '';
BEGIN

    SELECT STRING_AGG('Drop FUNCTION if exists ' || ROUTINE_name || ' CASCADE; ', '')
    INTO l_SQL
    FROM information_schema.routines r
    WHERE (ROUTINE_name LIKE 'fnch%insch'
        OR ROUTINE_name LIKE 'fnch%delch'
        OR ROUTINE_name LIKE 'fnch%updch')
      AND specific_schema = CURRENT_SCHEMA();

    IF l_sql IS NOT NULL
    THEN
        EXECUTE l_sql;
    END IF;

END
$$
    LANGUAGE plpgsql
