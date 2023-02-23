CREATE OR REPLACE FUNCTION S0000V0000.fnsysIDSetValAll(p_schema VARCHAR DEFAULT NULL, p_systemid INT DEFAULT NULL,
                                                       p_Debug BOOLEAN DEFAULT FALSE)
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

This function sets the seq id for all tables in a schema.

SELECT fnsysIDSetValAll('templateV0001',0);

*/
DECLARE
    l_sql      TEXT;
    l_SystemID INT;
BEGIN

    p_schema := LOWER(COALESCE(p_schema, CURRENT_SCHEMA()));
    l_SystemID := COALESCE(l_systemid, fnsysCurrentSystemID(p_schema := p_schema));

    IF NOT EXISTS(SELECT FROM pg_namespace WHERE nspname = p_Schema)
    THEN
        RAISE NOTICE 'source schema % does not exist!', p_Schema;
        RETURN;
    END IF;

    SELECT STRING_AGG(REPLACE(REPLACE('
SELECT fnsysIDSetVal(p_table:=''l_tablename'', p_systemid := l_systemid);
', 'l_tablename', name), 'l_systemid', l_SystemID::VARCHAR), '')
    INTO l_sql
        --select *
    FROM sysdictionarytable s
    WHERE LOWER(name) NOT IN ('exsystem', 'exsubnetserver');

    IF p_Debug
    THEN
        RAISE NOTICE '%',l_SQL;
    ELSE
        EXECUTE (l_SQL);
    END IF;

    RETURN;
END ;

$$
    LANGUAGE plpgsql