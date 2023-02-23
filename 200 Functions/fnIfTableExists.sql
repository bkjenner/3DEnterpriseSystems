CREATE OR REPLACE FUNCTION S0000V0000.fnIfTableExists(VARCHAR)
    RETURNS pg_catalog.bool AS
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
This function checks if a table exists.  One special thing it
does is recognize the name space you are in.  Without that
check, a temp table will exist even if it is created in another name space.

20210915 Blair Kjenner	Initial Code

select fnIfTableExists('t_temp')

*/
DECLARE

BEGIN

    /* check the table exist in database and is visible*/
    PERFORM n.nspname, c.relname
    FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n
              ON n.oid
                  = c.relnamespace
    WHERE pg_catalog.PG_TABLE_IS_VISIBLE(c.oid)
      AND UPPER(relname) = UPPER($1);

    IF FOUND
    THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;

END;
$$
    LANGUAGE 'plpgsql' VOLATILE