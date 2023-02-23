CREATE OR REPLACE FUNCTION S0000V0000.iscolumnfound(p_tablename varchar, p_columnname varchar)
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

This function tests if a column exists

20210607    Blair Kjenner   Initial Code

 select iscolumnfound('S0001V0000.glaccount', 'id')

 */
DECLARE
    l_found BOOLEAN;
BEGIN

    l_found := EXISTS(SELECT 1
                      FROM pg_attribute
                      WHERE attrelid = p_tablename::REGCLASS -- cast to a registered class (table)
                        AND attname = p_columnname
                        AND NOT attisdropped);

    RETURN l_found;
END;
$$
    LANGUAGE plpgsql;
