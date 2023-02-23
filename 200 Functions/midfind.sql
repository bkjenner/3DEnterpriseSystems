CREATE OR REPLACE FUNCTION S0000V0000.midfind(p_string VARCHAR, p_delimiter1 VARCHAR DEFAULT '.',
                                              p_delimiter2 VARCHAR DEFAULT NULL)
    RETURNS VARCHAR AS
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

This function returns anything mid of the delimiter

20211014    Blair Kjenner   Initial Code

select midfind('abcZZdefYghi','ZZ','Y')

 */
DECLARE
    l_var VARCHAR;
BEGIN
    p_delimiter2 := COALESCE(p_delimiter2, p_delimiter1);
    IF POSITION(p_delimiter1 IN p_string) = 0
    THEN
        l_var := '';
    ELSE
        l_var := SUBSTRING(p_string, POSITION(p_delimiter1 IN p_string) + LENGTH(p_delimiter1), 9999);
    END IF;
    IF POSITION(p_delimiter2 IN l_var) > 0
    THEN
        l_var := LEFT(l_var, POSITION(p_delimiter2 IN l_var) - 1);
    END IF;
    RETURN l_var;
END;
$$
    LANGUAGE plpgsql IMMUTABLE;
