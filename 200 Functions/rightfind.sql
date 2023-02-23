CREATE OR REPLACE FUNCTION S0000V0000.rightfind(p_string VARCHAR, p_delimiter VARCHAR DEFAULT '.')
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

This function returns anything right of the delimiter

20211014    Blair Kjenner   Initial Code

select rightfind('xxxx..xx..yy','..') -- yy

 */
DECLARE
    l_var VARCHAR;
BEGIN
    IF POSITION(p_delimiter IN p_string) = 0
    THEN
        l_var := '';
    ELSE
        l_var := REVERSE(p_string);
        l_var := REVERSE(LEFT(l_var, POSITION(REVERSE(p_delimiter) IN l_var) - 1));
    END IF;
    RETURN l_var;
END;
$$
    LANGUAGE plpgsql IMMUTABLE;
