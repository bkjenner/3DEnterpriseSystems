CREATE OR REPLACE FUNCTION S0000V0000.ViewLocals(p_variables VARCHAR)
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

This function creates a raise notice to view local variables for the purpose of debugging.  Copy paste
the variable string into it and then select it.

20211026    Blair Kjenner   Initial Code

select ViewLocals('
    l_sql               TEXT;
    l_schema            VARCHAR;
    l_exSystemID        BIGINT;
    l_exSystemIDMax     BIGINT;
    l_currentsearchpath VARCHAR;
    l_SubnetServerschema  VARCHAR;
    l_exSubnetServerID    INT;')

 */
DECLARE
    l_variablestring VARCHAR := '';
    l_array          VARCHAR[];
    l_variable       VARCHAR;
BEGIN
    l_array := STRING_TO_ARRAY(p_variables, '
');

    FOR i IN ARRAY_LOWER(l_array, 1) .. ARRAY_UPPER(l_array, 1)
    LOOP
        l_variable := COALESCE(leftfind(TRIM(l_array[i]), ' '), '');
        IF LENGTH(l_variable) > 0
        THEN
            l_array[i] := l_variable || ' %';
            l_variablestring := l_variablestring || FORMAT(' %s,', l_variable);
        END IF;

    END LOOP;
    RETURN 'RAISE NOTICE ''
SEARCH_PATH % ' || ARRAY_TO_STRING(l_array, '
', '*') || ''', current_setting(''search_path''), ' || LEFT(l_variablestring, LENGTH(l_variablestring) - 1) || ';';
END ;
$$
    LANGUAGE plpgsql;
