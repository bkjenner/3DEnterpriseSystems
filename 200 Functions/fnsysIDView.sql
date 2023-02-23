CREATE OR REPLACE FUNCTION S0000V0000.fnsysIDView(p_id BIGINT)
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

This function allows us to translate a system id into a user friendly format.

20210310    Blair Kjenner   Initial Code

select fnsysIDView (-9223369999999999999)
select fnsysIDView (1000000000000)

*/
DECLARE
    l_systemid INT;
    l_id       BIGINT;
    l_idtext   varchar;

BEGIN

    IF p_id < 0
    THEN
        p_id := p_id *-1;
        l_idtext := right('0000000000000000000' || p_id::VARCHAR,19);
        l_systemid := left(l_idtext, 6)::INT*2;
        l_id := RIGHT(l_idtext, 13)::BIGINT;
    ELSE
        l_idtext := right('0000000000000000000' || p_id::VARCHAR,19);
        l_systemid := left(l_idtext, 6)::INT;
        l_id := RIGHT(l_idtext, 13)::BIGINT;
    END IF;
    --SELECT l_formatid as FormatID, l_systemid as SystemID, l_id as ID INTO ret;
    RETURN l_systemid::varchar||'-'||l_id::varchar;
END;
$$
    STRICT
    IMMUTABLE
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION S0000V0000.fnsysIDView(p_id BIGINT,p_type char(1))
    RETURNS bigint
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

This function allows us to view components of a systems id ('s' for system, 'r' for recordid

20210310    Blair Kjenner   Initial Code

select fnsysIDView (-9223369999999999999,'s')
select fnsysIDView (1000000000000, 's')

*/
DECLARE
    l_systemid INT;
    l_id       BIGINT;
    l_idtext   varchar;

BEGIN

    IF p_id < 0
    THEN
        p_id := p_id *-1;
        l_idtext := right('0000000000000000000' || p_id::VARCHAR,19);
        l_systemid := left(l_idtext, 6)::INT*2;
        l_id := RIGHT(l_idtext, 13)::BIGINT;
    ELSE
        l_idtext := right('0000000000000000000' || p_id::VARCHAR,19);
        l_systemid := left(l_idtext, 6)::INT;
        l_id := RIGHT(l_idtext, 13)::BIGINT;
    END IF;
    --SELECT l_formatid as FormatID, l_systemid as SystemID, l_id as ID INTO ret;
    RETURN case when lower(p_type)='s' then l_systemid else l_id end;
END;
$$
    STRICT
    IMMUTABLE
    LANGUAGE plpgsql;
