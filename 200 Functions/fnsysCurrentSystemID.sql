CREATE OR REPLACE FUNCTION S0000V0000.fnsysCurrentSystemID(p_schema VARCHAR DEFAULT NULL)
    RETURNS INT
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

This function allows us to translate a system id into a user friendly format.

20210310    Blair Kjenner   Initial Code

select fnsysCurrentSystemID ()

*/
SELECT midfind(COALESCE(p_schema, CURRENT_SCHEMA())::VARCHAR, 's', 'v')::INT;
$$
    LANGUAGE sql;

CREATE OR REPLACE FUNCTION S0000V0000.fnsysCurrentSystemID(p_id int )
    RETURNS INT
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

This function retreives the system id portion of the id that is passed

20211031    Blair Kjenner   Initial Code

select fnsysCurrentSystemID (id)

*/
SELECT leftfind(fnsysIDView(p_id), '-')::INT;
$$
    LANGUAGE sql;