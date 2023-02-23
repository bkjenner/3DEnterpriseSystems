CREATE OR REPLACE FUNCTION S0000V0000.fnsysCurrentSchema(p_exSystemID INT, p_ReleaseID INT)
    RETURNS varchar AS
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

select fnsysCurrentSchema (1,0)

*/

SELECT 's' || RIGHT('0000' || p_exSystemID::VARCHAR, 4) || 'v' ||
       RIGHT('0000' || COALESCE(p_ReleaseID, 0)::VARCHAR, 4)
$$
    LANGUAGE sql;
