CREATE OR REPLACE FUNCTION S0000V0000.fnsysIDGet(p_systemid INT, p_RowID BIGINT)
    RETURNS BIGINT AS
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
This function creates an id based on systemid and rowid
20210310    Blair Kjenner   Initial Code

SELECT fnsysIDGet(1844672, 9999999999999)

SELECT fnsysIDGet(1,1)

*/

SELECT ((case when p_systemid > 922336 then p_systemid - 922336 else p_systemid end * 10000000000000) + p_rowid) * case when p_systemid > 922336 then -1 else 1 end;

$$
    LANGUAGE sql IMMUTABLE;

