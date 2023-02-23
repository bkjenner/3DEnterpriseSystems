CREATE OR REPLACE FUNCTION S0000V0000.fnexIsGovernor(p_sysdictionarytableid BIGINT, p_rowid BIGINT)
    RETURNS BOOLEAN
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

Parameters:

20221214   Blair Kjenner   Initial Code

*/
DECLARE
    l_IsGovernor BOOLEAN := FALSE;
    l_exsystemid INT;
BEGIN

    SELECT exsystemid
    INTO l_exsystemid
    FROM vwexGovernanceAWS
    WHERE sysdictionarytableid = p_sysdictionarytableid
      AND rowid = p_rowid
    ORDER BY transferdate DESC
    LIMIT 1;

    IF l_exsystemid = fnsysCurrentSystemID()
    THEN
        l_IsGovernor := TRUE;
    ELSE
        IF l_exsystemid IS NULL AND fnsysidview(p_rowid, 's') = fnsysCurrentSystemID()
        THEN
            l_IsGovernor := TRUE;
        END IF;
    END IF;

    RETURN l_IsGovernor;
END
$$ LANGUAGE plpgsql;
