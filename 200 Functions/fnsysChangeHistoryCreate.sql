CREATE OR REPLACE FUNCTION S0000V0000.fnsysChangeHistoryCreate(p_Comment varchar DEFAULT 'Test', p_crmcontactiduser BIGINT DEFAULT NULL, p_sysdictionarytableid BIGINT DEFAULT NULL, p_rowid BIGINT DEFAULT NULL)
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
This procedure sets up BottomUpLevel, TopDownLevel, DisplaySequence for hierarchical structures.  It calls glIntegrityCheck to
check for errors.  If errors are detected, it returns false otherwise it returns true.

20210326    Blair Kjenner   Initial Code

select fnsysChangeHistoryCreate('Sample Change History Record');
select fnsysChangeHistoryCreate(p_crmContactidUser:=1);

*/

DECLARE
    l_NewChangeHistoryID BIGINT;

BEGIN

INSERT INTO syschangehistory (
    crmcontactiduser, changedate, comments, rowstatus, sysdictionarytableidappliesto, Rowidappliesto)
SELECT p_crmcontactiduser, NOW()::TIMESTAMP, p_Comment, 'a', p_sysdictionarytableid, p_rowid
RETURNING ID INTO l_NewChangeHistoryID;

RETURN l_NewChangeHistoryID;

END;
$$ LANGUAGE plpgsql

