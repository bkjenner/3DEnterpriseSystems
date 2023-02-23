CREATE OR REPLACE FUNCTION S0000V0000.fnsysIDSetVal(p_table VARCHAR, p_systemid INT DEFAULT NULL)
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

OVERVIEW

This function sets the SEQ ID based on the systemid.  We could have records for
many system ids within a table.  What we need to do if find the highest rowid
for a given system id.

20211013    Blair Kjenner   Initial Code

SELECT fnsysIDSetVal(p_table:='glreconciliation');

*/
DECLARE
    l_nextid     BIGINT;
    l_previd     BIGINT;
    l_previdtext VARCHAR;
    l_nextrowid  BIGINT;
    l_systemid   INT;
BEGIN

    p_systemid := COALESCE(p_systemid, midfind(current_schema()::varchar, 's','v')::int);

    l_systemid:=p_systemid+1;

    -- Get the next id of the next range.
    l_nextid := fnsysidcreate(p_systemID := l_systemid, p_rowid := 1::BIGINT);

    -- Get the previous id of the next id range.  That should be our max id
    EXECUTE 'SELECT id
             FROM '|| p_table ||'
             WHERE id < $1
             ORDER BY id DESC;
    ' INTO l_previd
        USING l_nextid;

    IF l_previd IS NULL
    THEN
        l_nextrowid := 1;
    ELSE
        l_previdtext := fnsysIDView(l_previd);
        l_systemid := LEFTFIND(l_previdtext, '-')::INT;
        l_nextrowid := RIGHTFIND(l_previdtext, '-')::BIGINT;
        IF l_systemid = p_systemid
        THEN
            l_nextrowid := l_nextrowid + 1;
        ELSE
            l_nextrowid := 1;
        END IF;
    END IF;

    EXECUTE 'SELECT SETVAL('''||p_table||'_id_seq'', $1 , FALSE);' INTO l_nextid USING l_nextrowid;

    RETURN l_nextrowid;
END;


$$ LANGUAGE plpgsql;
