CREATE OR REPLACE FUNCTION S0000V0000.fnexWhereClauseCheck(p_exRecordGroupID bigint default null)
    RETURNS VARCHAR
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
This procedure checks the where clause on the exRecordGroup to make sure it is valid.

20220930    Blair Kjenner   Initial Code

select fnexWhereClauseCheck();
select fnexWhereClauseCheck(fnsysidget(101,5));

*/
DECLARE
    l_SQL      VARCHAR;
    l_template VARCHAR;
    l_rec      RECORD;
    e_msg      VARCHAR;
    e_state    VARCHAR;

BEGIN
    l_template := '
DROP TABLE IF EXISTS t_test;
CREATE TEMP TABLE t_test AS SELECT ID FROM p_tablename WHERE p_whereclause LIMIT 0;
';
    FOR l_rec IN
        SELECT dt.name tablename, rg.whereclause, fnsysidview(rg.id) id
        FROM exrecordgroup rg
        JOIN sysdictionarytable dt
             ON dt.id = rg.sysdictionarytableid
        WHERE COALESCE(rg.whereclause, '') != ''
        AND (p_exRecordGroupid is null or p_exRecordGroupID = rg.id)
    LOOP
        l_sql := REPLACE(REPLACE(l_template, 'p_tablename', l_rec.tablename), 'p_whereclause', l_rec.whereclause);
        --RAISE NOTICE ' sql %', l_sql;
        EXECUTE l_sql;

    END LOOP;

    RETURN NULL;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS e_msg = MESSAGE_TEXT, e_state = RETURNED_SQLSTATE;
        RETURN ' ( ' || l_rec.whereclause || ' ) for table ' || l_rec.tablename ||
               ' on recordgroup ' || l_rec.id || ' (' || e_msg || '-' || e_state || ')';

END ;
$$ LANGUAGE plpgsql;