CREATE OR REPLACE VIEW s0000v0000.vwexSubnetServerAWS
AS
/*This file is part of the 3D Enterprise System Platform. The 3D Enterprise System Platform is free
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

This view selects data from the exSubnetServer table on AWS

20211024    Blair Kjenner   Initial Code

select * from vwexSubnetServerAWS

*/
SELECT a.*
FROM fnsysMDSConnect()
JOIN LATERAL (SELECT * from DBLINK(
        'aws',
        'SELECT a.id, a.name, a.systemidSubnetServer, a.rowstatus, a.syschangehistoryid, b.schemaname as schemanameSubnetServer FROM s0000v0000.exSubnetServer A
         left join s0000v0000.exsystem b on b.id=a.systemidSubnetServer')
    AS t1(ID BIGINT, name VARCHAR, systemidSubnetServer BIGINT, rowstatus CHAR, syschangehistoryid BIGINT, schemanameSubnetServer VARCHAR )) a on TRUE;


