DROP VIEW IF EXISTS S0000V0000.vwexSystemAWS CASCADE;
CREATE OR REPLACE VIEW s0000v0000.vwexSystemAWS
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

This view selects data from the exSystems table on AWS

20211024    Blair Kjenner   Initial Code

select * from vwexSystemAWS

 */
SELECT a.*
FROM fnsysMDSConnect()
JOIN LATERAL (SELECT * FROM DBLINK(
        'aws',
        'SELECT a.id, a.exSubnetServerid, a.name, b.name exSubnetServerName, c.schemaname exSubnetServerSchema, c.id exSubnetServerSystemID, a.schemaname, a.productionversion, a.testversion, '''' subscriptionkey, a.rowstatus, a.syschangehistoryid
        FROM s0000v0000.exSystem a
        left join s0000v0000.exSubnetServer b on b.id=a.exSubnetServerid
        left join s0000v0000.exSystem c on c.id=b.systemidSubnetServer')
    AS t1(ID INT, exSubnetServerid bigint, name VARCHAR, exSubnetServerName varchar, exSubnetServerSchema varchar, exSubnetServerSystemID BIGINT, schemaname VARCHAR, productionversion INT, testversion INT, subscriptionkey varchar, rowstatus CHAR, syschangehistoryid BIGINT)) a on TRUE;
