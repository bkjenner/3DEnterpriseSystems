DROP VIEW IF EXISTS S0000V0000.vwexGovernanceAWS CASCADE;
CREATE OR REPLACE VIEW s0000v0000.vwexGovernanceAWS
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

This view selects data from the exGovernanceDetail and exGovernance tables on AWS

20221214   Blair Kjenner   Initial Code

select * from vwexGovernanceAWS

 */
SELECT a.*
FROM fnsysMDSConnect()
JOIN LATERAL (
         SELECT *
         FROM DBLINK(
                 'aws',
                 'SELECT c.id sysdictionarytableid, c.name sysdictionarytable, a.rowidsubscribedto rowid, e.id exsystemid, e.name exsystem, b.transferdate, d.id exrecordgroupid, d.name exrecordgroup FROM s0000v0000.exGovernanceDetail a
                  JOIN s0000v0000.exGovernance b on b.id=a.exgovernanceid
                  JOIN s0000v0000.sysdictionarytable c on c.id=a.sysdictionarytableidsubscribedto
                  JOIN s0000v0000.exRecordGroup d on d.id=a.exrecordgroupid
                  JOIN s0000v0000.exSystem e on e.id=a.exsystemid')
             AS t1(sysdictionarytableid BIGINT, sysdictionarytable varchar, rowid BIGINT, exsystemid BIGINT, exsystem varchar, transferdate timestamp, exrecordgroupid BIGINT, exrecordgroup varchar)) a ON TRUE;
