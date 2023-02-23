CREATE OR REPLACE FUNCTION S0000V0000.fyear(p_date TIMESTAMP, p_fiscalenddate DATE)
    RETURNS INTEGER AS
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
This function returns the fiscal year for a date based on a fiscal date

20210317    Blair Kjenner   Initial Code

select fyear(now()::timestamp, date '2020-11-30')

*/
DECLARE
    l_month  INT;
    l_fmonth INT;
BEGIN
    l_month := EXTRACT(MONTH FROM p_date);
    l_fmonth := EXTRACT(MONTH FROM p_fiscalenddate + cast('1 day' as interval));
    RETURN EXTRACT(YEAR FROM p_date) + case when l_month>=l_fmonth then 1 else 0 end;

END;
$$
    LANGUAGE plpgsql IMMUTABLE;
