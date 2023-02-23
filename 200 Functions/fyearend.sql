DROP FUNCTION IF EXISTS S0000V0000.fyearend(DATE, DATE);
CREATE OR REPLACE FUNCTION S0000V0000.fyearend(p_date DATE, p_fiscalenddate DATE)
    RETURNS DATE AS
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
This function returns the end of a fiscal date for a date based on a fiscal date

20210331    Blair Kjenner   Initial Code

select fyearend(date '2021-11-30', date '2021-11-30')

*/
DECLARE
    l_month  INT;
    l_fmonth INT;
    l_fyear  INT;
    l_fday    INT;
BEGIN
    l_month := EXTRACT(MONTH FROM p_date);
    l_fmonth := EXTRACT(MONTH FROM p_fiscalenddate);
    l_fday := EXTRACT(DAY FROM p_fiscalenddate);
    l_fyear := EXTRACT(YEAR FROM p_date) + CASE WHEN l_month > l_fmonth THEN 1 ELSE 0 END;
    RETURN MAKE_DATE(l_fyear, l_fmonth, l_fday);
END;
$$
    LANGUAGE plpgsql IMMUTABLE;
