CREATE OR REPLACE FUNCTION S0000V0000.dateadd(p_interval varchar, p_increment integer, p_date timestamp)
RETURNS timestamp AS
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
This function emulates MSSQL year

20210221    Blair Kjenner   Initial Code

 select S0000V0000.dateadd('month',1,now()::timestamp)
 select S0000V0000.dateadd('day',1,'2021-01-01')
 select S0000V0000.dateadd('hour',1,now()::timestamp)

 */
BEGIN
p_interval := lower(p_interval);
if p_Interval in ('m', 'mm', 'month') then
return p_Date + cast(p_increment || ' months' as interval);
elseif p_Interval in ('y', 'yy', 'year') then
return p_Date + cast(p_increment || ' years' as interval);
elseif p_Interval in ('d', 'dd', 'day') then
return p_Date + cast(p_increment || ' day' as interval);
elseif p_Interval in ('h', 'hh', 'hour') then
return p_Date + cast(p_increment || ' hour' as interval);
else
raise exception 'dateadd interval parameter not supported';
end if;
END;
$$
LANGUAGE plpgsql  IMMUTABLE;
