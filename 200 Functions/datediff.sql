CREATE OR REPLACE FUNCTION S0000V0000.DateDiff(p_interval varchar, p_start TIMESTAMP, p_end TIMESTAMP)
    RETURNS INT AS
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
This function emulates MSSQL DateDiff

20210221    Blair Kjenner   Initial Code

select S0000V0000.datediff('year',null,now()::timestamp)

 */
DECLARE
    l_diffinterval INTERVAL;
    l_diffint      INT = 0;
    l_diffyears    INT = 0;
BEGIN
    IF p_start IS NULL
    THEN
        p_start := DATE '1900-01-01';
    END IF;

    IF p_interval IN ('yy', 'yyyy', 'year', 'mm', 'm', 'month')
    THEN
        l_diffyears = DATE_PART('year', p_end) - DATE_PART('year', p_start);

        IF p_interval IN ('yy', 'yyyy', 'year')
        THEN
            -- SQL Server does not count full years passed (only difference between year parts)
            RETURN l_diffyears;
        ELSE
            -- If end month is less than start month it will subtracted
            RETURN l_diffyears * 12 + (DATE_PART('month', p_end) - DATE_PART('month', p_start));
        END IF;
    END IF;

    -- Minus operator returns interval 'DDD days HH:MI:SS'
    l_diffinterval = p_end - p_start;

    l_diffint = l_diffint + DATE_PART('day', l_diffinterval);

    IF p_interval IN ('wk', 'ww', 'week')
    THEN
        l_diffint = l_diffint / 7;
        RETURN l_diffint;
    END IF;

    IF p_interval IN ('dd', 'd', 'day')
    THEN
        RETURN l_diffint;
    END IF;

    l_diffint = l_diffint * 24 + DATE_PART('hour', l_diffinterval);

    IF p_interval IN ('hh', 'hour')
    THEN
        RETURN l_diffint;
    END IF;

    l_diffint = l_diffint * 60 + DATE_PART('minute', l_diffinterval);

    IF p_interval IN ('mi', 'n', 'minute')
    THEN
        RETURN l_diffint;
    END IF;

    l_diffint = l_diffint * 60 + DATE_PART('second', l_diffinterval);

    RETURN l_diffint;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


