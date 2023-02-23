DROP PROCEDURE IF EXISTS S0000V0000.spsysTemporalDataNormalize;
CREATE OR REPLACE PROCEDURE S0000V0000.spsysTemporalDataNormalize(p_table VARCHAR,
                                                                  p_columns VARCHAR DEFAULT NULL,
                                                                  p_sysChangeHistoryID BIGINT DEFAULT NULL,
                                                                  p_Debug BOOLEAN DEFAULT FALSE
)
    LANGUAGE plpgsql
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
This procedure reviews a temporal data and eliminates duplicate segments.  You can pass the columns
you want to pass for it to check for duplicates.

p_table - Name of table that we are eliminating duplicate segments for.
p_columns - Columns to check for duplicates.  By default it will check all columns in the table
except for sysChangeHistoryID, TemporalStartDate, TemporalEndDate.
p_Debug - Outputs the script for review.

call spsysTemporalDataNormalize(p_table:='lrinterest',p_debug:=true);

*/
DECLARE
    l_SQL          VARCHAR;
    l_columns      VARCHAR;
    l_allcolumns   VARCHAR;
    l_firstsegment VARCHAR;
    l_lastsegment  VARCHAR;
    l_selectlist   VARCHAR;
    l_schema       VARCHAR;
BEGIN

    -- Need to get the actual schema in case it is a temp table
    SELECT nspname
    INTO l_schema
    FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n
              ON n.oid
                  = c.relnamespace
    WHERE pg_catalog.PG_TABLE_IS_VISIBLE(c.oid)
      AND UPPER(relname) = UPPER(p_Table);

    -- Get the columns names if it was not passed
    SELECT STRING_AGG(column_name, ', '), STRING_AGG('a.' || column_name, ', ')
    INTO l_columns, l_selectlist
    FROM (
             SELECT column_name
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME ILIKE p_table
               AND TABLE_SCHEMA = l_schema
                 --AND Data_Type NOT IN ('bytea')
               AND column_name NOT IN ('syschangehistoryid', 'temporalstartdate', 'temporalenddate', 'rowstatus')
             ORDER BY ordinal_position) AS a;

    l_allcolumns := l_columns || ', rowstatus, syschangehistoryid, temporalstartdate, temporalenddate';
    l_firstsegment := l_selectlist || ', ''d'', ' || COALESCE(p_sysChangeHistoryID::VARCHAR, 'null') ||
                      ', ''1000-01-01''::date, dateadd(''d'',-1 ,a.temporalstartdate)';
    l_lastsegment := l_selectlist || ', ''d'', ' || COALESCE(p_sysChangeHistoryID::VARCHAR, 'null') ||
                     ', dateadd(''d'',1,a.temporalenddate), ''9999-12-31''::date';

    IF p_columns IS NULL
    THEN
        p_columns := l_columns || ', rowstatus';
    END IF;

    l_SQL := REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('
DO
$TEMP$
BEGIN
    -- Create temporary table that will be used to hold the information for the tables to be consolidated
    DROP TABLE IF EXISTS t_DuplicateRecords;
    CREATE TEMP TABLE t_DuplicateRecords
    (
        ID                BIGINT,
        TemporalStartDate DATE,
        TemporalEndDate   DATE
    );

    -- Inserts a starting segment of 1000-01-01 if it does not exist
    insert into p_table (l_allcolumns)
    select l_firstsegment from p_table a
    join (select aa.id, min(aa.temporalstartdate) temporalstartdate
    from p_table aa
    group by aa.id
    having min(aa.temporalstartdate) <> ''1000-01-01''
         ) b on b.id=a.id and b.temporalstartdate=a.temporalstartdate;

    -- Update the startdate to the enddate-1 of the previous segment if it is wrong
    update p_table as a set temporalstartdate=b.temporalstartdate, syschangehistoryid=' ||
                                                     COALESCE(p_sysChangeHistoryID::VARCHAR, 'null') || '
    from (  select aa.id, aa.temporalenddate, coalesce(dateadd(''d'',1,bb.temporalenddate),''1000-01-01''::date) temporalstartdate
            from p_table aa
            left join lateral (select aaa.temporalenddate
                               from p_table aaa
                               where aaa.id=aa.id
                               and aaa.temporalenddate<aa.temporalenddate
                               order by aaa.temporalenddate desc
                               limit 1) bb on true) b
    where b.id = a.id
    and b.temporalenddate=a.temporalenddate
    and b.temporalstartdate!=a.temporalstartdate;

    -- Update the enddate to 9999-12-31 for any record series that doesnt have one
    update p_table as a set temporalenddate=''9999-12-31'', syschangehistoryid=' ||
                                                     COALESCE(p_sysChangeHistoryID::VARCHAR, 'null') || '
    from (  select aa.id, max(aa.temporalenddate) temporalenddate
            from p_table aa
            group by aa.id) b
    where b.id = a.id
    and b.temporalenddate = a.temporalenddate
    and b.temporalenddate!=''9999-12-31'';

    -- We need to loop through because all segments for a record set cannot be consolidated in the same pass
    LOOP
        -- Build the SQL script to identify duplicate records. The premise of the SQL statement is to add one to the
        -- end date of a segment and compare it to the TemporalStartDate of the next segment if it exists. Then we do a group by
        -- on the fields that were passed and if we have a count of 2 it means the two segments had the same data

        insert into t_DuplicateRecords
        select ID, min(TemporalStartDate), MAX(TemporalEndDate) from
            (   select p_columns, TemporalStartDate, TemporalEndDate, TemporalStartDate as Linkdate from p_table
                union
                select p_columns, TemporalStartDate, TemporalEndDate, case when TemporalEndDate = ''9999-12-31'' then TemporalEndDate else dateadd(''d'',1,TemporalEndDate) end as LinkDate
                from p_table
            ) sub
            group by p_columns, LinkDate having COUNT(*)=2;

        -- If there are no records in the t_DuplicateRecords table it means that all duplicate records have been corrected and we can exit the loop
        IF NOT EXISTS (SELECT FROM t_DuplicateRecords)
        THEN
            EXIT;
        END IF;

        -- We need to delete records in the temp table that have records with start and end dates that overlap. If this happens it means that
        -- there are three or more consecutive segments that have the same data. If we tried to deal with these all at the same time we would be deleting
        -- segments that we were not expecting
        DELETE
        FROM t_DuplicateRecords AS z1
            USING t_DuplicateRecords AS z2
        WHERE z2.ID = z1.ID
          AND z1.TemporalEndDate > z2.TemporalStartDate
          AND z1.TemporalEndDate < z2.TemporalEndDate;

        DELETE FROM p_table a USING t_DuplicateRecords d where d.ID = a.ID and d.TemporalEndDate = a.TemporalEndDate;

        UPDATE p_table as a set TemporalEndDate=d.TemporalEndDate
        from t_DuplicateRecords d
        where d.ID=a.ID
        and d.TemporalStartDate = a.TemporalStartDate;

        TRUNCATE TABLE t_DuplicateRecords;

    END LOOP;

    DROP TABLE IF EXISTS t_DuplicateRecords;
END;
$TEMP$
', 'p_table', p_table), 'p_columns', p_columns), 'l_firstsegment', l_firstsegment), 'l_lastsegment', l_lastsegment),
                     'l_allcolumns', l_allcolumns);

    -- Execute the SQL Script
    IF p_Debug = FALSE
    THEN
        EXECUTE (l_SQL);
    ELSE
        RAISE NOTICE '%', l_SQL;
    END IF;

END;
$$;
