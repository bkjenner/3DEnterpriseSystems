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
OVERVIEW
This script sets up scheduled jobs to distribute and import data for the proof of concept environment.
---------------------------------------
Instructions

Set database to postgres

Select * from pgagent.pga_job

select a.jslid, b.jobname, a.jslstatus, a.jslresult, jslstart, jslduration, jsloutput from pgagent.pga_jobsteplog a
join pgagent.pga_job b on b.jobid = a.jsljstid
order by a.jslid desc

-- Will cause jobs to run immediately
UPDATE pgagent.pga_job SET jobnextrun = now()

 */
DO
$$
    DECLARE
        jid       INTEGER;
        minutes   BOOLEAN[];
        hours     BOOLEAN[];
        weekdays  BOOLEAN[];
        monthdays BOOLEAN[];
        months    BOOLEAN[];
    BEGIN
        minutes := ARRAY_FILL(TRUE, ARRAY [60]);
        hours := ARRAY_FILL(FALSE, ARRAY [24]);
        weekdays := ARRAY_FILL(FALSE, ARRAY [7]);
        monthdays := ARRAY_FILL(TRUE, ARRAY [32]);
        months := ARRAY_FILL(TRUE, ARRAY [12]);

        truncate table pgagent.pga_jobsteplog cascade;
        truncate table pgagent.pga_jobstep cascade;
        truncate table pgagent.pga_schedule cascade;
        truncate table pgagent.pga_job cascade;

--Setup job for sn000010
        -- Creating a new job
        INSERT INTO pgagent.pga_job(
            jobjclid, jobname, jobdesc, jobhostagent, jobenabled)
        VALUES (1::INTEGER, 'sn000010 Distribute'::TEXT, ''::TEXT, ''::TEXT, TRUE)
        RETURNING jobid INTO jid;

        -- Inserting a step (jobid: NULL)
        INSERT INTO pgagent.pga_jobstep (
            jstjobid, jstname, jstenabled, jstkind,
            jstconnstr, jstdbname, jstonerror,
            jstcode, jstdesc)
        VALUES ( jid, 'Distribute and import'::TEXT, TRUE, 's'::CHARACTER(1),
                   ''::TEXT, 'sn000010'::name, 'f'::CHARACTER(1),
                   'set SEARCH_PATH to s0090v0000, s0000v0000, public;
               CALL spexpackagedistribute();
               set SEARCH_PATH to s0100v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               set SEARCH_PATH to s0101v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               set SEARCH_PATH to s0102v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               set SEARCH_PATH to s0103v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               set SEARCH_PATH to s0104v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               set SEARCH_PATH to s0105v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               set SEARCH_PATH to s0108v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               set SEARCH_PATH to s0109v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               '::TEXT, ''::TEXT);

        -- Inserting a schedule
        INSERT INTO pgagent.pga_schedule(
            jscjobid, jscname, jscdesc, jscenabled,
            jscstart, jscminutes, jschours, jscweekdays, jscmonthdays, jscmonths)
        VALUES ( jid, 'Every minute'::TEXT, ''::TEXT, TRUE,
                   '2022-01-01 08:00:00-00'::TIMESTAMP WITH TIME ZONE, minutes, hours, weekdays, monthdays, months);

--Setup job for sn000011
        -- Creating a new job
        INSERT INTO pgagent.pga_job(
            jobjclid, jobname, jobdesc, jobhostagent, jobenabled)
        VALUES ( 1::INTEGER, 'sn000011 Distribute'::TEXT, ''::TEXT, ''::TEXT, TRUE)
        RETURNING jobid INTO jid;

        -- Inserting a step (jobid: NULL)
        INSERT INTO pgagent.pga_jobstep (
            jstjobid, jstname, jstenabled, jstkind,
            jstconnstr, jstdbname, jstonerror,
            jstcode, jstdesc)
        VALUES (  jid, 'Distribute and import'::TEXT, TRUE, 's'::CHARACTER(1),
                   ''::TEXT, 'sn000011'::name, 'f'::CHARACTER(1),
                   'set SEARCH_PATH to s0091v0000, s0000v0000, public;
               CALL spexpackagedistribute();
               SET SEARCH_PATH to s0106v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               '::TEXT, ''::TEXT);

        -- Inserting a schedule
        INSERT INTO pgagent.pga_schedule(
            jscjobid, jscname, jscdesc, jscenabled,
            jscstart, jscminutes, jschours, jscweekdays, jscmonthdays, jscmonths)
        VALUES ( jid, 'Every minute'::TEXT, ''::TEXT, TRUE,
                   '2022-01-01 08:00:00-00'::TIMESTAMP WITH TIME ZONE, minutes, hours, weekdays, monthdays, months);

--Setup job for sn000012
        -- Creating a new job
        INSERT INTO pgagent.pga_job(
            jobjclid, jobname, jobdesc, jobhostagent, jobenabled)
        VALUES ( 1::INTEGER, 'sn000012 Distribute'::TEXT, ''::TEXT, ''::TEXT, TRUE)
        RETURNING jobid INTO jid;

        -- Inserting a step (jobid: NULL)
        INSERT INTO pgagent.pga_jobstep (
            jstjobid, jstname, jstenabled, jstkind,
            jstconnstr, jstdbname, jstonerror,
            jstcode, jstdesc)
        VALUES (  jid, 'Distribute and import'::TEXT, TRUE, 's'::CHARACTER(1),
                   ''::TEXT, 'sn000012'::name, 'f'::CHARACTER(1),
                   'set SEARCH_PATH to s0092v0000, s0000v0000, public;
               CALL spexpackagedistribute();
               SET SEARCH_PATH to s0107v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               '::TEXT, ''::TEXT);

        -- Inserting a schedule
        INSERT INTO pgagent.pga_schedule(
            jscjobid, jscname, jscdesc, jscenabled,
            jscstart, jscminutes, jschours, jscweekdays, jscmonthdays, jscmonths)
        VALUES ( jid, 'Every minute'::TEXT, ''::TEXT, TRUE,
                   '2022-01-01 08:00:00-00'::TIMESTAMP WITH TIME ZONE, minutes, hours, weekdays, monthdays, months);

--Setup job for sn000013
        -- Creating a new job
        INSERT INTO pgagent.pga_job(
            jobjclid, jobname, jobdesc, jobhostagent, jobenabled)
        VALUES (  1::INTEGER, 'sn000013 Distribute'::TEXT, ''::TEXT, ''::TEXT, TRUE)
        RETURNING jobid INTO jid;

        -- Inserting a step (jobid: NULL)
        INSERT INTO pgagent.pga_jobstep (
            jstjobid, jstname, jstenabled, jstkind,
            jstconnstr, jstdbname, jstonerror,
            jstcode, jstdesc)
        VALUES (  jid, 'Distribute and import'::TEXT, TRUE, 's'::CHARACTER(1),
                   ''::TEXT, 'sn000013'::name, 'f'::CHARACTER(1),
                   'set SEARCH_PATH to s0093v0000, s0000v0000, public;
               CALL spexpackagedistribute();
               SET SEARCH_PATH to s0110v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               '::TEXT, ''::TEXT);

        -- Inserting a schedule
        INSERT INTO pgagent.pga_schedule(
            jscjobid, jscname, jscdesc, jscenabled,
            jscstart, jscminutes, jschours, jscweekdays, jscmonthdays, jscmonths)
        VALUES (
                   jid, 'Every minute'::TEXT, ''::TEXT, TRUE,
                   '2022-01-01 08:00:00-00'::TIMESTAMP WITH TIME ZONE, minutes, hours, weekdays, monthdays, months);

--Setup job for sn000014
        -- Creating a new job
        INSERT INTO pgagent.pga_job(
            jobjclid, jobname, jobdesc, jobhostagent, jobenabled)
        VALUES (  1::INTEGER, 'sn000014 Distribute'::TEXT, ''::TEXT, ''::TEXT, TRUE)
        RETURNING jobid INTO jid;

        -- Inserting a step (jobid: NULL)
        INSERT INTO pgagent.pga_jobstep (
            jstjobid, jstname, jstenabled, jstkind,
            jstconnstr, jstdbname, jstonerror,
            jstcode, jstdesc)
        VALUES (  jid, 'Distribute and import'::TEXT, TRUE, 's'::CHARACTER(1),
                   ''::TEXT, 'sn000014'::name, 'f'::CHARACTER(1),
                   'set SEARCH_PATH to s0094v0000, s0000v0000, public;
               CALL spexpackagedistribute();
               SET SEARCH_PATH to s0120v0000, s0000v0000, public;
               CALL spexpackageimport(p_overridewarningconditions := true);
               '::TEXT, ''::TEXT);

        -- Inserting a schedule
        INSERT INTO pgagent.pga_schedule(
            jscjobid, jscname, jscdesc, jscenabled,
            jscstart, jscminutes, jschours, jscweekdays, jscmonthdays, jscmonths)
        VALUES (
                   jid, 'Every minute'::TEXT, ''::TEXT, TRUE,
                   '2022-01-01 08:00:00-00'::TIMESTAMP WITH TIME ZONE, minutes, hours, weekdays, monthdays, months);


    END
$$;

