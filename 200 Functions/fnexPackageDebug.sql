CREATE OR REPLACE FUNCTION S0000V0000.fnexPackageDebug(p_rowid BIGINT DEFAULT NULL
    , p_tableid BIGINT DEFAULT NULL,
                                                       p_SystemIdSender INT DEFAULT NULL,
                                                       p_SystemIdReceiver INT DEFAULT NULL,
                                                       p_sysChangeHistoryRowId BIGINT DEFAULT NULL)
    RETURNS TABLE
            (
                ROWID   INT,
                MESSAGE VARCHAR
            )
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
This procedure checks to see why a particular row did not get exported to another system.  The row could
be exported as a result of a subscription being created, a foreign key reference being created or
the record changing.

The procedure starts with the source system, then checks the data server, and finally the destination system.

It does not current support exports that occur across multiple databases.  This could be an enhancement.

If you use this function and find an error condition it needs to check for that will simplify debugging, then
add the condition.

Parameters:

p_tableid - sysDictionaryTableID of the table you expected to export.  If you supply a p_sysChangeHistoryRowId
            it can pick it up from that.  Likewise with the next parameter.
p_rowid - Row id that you expect to have exported
p_SystemIdSender - Id of the system that sent the record.  If it is null it will default to the current system
p_SystemIdReceiver - Id of the system that is supposed to receive the record
p_sysChangeHistoryRowId - Id of the change history row you expected to have exported.  This could be an
                          initial export record or a record due to a change.  This can be null.

20221004    Blair Kjenner   Initial Code

-- Identify test data.
NOTE:  If there is no change history row then check that change history is turned on for the table and that the isChangeHistory is on for the column.

select format('select * from fnexPackageDebug(p_tableid:=%s::bigint, p_rowid:=%s,p_SystemIdSender:=%s,p_SystemIdReceiver:=%s, p_syschangehistoryrowid:=%s)'
,sysdictionarytableidappliesto, rowidappliesto, fnsysCurrentSystemID(), exsystemiddestination, b.id), operationtype
from exhistory a
join syschangehistoryrow b on b.id=a.syschangehistoryrowidexported
where exsystemiddestination=102
order by b.id desc;

select * from fnexPackageDebug(p_tableid:=112::bigint, p_rowid:=10000000149630,p_SystemIdSender:=1,p_SystemIdReceiver:=7, p_syschangehistoryrowid:=10000000000130::bigint)

*/
DECLARE
    l_searchpathcurrent VARCHAR;
    l_schemasender      VARCHAR;
    l_schemadataserver  VARCHAR;
    l_schemareceiver    VARCHAR;
    l_exhistorybatchid  BIGINT;
    l_exhistoryid       BIGINT;
BEGIN

    DROP TABLE IF EXISTS t_message;
    CREATE TEMP TABLE t_message
    (
        rowid   SERIAL,
        message VARCHAR
    );

    p_SystemIdSender := COALESCE(p_SystemIdSender, fnsysCurrentSystemID(CURRENT_SCHEMA()::VARCHAR));
    l_schemasender := CURRENT_SCHEMA();

    IF p_systemidreceiver IS NULL
    THEN
        RAISE EXCEPTION 'p_systemidreceiver must be specified';
    END IF;

    IF NOT EXISTS(SELECT FROM vwexsystemaws WHERE id = p_systemidreceiver)
    THEN
        RAISE EXCEPTION 'p_systemidreceiver % does not exist on vwexsystemaws', p_systemidreceiver;
    END IF;

    IF NOT EXISTS(SELECT FROM vwexsystemaws WHERE id = p_systemidsender)
    THEN
        RAISE EXCEPTION 'p_systemidsender % does not exist on vwexsystemaws', p_systemidsender;
    END IF;

    l_searchpathcurrent := CURRENT_SETTING('search_path');
    l_schemasender := fnsysCurrentSchema(p_SystemIdSender, 0);
    l_schemadataserver := fnsysCurrentSchema(fnsysCurrentSubnetServerSystemID(), 0);
    l_schemareceiver := fnsysCurrentSchema(p_SystemIdReceiver, 0);

    IF EXISTS(SELECT n.nspname, c.relname
              FROM pg_catalog.pg_class c
              LEFT JOIN pg_catalog.pg_namespace n
                        ON n.oid
                            = c.relnamespace
              WHERE n.nspname = l_schemasender
                AND c.relname = 'syschangehistoryrow')
    THEN

        PERFORM SET_CONFIG('search_path', l_schemasender, TRUE);

        IF p_sysChangeHistoryRowId IS NOT NULL
        THEN
            p_rowid := COALESCE(p_rowid, (
                                             SELECT rowidappliesto
                                             FROM syschangehistoryrow s
                                             WHERE id = p_sysChangeHistoryRowId));
            p_tableid := COALESCE(p_tableid, (
                                                 SELECT sysdictionarytableidappliesto
                                                 FROM syschangehistoryrow s
                                                 WHERE id = p_sysChangeHistoryRowId));
        END IF;

        IF p_rowid IS NULL OR p_tableid IS NULL
        THEN
            RAISE EXCEPTION 'Row ID and/or Tableid is null and could not be deduced from change history row record';
        END IF;

        p_sysChangeHistoryRowId := COALESCE(p_sysChangeHistoryRowId, (
                                                                         SELECT MAX(id)
                                                                         FROM syschangehistoryrow
                                                                         WHERE rowidappliesto = p_rowid
                                                                           AND sysdictionarytableidappliesto = p_tableid));

        IF p_sysChangeHistoryRowId IS NULL
        THEN
            INSERT INTO t_message (message)
            SELECT '
No change history events for it.  Is change history on for the table?
' || REPLACE(REPLACE(REPLACE('
select *
from l_schemasender.syschangehistoryrow a
where a.rowidappliesto = p_rowid and a.sysdictionarytableidappliesto = p_tableid', 'p_rowid'
                                            , p_rowid::VARCHAR), 'p_tableid'
                                    , p_tableid::VARCHAR), 'l_schemasender',
             l_schemasender);
        ELSE
            INSERT INTO t_message (message)
            SELECT 'sysChangeHistoryRow ' || p_sysChangeHistoryRowId::VARCHAR ||
                   ' found on ' || l_schemasender;
        END IF;

        IF NOT EXISTS(
                SELECT
                FROM exsubscriptiondetail a
                JOIN exsubscriber e
                     ON a.exsubscriberid = e.id
                WHERE a.rowidsubscribedto = p_rowid
                  AND a.sysdictionarytableidsubscribedto = p_tableid
                  AND e.exsystemid = p_systemidreceiver)
        THEN
            INSERT INTO t_message (message)
            SELECT '
Not picked up in a subscription
' || REPLACE(REPLACE(REPLACE(REPLACE('
select *
from l_schemasender.exsubscriptiondetail a
join l_schemasender.exsubscriber e
on a.exsubscriberid = e.id
where a.rowidsubscribedto = p_rowid
and a.sysdictionarytableidsubscribedto = p_tableid
and e.exsystemid = p_systemidreceiver', 'p_rowid'
                                                    , p_rowid::VARCHAR),
                             'p_tableid'
                                            , p_tableid::VARCHAR),
                     'p_systemidreceiver', p_systemidreceiver::VARCHAR), 'l_schemasender', l_schemasender);
        ELSE
            INSERT INTO t_message (message)
            SELECT 'Subscription record found on ' || l_schemasender || ' for ' ||
                   l_schemareceiver;
        END IF;

        SELECT id, exhistorybatchid
        INTO l_exhistoryid, l_exhistorybatchid
        FROM exhistory a
        WHERE a.syschangehistoryrowidexported = p_sysChangeHistoryRowId
          AND a.exsystemiddestination = p_systemidreceiver;
        IF l_exhistoryid IS NULL
        THEN
            INSERT INTO t_message (message)
            SELECT '
Not exported to exhistory.  Has the export been run?
' || REPLACE(REPLACE(REPLACE('
select *
from l_schemasender.exhistory a
where a.syschangehistoryrowidexported = p_syschangehistoryrowid
  and a.exsystemiddestination = p_systemidreceiver', 'p_syschangehistoryrowid', p_syschangehistoryrowid::VARCHAR)
                                    , 'p_systemidreceiver',
                     p_systemidreceiver::VARCHAR), 'l_schemasender',
             l_schemasender);
        ELSE
            INSERT INTO t_message (message)
            SELECT 'exHistory ' || l_exhistoryid::VARCHAR || ' found on ' ||
                   l_schemasender;
        END IF;
    ELSE
        INSERT INTO t_message (message)
        VALUES (l_schemasender || ' does not exist on the current data server');
    END IF;

    PERFORM SET_CONFIG('search_path', l_schemadataserver, TRUE);

    IF l_exhistoryid IS NULL
    THEN
        SELECT id, exhistorybatchid
        INTO l_exhistoryid, l_exhistorybatchid
        FROM exhistory a
        WHERE a.syschangehistoryrowidexported = p_sysChangeHistoryRowId
          AND a.exsystemiddestination = p_systemidreceiver;

    END IF;

    IF l_exhistoryid IS NOT NULL
    THEN

        IF NOT EXISTS(
                SELECT *
                FROM exhistorybatch a
                WHERE ID = l_exhistorybatchid)
        THEN
            INSERT INTO t_message (message)
            SELECT REPLACE(REPLACE('exHistoryBatch record l_exhistorybatchid does not exist on data server
select *
from l_schemadataserver.exhistorybatch a
where id = l_exhistorybatchid
'
                               , 'l_exhistorybatchid',
                                   l_exhistorybatchid::VARCHAR)
                       , 'l_schemadataserver',
                           l_schemadataserver);
        ELSE
            INSERT INTO t_message (message)
            SELECT 'exHistoryBatch ' || l_exhistorybatchid::VARCHAR ||
                   ' found on ' || l_schemadataserver;
        END IF;

        IF NOT EXISTS(
                SELECT *
                FROM exhistorybatch a
                WHERE ID = l_exhistorybatchid
                  AND distributiondate IS NOT NULL
            )
        THEN
            INSERT INTO t_message (message)
            SELECT REPLACE(REPLACE('exHistoryBatch l_exhistorybatchid exists on data server but has not been distributed.  Has spexPackageDistribute run?
select *
from l_schemadataserver.exhistorybatch a
where id = l_exhistorybatchid
'
                               , 'l_exhistorybatchid',
                                   l_exhistorybatchid::VARCHAR)
                       , 'l_schemadataserver',
                           l_schemadataserver);
        END IF;
        IF NOT EXISTS(
                SELECT *
                FROM exhistory a
                WHERE ID = l_exhistoryid)
        THEN
            INSERT INTO t_message (message)
            SELECT REPLACE(REPLACE('exhistory record l_exhistoryid does not exist on data server
select *
from l_schemadataserver.exhistory a
where id = l_exhistoryid
'
                               , 'l_exhistoryid', l_exhistoryid::VARCHAR)
                       , 'l_schemadataserver',
                           l_schemadataserver);
        ELSE
            INSERT INTO t_message (message)
            SELECT 'exHistory ' || l_exhistoryid::VARCHAR ||
                   ' found on ' || l_schemadataserver;
        END IF;
    END IF;

    IF NOT EXISTS(
            SELECT *
            FROM syschangehistoryrow a
            WHERE ID = p_syschangehistoryrowid)
    THEN
        INSERT INTO t_message (message)
        SELECT REPLACE(REPLACE('syschangehistoryrow record l_syschangehistoryrowid does not exist on data server
select *
from l_schemadataserver.syschangehistoryrow a
where id = l_syschangehistoryrowid
'
                           , 'l_syschangehistoryrowid',
                               p_syschangehistoryrowid::VARCHAR)
                   , 'l_schemadataserver',
                       l_schemadataserver);
    ELSE
        INSERT INTO t_message (message)
        SELECT 'sysChangeHistoryRow ' || p_sysChangeHistoryRowId::VARCHAR ||
               ' found on ' || l_schemadataserver;
    END IF;

    IF EXISTS(SELECT n.nspname, c.relname
              FROM pg_catalog.pg_class c
              LEFT JOIN pg_catalog.pg_namespace n
                        ON n.oid = c.relnamespace
              WHERE n.nspname = l_schemareceiver
                AND c.relname = 'syschangehistoryrow')
    THEN
        PERFORM SET_CONFIG('search_path', l_schemareceiver || ',' || l_searchpathcurrent, TRUE);

        IF l_exhistoryid IS NULL
        THEN
            SELECT id, exhistorybatchid
            INTO l_exhistoryid, l_exhistorybatchid
            FROM exhistory a
            WHERE a.syschangehistoryrowidexported = p_sysChangeHistoryRowId
              AND a.exsystemiddestination = p_systemidreceiver;

        END IF;

        IF l_exhistoryid IS NOT NULL
        THEN
            IF NOT EXISTS(
                    SELECT *
                    FROM exhistorybatch a
                    WHERE ID = l_exhistorybatchid)
            THEN
                INSERT INTO t_message (message)
                SELECT REPLACE(REPLACE('exHistoryBatch record l_exhistorybatchid does not exist on receiver system
select *
from l_schemareceiver.exhistorybatch a
where id = l_exhistorybatchid
'
                                   , 'l_exhistorybatchid',
                                       l_exhistorybatchid::VARCHAR)
                           , 'l_schemareceiver',
                               l_schemareceiver);
            ELSE
                INSERT INTO t_message (message)
                SELECT 'exHistoryBatch ' || l_exhistorybatchid::VARCHAR || ' found on ' ||
                       l_schemareceiver;
            END IF;

            IF NOT EXISTS(
                    SELECT *
                    FROM exhistorybatch a
                    WHERE ID = l_exhistorybatchid
                      AND applieddate IS NOT NULL
                )
            THEN
                INSERT INTO t_message (message)
                SELECT REPLACE(REPLACE('exHistoryBatch l_exhistorybatchid exists on receiver system but has not been applied.  Has spexPackageImport run?
select *
from l_schemareceiver.exhistorybatch a
where id = l_exhistorybatchid
'
                                   , 'l_exhistorybatchid',
                                       l_exhistorybatchid::VARCHAR)
                           , 'l_schemareceiver',
                               l_schemareceiver);
            END IF;
            IF NOT EXISTS(
                    SELECT *
                    FROM exhistory a
                    WHERE ID = l_exhistoryid)
            THEN
                INSERT INTO t_message (message)
                SELECT REPLACE(REPLACE('exhistory record l_exhistoryid does not exist on receiver system
select *
from l_schemareceiver.exhistory a
where id = l_exhistoryid
'
                                   , 'l_exhistoryid', l_exhistoryid::VARCHAR)
                           , 'l_schemareceiver',
                               l_schemareceiver);
            ELSE
                INSERT INTO t_message (message)
                SELECT 'exHistory ' || l_exhistoryid::VARCHAR || ' found on ' ||
                       l_schemareceiver;
            END IF;

        END IF;

        IF NOT EXISTS(
                SELECT *
                FROM syschangehistoryrow a
                WHERE ID = p_syschangehistoryrowid)
        THEN
            INSERT INTO t_message (message)
            SELECT REPLACE(REPLACE('sysChangeHistoryRow record l_syschangehistoryrowid does not exist on receiver system
select *
from l_schemareceiver.syschangehistoryrow a
where id = l_syschangehistoryrowid
'
                               , 'l_syschangehistoryrowid',
                                   p_syschangehistoryrowid::VARCHAR)
                       , 'l_schemareceiver',
                           l_schemareceiver);
        ELSE
            INSERT INTO t_message (message)
            SELECT 'sysChangeHistoryRow ' || p_sysChangeHistoryRowId::VARCHAR ||
                   ' found on ' || l_schemareceiver;
        END IF;

    ELSE
        INSERT INTO t_message (message)
        VALUES (l_schemareceiver || ' does not exist on the current data server');
    END IF;

    PERFORM SET_CONFIG('search_path', l_searchpathcurrent, TRUE);

    RETURN QUERY SELECT * FROM t_message;
END
$$ LANGUAGE plpgsql;
/*
set SEARCH_PATH TO s0002v0000, s0000v0000, public;
call spexPackageDistribute();
set SEARCH_PATH TO default;
*/