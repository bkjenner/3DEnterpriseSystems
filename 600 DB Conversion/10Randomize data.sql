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
This script randomizes name data
*/
DO
$$

    BEGIN

        UPDATE crmcontact A
        SET firstname=preferredfirstname, preferredfirstname=NULL
        WHERE firstname IS NULL
          AND preferredfirstname IS NOT NULL;

        UPDATE crmcontact a
        SET lastname = leftfind(name, ',')
        WHERE lastname IS NULL
          AND name LIKE '%,%'
          AND name NOT ILIKE '%alberta%'
          AND name NOT ILIKE '%inc%'
          AND name NOT ILIKE '%.%'
          AND name NOT ILIKE '%the%'
          AND name NOT ILIKE '%aadac%'
          AND name NOT ILIKE '%&%';

        UPDATE crmcontact a
        SET firstname = rightfind(name, ',')
        WHERE lastname IS NULL
          AND name LIKE '%,%'
          AND name NOT ILIKE '%alberta%'
          AND name NOT ILIKE '%inc%'
          AND name NOT ILIKE '%.%'
          AND name NOT ILIKE '%the%'
          AND name NOT ILIKE '%aadac%'
          AND name NOT ILIKE '%&%';

        UPDATE crmcontact a
        SET rowstatus='a';

        IF fnIfTableExists('sprandomfirstname') = FALSE
        THEN
            CREATE TABLE spRandomFirstName
            (
                id           BIGSERIAL
                    CONSTRAINT spRandomFirstName_pkey
                        PRIMARY KEY,
                firstname    VARCHAR,
                newfirstname VARCHAR
            );

            CREATE TABLE spRandomLastName
            (
                id          BIGSERIAL
                    CONSTRAINT spRandomLastName_pkey
                        PRIMARY KEY,
                lastname    VARCHAR,
                newlastname VARCHAR
            );

            INSERT INTO spRandomFirstName (FirstName)
            SELECT DISTINCT firstname
            FROM crmcontact
            WHERE firstname IS NOT NULL;

            INSERT INTO spRandomLastName (LastName)
            SELECT DISTINCT lastname
            FROM crmcontact
            WHERE lastname IS NOT NULL;
        END IF;

        UPDATE spRandomFirstName a
        SET newfirstname=b.firstname
        FROM spRandomFirstName b
        WHERE b.ID = a.id + 1;

        UPDATE spRandomFirstName a
        SET newfirstname=b.firstname
        FROM spRandomFirstName b
        WHERE b.ID = 1
          AND a.newfirstname IS NULL;

        UPDATE spRandomlastName a
        SET newlastname=b.lastname
        FROM spRandomlastName b
        WHERE b.ID = a.id + 1;

        UPDATE spRandomlastName a
        SET newlastname=b.lastname
        FROM spRandomlastName b
        WHERE b.ID = 1
          AND a.newlastname IS NULL;

        UPDATE crmcontact a
        SET firstname = b.newfirstname
        FROM sprandomfirstname b
        WHERE b.firstname = a.firstname
          AND a.id <> 100000000353347;

        UPDATE crmcontact a
        SET lastname = b.newlastname
        FROM sprandomlastname b
        WHERE b.lastname = a.lastname
          AND a.id <> 100000000353347;

        UPDATE crmcontact A
        SET name=a.lastname || ', ' || firstname
        WHERE a.lastname IS NOT NULL
          AND a.firstname IS NOT NULL;

        UPDATE glbillingaccount a
        SET name = c.name || ' - ' || b.description
        FROM glaccount b,
             crmcontact c
        WHERE a.glaccountid = b.id
          AND a.crmcontactid = c.id;

        DROP TABLE IF EXISTS spRandomContactPerson;
        CREATE TABLE IF NOT EXISTS spRandomContactPerson
        (
            id                  BIGSERIAL
                CONSTRAINT spRandomContactPerson_pkey
                    PRIMARY KEY,
            crmContactID        BIGINT,
            spRandomFirstNameID BIGINT,
            ContactPerson       VARCHAR
        );

        INSERT INTO spRandomContactPerson (crmContactID)
        SELECT id
        FROM crmcontact
        WHERE contactperson IS NOT NULL;

        UPDATE sprandomcontactperson
        SET spRandomFirstNameID = CASE WHEN id > (
                                                     SELECT MAX(id)
                                                     FROM sprandomfirstname) THEN id - (
                                                                                           SELECT MAX(id)
                                                                                           FROM sprandomfirstname)
                                       ELSE id
                                       END;

        UPDATE crmcontact a
        SET contactperson=b.contactperson
        FROM (
                 SELECT aa.crmcontactid, cc.firstname || ' ' || bb.lastname contactperson
                 FROM sprandomcontactperson aa
                 JOIN sprandomlastname bb
                      ON bb.id = aa.id
                 JOIN spRandomFirstName cc
                      ON cc.id = aa.sprandomfirstnameid) b
        WHERE b.crmContactID = a.id;

        --select * from crmcontact where contactperson is not null;

    END
$$

/*
-- Create a list of unique words to randomize
DO
$$
    DECLARE
        l_string VARCHAR;
    BEGIN

        SELECT STRING_AGG('
select ''' || ARRAY_TO_STRING(STRING_TO_ARRAY(REPLACE(replace(replace(name,'.',''),',',''), '  ', ' '), ' '), ''' word union all
select ''', '*') || ''' union all', '')
        INTO l_string
        FROM crmcontact
        WHERE lastname IS NULL
		AND name not like '%''%';

        l_string := 'select distinct word from (' || LEFT(l_string, LENGTH(l_string) - 10) || '
) a where word not in (''inc'','' '',''and'',''of'')
and lower(word) > ''a1''
order by word
';
        RAISE NOTICE '%',l_string;
    END;
$$ LANGUAGE plpgsql

 */