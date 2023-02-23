SET search_path TO _staging, s0000v0000,public;

DROP TABLE IF EXISTS t_scramble;

CREATE TEMP TABLE t_scramble
(
    FromText VARCHAR,
    ToText   VARCHAR
);

COPY t_scramble FROM 'c:\temp\BMSData\aascramble.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

CREATE OR REPLACE PROCEDURE spScramble(p_tablename varchar, p_columnname varchar)
AS
$$
    DECLARE
        l_SQL VARCHAR;
        l_template VARCHAR;
        l_rec RECORD;

    BEGIN

        l_template := '
UPDATE l_tablename SET l_columnname=REGEXP_REPLACE(l_columnname, ''l_fromtext'', ''l_totext'',''i'') WHERE l_columnname ~* ''(\ml_fromtext\M)'';
        ';

        FOR l_rec IN SELECT FromText,
                            ToText
                     FROM t_scramble f
        LOOP
            l_SQL := replace(replace(replace(replace(l_template,'l_tablename',p_tablename),'l_columnname',p_columnname),'l_fromtext',l_rec.FromText),'l_totext',l_rec.ToText);
            --raise notice '%', l_SQL;
            EXECUTE l_SQL;

        END LOOP;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE spScramblenum(p_tablename varchar, p_columnname varchar)
AS
$$
    DECLARE
        l_SQL VARCHAR;
        l_template VARCHAR;
        l_rec RECORD;

    BEGIN

        l_template := '
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''2'', ''7'') WHERE l_columnname like ''%2%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''3'', ''4'') WHERE l_columnname like ''%3%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''4'', ''8'') WHERE l_columnname like ''%4%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''5'', ''9'') WHERE l_columnname like ''%5%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''6'', ''2'') WHERE l_columnname like ''%6%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''7'', ''5'') WHERE l_columnname like ''%7%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''8'', ''3'') WHERE l_columnname like ''%8%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''9'', ''6'') WHERE l_columnname like ''%9%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''530'', ''780'') WHERE l_columnname like ''%530%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''804'', ''403'') WHERE l_columnname like ''%804%'';
UPDATE l_tablename SET l_columnname=REPLACE(l_columnname, ''300'', ''800'') WHERE l_columnname like ''%300%'';

        ';

        l_SQL := replace(replace(l_template,'l_tablename',p_tablename),'l_columnname',p_columnname);
        --raise notice '%', l_SQL;
        EXECUTE l_SQL;

    END;
$$ LANGUAGE plpgsql;

call spscramble('Contacts', 'Name');
call spscramble('Contacts', 'Address1');
call spscramble('Contacts', 'Address2');
call spscramble('Contacts', 'Province');
call spscramble('Contacts', 'MailingAddress1');
call spscramble('Contacts', 'MailingAddress2');
call spscramble('Contacts', 'Comments');
call spscramble('Contacts', 'Contact');
call spscramble('PeopleDatabase', 'FirstName');
call spscramble('PeopleDatabase', 'MiddleName');
call spscramble('PeopleDatabase', 'LastName');
call spscramble('PeopleDatabase', 'GivenName');
call spscramble('Relationships', 'Comments');
call spscramble('Activities', 'Description');
call spscramble('ActivityProjects', 'Description');
call spscramble('ActivityProjects', 'Contact');
call spscramble('Employees', 'FirstName');
call spscramble('Employees', 'MiddleName');
call spscramble('Employees', 'LastName');
call spscramble('Employees', 'Comments');
call spscramble('PersonalComments', 'Comments');
call spscramble('GLEntries', 'Description');
call spscramble('BillingAccounts', 'Name');
call spscramble('TransactionsCheques', 'Name');
call spscramble('TransactionsCheques', 'Address1');
call spscramble('TransactionsCheques', 'Address2');
call spscramble('Transactions', 'Description');
call spscramblenum('Contacts', 'Phone1');
call spscramblenum('Contacts', 'Phone2');
call spscramblenum('Contacts', 'Address1');
call spscramblenum('Contacts', 'Address2');
call spscramblenum('Contacts', 'MailingAddress1');
call spscramblenum('Contacts', 'MailingAddress2');
call spscramblenum('Contacts', 'Postal');
call spscramblenum('Contacts', 'Comments');
call spscramblenum('Employees', 'Comments');

update contacts set email=trim(replace(replace(replace(replace(leftfind(name,','),' ',''),'.',''),'(',''),')','')||'@'||rightfind(email,'@')) where trim(coalesce(email,'')) <> '';
update contacts set email=email||'shaw.ca' where right(email,1) = '@';

DROP TABLE IF EXISTS t_scramble;
