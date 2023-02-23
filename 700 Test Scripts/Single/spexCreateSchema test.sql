DO
$$
    DECLARE
        l_SQL TEXT;
    BEGIN
        SELECT STRING_AGG(FORMAT('
call spsysschemacreate (
p_firstname:=''%s'',
p_lastname:=''%s'',
p_username:=''%s'',
p_email:=''%s'',
p_organizationname:=''%s'',
p_systemname := ''%s'');
'
                              , leftfind(contactperson, ' ')
                              , rightfind(contactperson, ' ')
                              , LEFT(contactperson, 1) || rightfind(contactperson, ' ')
                              , email
                              , contactname
                              , contactname), '')
        INTO l_SQL
        FROM (
                 SELECT fixquote(contactperson) contactperson,
                        fixquote(name)          contactname,
                        fixquote(email)         email
                 FROM crmcontact aa
                 JOIN crmaddressemail bb
                      ON bb.crmcontactid = aa.id
                 WHERE LENGTH(aa.contactperson) - LENGTH(REPLACE(aa.contactperson, ' ', '')) = 1
                 LIMIT 10) a;

        RAISE NOTICE '%', l_SQL;
        SET CLIENT_MIN_MESSAGES = WARNING;
        EXECUTE (l_SQL);
        SET CLIENT_MIN_MESSAGES = NOTICE;
    END
$$
/*
--DROP All schemas
DO
$$
    DECLARE
        l_sql VARCHAR;
    BEGIN
        SELECT STRING_AGG(FORMAT('DROP SCHEMA IF EXISTS %I CASCADE;', nspname), E'\n')
        INTO l_SQL
        FROM pg_namespace
        WHERE nspname LIKE 's00%'
          AND nspname > 's0002v0000';

        l_sql := '
SET CLIENT_MIN_MESSAGES = WARNING;
' || l_SQL || '
SET CLIENT_MIN_MESSAGES = NOTICE;
';
        EXECUTE l_SQL;
        --RAISE NOTICE '%', l_SQL;
    END
$$

*/