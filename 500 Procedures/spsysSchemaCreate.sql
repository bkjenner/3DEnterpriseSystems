CREATE OR REPLACE PROCEDURE S0000V0000.spsysSchemaCreate(
    p_SystemName VARCHAR,
    p_FirstName VARCHAR DEFAULT NULL,
    p_LastName VARCHAR DEFAULT NULL,
    p_UserName VARCHAR DEFAULT NULL,
    p_Email VARCHAR DEFAULT NULL,
    p_OrganizationName VARCHAR DEFAULT NULL,
    p_IsSubnetServer BOOLEAN DEFAULT FALSE,
    p_PasswordHash VARCHAR DEFAULT NULL,
    p_exSystemID INT DEFAULT NULL,
    p_Turnontriggersandconstraints BOOLEAN DEFAULT NULL,
    p_debug BOOLEAN DEFAULT FALSE
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
OVERVIEW

This procedure sets up a new schema based on the parameters passed.  It performs the following tasks

- Creates a system entry in the system table on the subnet server
- Creates a new schema based on the next available system number and the template
- Insert new user
- Inserts a company record
- Inserts a relationship record
- Copies the inserted records to the subnet server?

CHANGE LOG
20211010    Blair Kjenner   Initial Code

PARAMETERS
p_FirstName - First name for Root User
p_LastName  - Last name for Root User
p_UserName -  - User name for Root User
p_Email  - Email address for Root User
p_OrganizationName - Company name for Root User
P_SystemName - Name of System
p_PasswordHash  [OPTIONAL] - Password for Root User
p_exSystemID - Used to assign a new schema to a specific id.  Primarily used for testing.

SAMPLE CALL

call spsysSchemaCreate (
p_FirstName:='Bill',
p_LastName:='Smith',
p_UserName:='bsmith',
p_Email:='bsmith@gmail.com',
p_OrganizationName:='bill smith Inc.',
p_systemname := 'Organization A',
p_IsSubnetServer := FALSE,
p_PasswordHash:='Password@123');

 */
DECLARE
    l_crmContactIDOrganization BIGINT;
    l_crmContactIDUser         BIGINT;
    l_currentSearchPath        VARCHAR;
    l_SubnetServerName         VARCHAR;
    l_DestSchema               VARCHAR;
    l_exSubnetServerSystemID   INT;
    l_exSubnetServerSchemaName VARCHAR;
    l_exSystemID               INT;
    l_syschangehistoryid       BIGINT;
    l_exSubnetServerid         INT;
BEGIN

    -- Default the triggers and constraints to off for the subnet server
    -- Data is just flowing through this system and we dont need to generate
    -- change history to check foreign keys (like the sysDictionaryColumnId on sysChangeHistoryColumn)
    if p_IsSubnetServer and p_Turnontriggersandconstraints is NULL
    then
        p_Turnontriggersandconstraints := false;
    end if;

    l_exSubnetServerid := fnsyscurrentSubnetServerid();
    l_SubnetServerName := CURRENT_SCHEMA();

    -- The following function creates the a system on the Master subnet server and returns an ID to us
    SELECT fnsysMDSGetNextSystemID(p_SystemName, p_IsSubnetServer, p_exSystemID) INTO l_exSystemID;

    DROP TABLE IF EXISTS t_exsystem;
    CREATE TEMP TABLE t_exsystem
    AS
    SELECT *
    FROM vwexsystemaws
    WHERE id = l_exSystemID
       OR id IN (
                    SELECT systemidSubnetServer
                    FROM vwexSubnetServeraws
                    WHERE ID = l_exSubnetServerid);

    SELECT schemaname, exSubnetServerSystemID, exSubnetServerSchema
    INTO l_destschema, l_exSubnetServerSystemID, l_exSubnetServerSchemaName
    FROM t_exsystem
    WHERE id = l_exSystemID;

    CALL spsysSchemaCopy(p_sourceschema := 's0000v0000', p_destschema := l_DestSchema, p_sourceprefix := 'c_',
                         p_SystemName := p_SystemName);

    CALL spsysViewCreate(p_schemaname := l_destschema, p_debug := p_debug);

    l_currentSearchPath := CURRENT_SETTING('search_path');
    PERFORM SET_CONFIG('search_path', l_destschema || ',' || CURRENT_SETTING('search_path'), FALSE);

    IF p_Turnontriggersandconstraints
    THEN
        CALL spsysChangeHistoryRefreshTriggers();
        CALL spsysForeignKeyConstraintGenerate();
    END IF;

    INSERT INTO syschangehistory (changedate, comments)
    VALUES (NOW()::TIMESTAMP, '
FirstName - ' || p_FirstName || '
LastName - ' || p_LastName || '
UserName - ' || p_UserName || '
Email - ' || p_Email || '
OrganizationName - ' || p_OrganizationName || '
SystemName - ' || p_SystemName || '
exSubnetServerID - ' || l_exSubnetServerid::VARCHAR)
    RETURNING id INTO l_syschangehistoryid;

    INSERT INTO exSubnetServer (id, systemidSubnetServer, name, rowstatus, syschangehistoryid)
    SELECT a.id, a.systemidSubnetServer, a.name, a.rowstatus, a.syschangehistoryid
    FROM vwexSubnetServeraws a;

    INSERT INTO exsystem (id, exSubnetServerid, name, schemaname, productionversion, testversion, rowstatus, syschangehistoryid)
    SELECT id,
           exSubnetServerid,
           name,
           schemaname,
           productionversion,
           testversion,
           rowstatus,
           l_syschangehistoryid
    FROM t_exsystem a;

    IF p_firstname IS NOT NULL OR p_lastname IS NOT NULL
    THEN
        INSERT INTO crmContact (firstname, lastname, name, rowstatus, syschangehistoryid)
        VALUES (p_FirstName, p_LastName, p_LastName || ', ' || p_FirstName, 'a', l_syschangehistoryid)
        RETURNING id INTO l_crmContactIDUser;

        UPDATE syschangehistory s
        SET sysdictionarytableidappliesto=100, rowidappliesto=l_crmContactIDUser
        WHERE id = l_syschangehistoryid;

        INSERT INTO crmcontactsubtypeuser (id, crmcontactid, login, password, rowstatus, syschangehistoryid)
        VALUES (l_crmContactIDUser, l_crmContactIDUser, p_UserName, p_PasswordHash, 'a', l_syschangehistoryid);

        INSERT INTO crmaddressemail (crmaddresstypeid, crmcontactid, email, isprimary, syschangehistoryid)
        VALUES (30 /*business*/, l_crmContactIDUser, p_email, TRUE, l_syschangehistoryid);
    END IF;

    IF p_OrganizationName IS NOT NULL
    THEN
        INSERT INTO crmContact (name, rowstatus, syschangehistoryid)
        VALUES (p_OrganizationName, 'a', l_syschangehistoryid)
        RETURNING id INTO l_crmContactIDOrganization;
        UPDATE glsetup SET crmcontactidcompany=l_crmContactIDOrganization;
    END IF;

    IF (p_firstname IS NOT NULL OR p_lastname IS NOT NULL)
        AND p_OrganizationName IS NOT NULL
    THEN
        INSERT INTO crmrelationship (crmcontactid1, crmcontactid2, crmrelationshiptypeid, syschangehistoryid)
        VALUES (l_crmContactIDUser, l_crmContactIDOrganization, (
                                                                    SELECT id
                                                                    FROM crmrelationshiptype
                                                                    WHERE LOWER(primaryname) = 'employee'), l_syschangehistoryid),
               (l_crmContactIDOrganization, l_crmContactIDUser, (
                                                                    SELECT id
                                                                    FROM crmrelationshiptype
                                                                    WHERE LOWER(primaryname) = 'employer'), l_syschangehistoryid);

        IF p_Turnontriggersandconstraints
        THEN
            CALL spexPackageExport(l_syschangehistoryid, p_debug);
        END IF;
    END IF;

    PERFORM SET_CONFIG('search_path', l_currentSearchPath, FALSE);
END;

$$
    LANGUAGE plpgsql
