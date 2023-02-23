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
This script creates a schema based on the data dictionary
*/
DO
$$
    DECLARE
        p_systemid            INT     := 1;
        p_schema              VARCHAR := 's0001v0000';
        l_PrevTableName       VARCHAR := '';
        l_PrevIsTableTemporal BOOLEAN := FALSE;
        l_tString             VARCHAR;
        l_FirstColumn         BOOLEAN;
        l_SQL                 VARCHAR := '';
        l_cur                 RECORD;
        l_currentTableName    VARCHAR := '';
    BEGIN

        ALTER DATABASE sn000001 SET search_path TO S0001V0000, S0000V0000, PUBLIC;

        SET SEARCH_PATH TO PUBLIC;
        CREATE EXTENSION IF NOT EXISTS dblink;
        SET search_path TO DEFAULT;

        DROP SCHEMA IF EXISTS s0001v0000 CASCADE;
        CREATE SCHEMA IF NOT EXISTS s0001v0000;

        CREATE SEQUENCE sysDictionaryColumn_id_seq;

        CREATE TABLE sysdictionarycolumn
        (
            id                           BIGINT  DEFAULT fnsysidcreate(p_table := 'sysDictionaryColumn',
                                                                       p_systemid := 1) NOT NULL
                PRIMARY KEY,
            sysdictionarytableid         BIGINT                                         NOT NULL,
            sysdictionarytableidforeign  BIGINT,
            columnsequence               INTEGER,
            datalength                   INT,
            datatype                     VARCHAR                                        NOT NULL,
            decimals                     INTEGER,
            defaultvalue                 VARCHAR,
            description                  VARCHAR,
            ischangehistoryused          BOOLEAN DEFAULT FALSE                          NOT NULL,
            isheadercolumn               BOOLEAN DEFAULT FALSE                          NOT NULL,
            isencrypted                  BOOLEAN DEFAULT FALSE                          NOT NULL,
            isnullable                   BOOLEAN DEFAULT FALSE                          NOT NULL,
            isincludedinuniqueconstraint BOOLEAN DEFAULT FALSE                          NOT NULL,
            label                        VARCHAR                                        NOT NULL,
            longname                     VARCHAR,
            name                         VARCHAR                                        NOT NULL,
            purpose                      VARCHAR                                        NOT NULL,
            rowstatus                    CHAR    DEFAULT 'a'::bpchar                    NOT NULL,
            syschangehistoryid           BIGINT
        );

        INSERT INTO sysdictionarycolumn(id, sysdictionarytableid, sysdictionarytableidforeign, columnsequence, datalength, datatype, decimals, defaultvalue, description, ischangehistoryused, isheadercolumn, isencrypted, isnullable, isincludedinuniqueconstraint, label, longname, name, purpose, rowstatus, syschangehistoryid)
        SELECT ID::BIGINT                       AS                      ID,
               dictionarytableid::BIGINT        AS                      sysdictionarytableid,
               dictionarytableidforeign::BIGINT AS                      sysdictionarytableidforeign,
               NULL::INT                                                columnsequence,
               CASE WHEN LOWER(jl1.datatype) IN ('char', 'varchar', 'nvarchar', 'nchar', 'decimal')
                        THEN datalength::INT
                    END::INTEGER                                        datalength,
               CASE WHEN COALESCE(decimals, 0) > 0 THEN 'decimal'
                    ELSE jl1.datatype
                    END::VARCHAR                                        datatype,
               CASE WHEN LOWER(jl1.datatype) = 'decimal' THEN decimals::INT
                    END::INTEGER                                        decimals,
               LOWER(defaultvalue)::VARCHAR                             defaultvalue,
               description::VARCHAR                                     description,
               UPPER(COALESCE(ischangehistoryused, 'N')) = 'Y'          ischangehistoryused,
               UPPER(COALESCE(isHeaderColumn, 'N')) = 'Y'               isHeaderColumn,
               UPPER(COALESCE(isEncrypted, 'N')) = 'Y'                  isEncrypted,
               UPPER(COALESCE(isnullable, 'Y')) = 'Y'                   isnullable,
               UPPER(COALESCE(isIncludedInUniqueConstraint, 'N')) = 'Y' isIncludedInUniqueConstraint,
               label::VARCHAR                                           label,
               longname::VARCHAR                                        longname,
               LOWER(name)::VARCHAR                                     name,
               LOWER(purpose)::VARCHAR                                  purpose,
               'a'::CHAR                                                rowstatus,
               NULL::BIGINT                                             syschangehistoryid
        FROM _staging.aadictionarycolumn a
        JOIN LATERAL (SELECT LOWER(COALESCE(a.datatype, 'null')) datatype) jl1
             ON TRUE
        WHERE systemname = 'NEWBMS';

        CREATE SEQUENCE sysDictionaryTable_id_seq;

        CREATE TABLE sysdictionarytable
        (
            id                  BIGINT DEFAULT fnsysidcreate(p_table := 'sysDictionaryTable',
                                                             p_systemid := 1) NOT NULL
                PRIMARY KEY,
            syscommandid        BIGINT,
            changehistorylevel  INTEGER,
            changehistoryscope  VARCHAR,
            description         VARCHAR,
            ischangehistoryused BOOLEAN,
            istabletemporal     BOOLEAN                                       NOT NULL,
            name                VARCHAR                                       NOT NULL,
            normalizedname      VARCHAR                                       NOT NULL,
            objectid            INTEGER,
            pluralname          VARCHAR,
            singularname        VARCHAR,
            systemmodule        VARCHAR                                       NOT NULL,
            tabletype           VARCHAR                                       NOT NULL,
            translation         VARCHAR,
            rowstatus           CHAR   DEFAULT 'a'::bpchar                    NOT NULL,
            syschangehistoryid  BIGINT
        );

        INSERT INTO sysdictionarytable (id, syscommandid, changehistorylevel, changehistoryscope, description, ischangehistoryused, istabletemporal, name, normalizedname, objectid, pluralname, singularname, systemmodule, tabletype, translation, rowstatus, syschangehistoryid)
        SELECT ID::BIGINT                         AS id,
               NULL::BIGINT                       AS syscommandid,
               NULL::INTEGER                      AS changehistorylevel,
               LOWER(changehistoryscope)::VARCHAR AS changehistoryscope,
               description::VARCHAR               AS description,
               NULL::BOOLEAN                      AS ischangehistoryused,
               UPPER(istabletemporal) = 'Y'       AS istabletemporal,
               LOWER(name)::VARCHAR               AS name,
               normalizedname::VARCHAR            AS normalizedname,
               NULL::INT                          AS objectid,
               pluralname::VARCHAR                AS pluralname,
               singularname::VARCHAR              AS singularname,
               systemmodule::VARCHAR              AS systemmodule,
               LOWER(tabletype)::VARCHAR          AS tabletype,
               translation::VARCHAR               AS translation,
               'a'::CHAR                          AS rowstatus,
               NULL::BIGINT                       AS syschangehistoryid
        FROM _staging.aadictionarytable
        WHERE systemname = 'NEWBMS';

        CALL spsysSchemaUpdate();

    END ;
$$ LANGUAGE plpgsql;

