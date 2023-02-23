CREATE OR REPLACE FUNCTION S0000V0000.fnsysIDCreate(p_table VARCHAR DEFAULT NULL, p_RowID BIGINT DEFAULT NULL,
                                                    p_systemid INT DEFAULT NULL, p_schema VARCHAR DEFAULT NULL)
    RETURNS BIGINT AS
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
This function creates system ids based table, row id and system id.  The max sizes of System IDs and
Record ID based on a BIGINT will be as follows.

     Max System IDs 1,844,672
     Max Record ID value 10 to power of 15 (quadrillion)

If a row ID is not passed (which it typically isn't), the system will use the identity increment.
The premise is that when we create Row IDs in a given system we are only ever creating values
based on the system id for the system.

Having said that, we will be importing ids from other systems but in those cases the
Row ID portion of the ID will already by populated.

The benefits of the format 0 bigint id are:
- It is much smaller than a GUID
- It will work well with clustered indexes (because it is sequential)
- We can support 1,844,672 different system ids before we have to do something different
- We can support a quadiollion (10**15-1) rows which is considerably more than an INT 147,483,648
- We can create an id and move it anywhere without colliding
- For System 0, the IDS will look as native INT's

20210310    Blair Kjenner   Initial Code

SELECT fnsysidcreate(p_systemid:=1844672, p_rowid:=9999999999999::bigint)

SELECT fnsysidcreate(p_rowid:=1::bigint)

*/
DECLARE
    l_rowid   BIGINT;
    l_id      BIGINT;
    l_sign    SMALLINT = 1;
    l_Error   RECORD;
    e_State   TEXT;
    e_Msg     TEXT;
    e_Context TEXT;
BEGIN
    p_schema:=coalesce(p_schema, CURRENT_SCHEMA());

    IF p_systemid IS NULL
    THEN
        p_systemid := midfind(p_schema, 's', 'v')::INT;
    END IF;
    IF p_systemid < 0 OR p_systemid > 1844672
    THEN
        RAISE SQLSTATE '51026' USING MESSAGE = p_systemid;
    END IF;
    IF p_rowid IS NULL
    THEN
        l_rowid := NEXTVAL(p_schema||'.'||p_table || '_id_seq');
    ELSE
        l_rowid = p_RowID;
    END IF;
    IF l_rowid < 0 OR l_rowid > 9999999999999
    THEN
        RAISE SQLSTATE '51027' USING MESSAGE = l_rowid;
    END IF;

    IF p_systemid > 922336
    THEN
        p_systemid := p_systemid - 922336;
        l_sign := -1;
    END IF;

    l_ID := ((p_systemid * 10000000000000) + l_rowid) * l_sign;

    RETURN l_ID;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            e_State = RETURNED_SQLSTATE,
            e_Msg = MESSAGE_TEXT,
            e_Context = PG_EXCEPTION_CONTEXT;
        l_Error := fnsysError(e_State, e_Msg, e_Context);
        IF l_error.IsExceptionRaised = TRUE
        THEN
            RAISE EXCEPTION '%', l_Error.Message;
        ELSE
            IF l_error.IsExceptionRaised = TRUE
            THEN
                RAISE EXCEPTION '%', l_Error.Message;
            ELSE
                RAISE NOTICE '%', l_Error.Message ;
            END IF;
        END IF;
END;
$$
    LANGUAGE plpgsql;
