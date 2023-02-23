CREATE OR REPLACE PROCEDURE S0000V0000.spglTransactionDelete(p_gltransactionid BIGINT DEFAULT NULL)
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


 call spglTransactionDelete

 */
BEGIN
    IF p_gltransactionid IS NULL
    THEN
        p_gltransactionid := (
                                 SELECT MAX(id)
                                 FROM gltransaction);
    END IF;
    UPDATE gltransaction SET glentryidmain=NULL WHERE id = p_gltransactionid;
    DELETE
    FROM glreconciliation
    WHERE glentryidfrom IN (
                               SELECT id
                               FROM glentry
                               WHERE gltransactionid = p_gltransactionid);
    DELETE
    FROM glreconciliation
    WHERE glentryidto IN (
                             SELECT id
                             FROM glentry
                             WHERE gltransactionid = p_gltransactionid);
    DELETE FROM glentry WHERE gltransactionid = p_gltransactionid;
    DELETE FROM gltransactionsubtypecashreceipt WHERE id = p_gltransactionid;
    DELETE FROM gltransaction WHERE id = p_gltransactionid;

END;
$$
    LANGUAGE plpgsql;

