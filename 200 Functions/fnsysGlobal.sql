CREATE OR REPLACE FUNCTION S0000V0000.fnsysGlobal(p_variable varchar)
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

This function allows us to use global variables to reference ids in code that we often require.
It also allows us to personalize the ids by system in case there are overrides.

20210217    Blair Kjenner   Initial Code

select fnsysGlobal('ps-posted')

*/
SELECT CASE
           WHEN LOWER(p_variable) = 'ps-pending'         THEN 1
           WHEN LOWER(p_variable) = 'ps-posted'          THEN 2
           WHEN LOWER(p_variable) = 'bs-pending'         THEN 1
           WHEN LOWER(p_variable) = 'bs-approved'        THEN 2
           WHEN LOWER(p_variable) = 'bs-exported'        THEN 3
           WHEN LOWER(p_variable) = 'bs-integrated'      THEN 4
           WHEN LOWER(p_variable) = 'ml.glentry.bl'      THEN 392384
           WHEN LOWER(p_variable) = 'ml.glentry.fa'      THEN 392384
           WHEN LOWER(p_variable) = 'ml.glentry.cl'      THEN 392384
           WHEN LOWER(p_variable) = 'ml.glentry.ap'      THEN 392384
           WHEN LOWER(p_variable) = 'dt-activity'        THEN 420
           WHEN LOWER(p_variable) = 'dt-project'         THEN 401
           WHEN LOWER(p_variable) = 'dt-batch'           THEN 310
           WHEN LOWER(p_variable) = 'dt-billingaccount'  THEN 321
           WHEN LOWER(p_variable) = 'dt-contact'         THEN 100
           WHEN LOWER(p_variable) = 'dt-fixed asset'     THEN 200
           WHEN LOWER(p_variable) = 'dt-employee'        THEN 101
           WHEN LOWER(p_variable) = 'dt-location'        THEN 503
           WHEN LOWER(p_variable) = 'dt-glentry'         THEN 316
           WHEN LOWER(p_variable) = 'dt-relationship'    THEN 120
           WHEN LOWER(p_variable) = 'dt-disposalreason'  THEN 207
           WHEN LOWER(p_variable) = 'dt-transaction'     THEN 311
           WHEN LOWER(p_variable) = 'dt-cashreceipt'     THEN 312
           WHEN LOWER(p_variable) = 'dt-dictionarytable' THEN 1
           WHEN LOWER(p_variable) = 'sys-maxcangehistoryinbatch' THEN 100
           WHEN LOWER(p_variable) = 'sys-temporalresolution' THEN 3 -- Default temporal resolution of a day
           END
$$ LANGUAGE sql IMMUTABLE;

