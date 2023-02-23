DROP VIEW IF EXISTS S0000V0000.vwcrmcontactaddress CASCADE;
CREATE VIEW S0000V0000.vwcrmcontactaddress
AS
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
This view is the primary view for displaying addresses.  Note addresses are typically connected to add contact
but can also be connected to add location.

20210608    Blair Kjenner   Initial Code

*/
SELECT add.id,
       at.description                       AS addresstype,
       crm.name                             AS contactname,
       add.address1,
       add.address2,
       add.address3,
       add.address4,
       COALESCE(cit.name, add.city)           AS city,
       COALESCE(prv.name, add.provincestate) AS provincestate,
       cou.name,
       add.postalzip,
       add.additionalinformation,
       add.isprimaryaddress,
       add.crmaddressidinheritedfrom,
       add.comcityid,
       cit.comprovincestateid,
       cit.comcountryid,
       add.crmaddresstypeid,
       crm.id                                  AS crmContactID,
       add.effectivedate,
       add.rowstatus
FROM crmaddress add
JOIN      crmcontact crm
          ON crm.id = add.crmContactID
LEFT JOIN comcity cit
          ON cit.id = add.comcityid
LEFT JOIN comprovincestate prv
          ON prv.id = cit.comprovincestateid
LEFT JOIN comcountry cou
          ON cou.id = cit.comcountryid
LEFT JOIN crmaddresstype at
          ON at.id = add.crmaddresstypeid
WHERE add.rowstatus = 'a';

