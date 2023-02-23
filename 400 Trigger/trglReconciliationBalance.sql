DROP TABLE IF EXISTS glReconciliationOld;
CREATE TABLE glReconciliationOld (id BIGINT);
DROP TABLE IF EXISTS glReconciliationNew;
CREATE TABLE glReconciliationNew (id BIGINT);

DROP FUNCTION IF EXISTS S0000V0000.trglReconciliationBalance() CASCADE;
CREATE FUNCTION S0000V0000.trglReconciliationBalance()
RETURNS TRIGGER AS
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
This trigger updates reconciliation balance on glentries based on the glreconciliation record

20210306    Blair Kjenner   Initial Code

*/
DECLARE
    r1 RECORD;
BEGIN
    DROP TABLE IF EXISTS t_changes;
    CREATE TEMP TABLE t_changes
    (
        GLEntryIDTo BIGINT,
        Amount      DECIMAL(19, 2)
    );
    IF (TG_OP = 'DELETE')
    THEN
        INSERT INTO t_changes
        SELECT glEntryIDTo, amount * -1
        FROM glReconciliationOld;
    ELSIF (TG_OP = 'UPDATE')
    THEN
        INSERT INTO t_changes
        SELECT glEntryIDTo, amount * -1
        FROM glReconciliationOld
        UNION ALL
        SELECT glEntryIDTo, amount
        FROM glReconciliationNew;
    ELSIF (TG_OP = 'INSERT')
    THEN
        INSERT INTO t_changes
        SELECT glEntryIDTo, amount
        FROM glReconciliationNew;
    END IF;
/*
The following select takes The inserted glReconciliation and The deleted glReconciliation and
unions them.  The deleted glReconciliation The amount is negated
Then we sum them so if The update does not affect The glEntryIDTo or The amount The
Balance will be zero and no updates will occur
*/
    FOR r1 IN SELECT glEntryIDTo,
                     SUM(amount) AS Amount
              FROM t_changes
              GROUP BY glEntryIDTo
              HAVING SUM(amount) <> 0
    LOOP
        IF r1.Amount <> 0
        THEN
            UPDATE glEntry
            SET ReconciliationBalance = (CASE WHEN COALESCE(ReconciliationBalance, Amount) + r1.Amount = Amount
                                                 THEN NULL
                                                 ELSE COALESCE(ReconciliationBalance, Amount) + r1.Amount
                                             END)
            WHERE ID = r1.glEntryIDTo;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

'Distribute and import' IF EXISTS glReconciliation_ins ON glReconciliation;
CREATE TRIGGER glReconciliation_ins
    AFTER INSERT
    ON glReconciliation
    REFERENCING NEW TABLE AS glReconciliationNew
    FOR EACH STATEMENT
EXECUTE PROCEDURE trglReconciliationBalance();

DROP TRIGGER IF EXISTS glReconciliation_upd ON glReconciliation;
CREATE TRIGGER glReconciliation_upd
    AFTER UPDATE
    ON glReconciliation
    REFERENCING OLD TABLE AS glReconciliationOld NEW TABLE AS glReconciliationNew
    FOR EACH STATEMENT
EXECUTE PROCEDURE trglReconciliationBalance();

DROP TRIGGER IF EXISTS glReconciliation_del ON glReconciliation;
CREATE TRIGGER glReconciliation_del
    AFTER DELETE
    ON glReconciliation
    REFERENCING OLD TABLE AS glReconciliationOld
    FOR EACH STATEMENT
EXECUTE PROCEDURE trglReconciliationBalance();

DROP TABLE IF EXISTS glReconciliationOld;
DROP TABLE IF EXISTS glReconciliationNew;
