-- Note if you drop trigger with cascade you will need to restore the trigger in all dbs
CREATE OR REPLACE FUNCTION S0000V0000.trexHistoryImportCall()
    RETURNS TRIGGER AS
$$
BEGIN
    CALL spexpackageimport();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


