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
This script loads data dictionary from csv files and runs integrity checks.  Some
additional checks that could be added are as follows:

- Add check in to see if the multiple source columns are going to the same destination column
- Add checks to make sure that decimal, varchar, nvarchar, etc all have lengths
- Check if foreign and destination have compatible data types.
- check that 'Varchar', 'Varchar2', 'nVarchar', 'Char', 'nCHar','varbinary', 'decimal' all have data lengths and the decimals have decimals
- check that source and dest tables are not the same
- check if id column exists 
- check if source and destination ids match

*/
DO
$$
    DECLARE
        l_id   INT;
        l_name VARCHAR;
    BEGIN

        PERFORM SET_CONFIG('search_path', '_Staging,' || CURRENT_SETTING('search_path'), TRUE);

        DROP TABLE IF EXISTS aadictionarycolumn;

        CREATE TABLE aadictionarycolumn
        (
            ID                          NUMERIC(10),
            SystemName                  VARCHAR,
            CombinedName                VARCHAR,
            DictionaryTableID           NUMERIC(10),
            TableName                   VARCHAR,
            Name                        VARCHAR,
            Description                 VARCHAR,
            TableNormalizedName         VARCHAR,
            NormalizedName              VARCHAR,
            LongName                    VARCHAR,
            Label                       VARCHAR,
            Source                      VARCHAR,
            DataType                    VARCHAR,
            DataLength                  NUMERIC(10),
            Decimals                    NUMERIC(10),
            OldPicture                  VARCHAR,
            IsNullable                  VARCHAR,
            ValidationRule              VARCHAR,
            DefaultValue                VARCHAR,
            ColumnSequence              VARCHAR,
            Purpose                     VARCHAR,
            DictionaryTableIDForeign    NUMERIC(10),
            ForeignTable                VARCHAR,
            ForeignKeySuffix            VARCHAR,
            isIncludedInUniqueConstraint VARCHAR,
            ReferenceTable              VARCHAR,
            IsUsed                      VARCHAR,
            IsChangeHistoryUsed         VARCHAR,
            IsIndexed                   VARCHAR,
            DictionaryColumnIDNew       NUMERIC(10),
            DictionaryColumnNew         VARCHAR,
            ConversionComments          VARCHAR,
            IsEncrypted                 VARCHAR,
            IsHeaderColumn              VARCHAR
        );

        COPY aadictionarycolumn FROM 'c:\temp\bmsdata\aadictionarycolumn.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        DROP TABLE IF EXISTS aadictionarytable;

        CREATE TABLE aadictionarytable
        (
            ID                   NUMERIC(10),
            SystemName           VARCHAR,
            Name                 VARCHAR,
            Description          VARCHAR,
            NormalizedName       VARCHAR,
            SingularName         VARCHAR,
            PluralName           VARCHAR,
            TableType            VARCHAR,
            isTableTemporal      VARCHAR,
            RecordCount          VARCHAR,
            PrimaryKey           VARCHAR,
            Translation          VARCHAR,
            RegCodeTranslation   VARCHAR,
            WhereClause          VARCHAR,
            AdditionalIndexKeys  VARCHAR,
            Category             VARCHAR,
            DictionaryTableIDNew VARCHAR,
            ChangeHistoryScope   VARCHAR,
            ChangeHistoryLevel   VARCHAR,
            SystemModule         VARCHAR,
            Source               VARCHAR,
            MergeKey             VARCHAR,
            ConversionSequence   VARCHAR
        );

        COPY aadictionarytable FROM 'c:\temp\bmsdata\aadictionarytable.csv' DELIMITER ',' CSV ENCODING 'windows-1251';

        IF TO_REGCLASS('tmpDataType') IS NULL
        THEN
            CREATE TEMP TABLE tmpDataType
            (
                DataType           VARCHAR,
                TypeClassification VARCHAR
            );
            INSERT INTO tmpDataType
            SELECT 'bigint' DataType,
                   'Int'    TypeClassification
            UNION ALL
            SELECT 'datetime' DataType,
                   'Date'     TypeClassification
            UNION ALL
            SELECT 'decimal' DataType,
                   'Decimal' TypeClassification
            UNION ALL
            SELECT 'float'   DataType,
                   'Decimal' TypeClassification
            UNION ALL
            SELECT 'money'   DataType,
                   'Decimal' TypeClassification
            UNION ALL
            SELECT 'nchar' DataType,
                   'Char'  TypeClassification
            UNION ALL
            SELECT 'ntext' DataType,
                   'Char'  TypeClassification
            UNION ALL
            SELECT 'numeric' DataType,
                   'Decimal' TypeClassification
            UNION ALL
            SELECT 'nvarchar' DataType,
                   'Char'     TypeClassification
            UNION ALL
            SELECT 'smalldatetime' DataType,
                   'Date'          TypeClassification
            UNION ALL
            SELECT 'smallint' DataType,
                   'Int'      TypeClassification
            UNION ALL
            SELECT 'text' DataType,
                   'Char' TypeClassification
            UNION ALL
            SELECT 'time' DataType,
                   'Time' TypeClassification
            UNION ALL
            SELECT 'timestamp' DataType,
                   'Time'      TypeClassification
            UNION ALL
            SELECT 'tinyint' DataType,
                   'Int'     TypeClassification
            UNION ALL
            SELECT 'varbinary' DataType,
                   'Binary'    TypeClassification
            UNION ALL
            SELECT 'varchar' DataType,
                   'Char'    TypeClassification;
        END IF;

        SELECT MIN(DictionaryTableID)
        INTO l_id
        FROM aadictionarycolumn DF
        LEFT JOIN
            aadictionarytable dt
            ON dt.id = DF.DictionaryTableID
        WHERE df.id IS NULL;
        IF l_id IS NOT NULL
        THEN
            RAISE EXCEPTION 'Dictionary TableID % is not valid', l_id;
        END IF;

        SELECT MIN(id)
        INTO l_id
        FROM (
                 SELECT ID
                 FROM aadictionarycolumn
                 GROUP BY id
                 HAVING COUNT(*) > 1
        ) a;

        IF l_id IS NOT NULL
        THEN
            RAISE EXCEPTION 'Column ID % is duplicated', l_id;
        END IF;

        SELECT MIN(DictionaryTableID)
        INTO l_id
        FROM aadictionarycolumn DF
        LEFT JOIN
            aadictionarytable dt
            ON dt.id = DF.DictionaryTableIDForeign
        WHERE df.id IS NULL;

        IF l_id IS NOT NULL
        THEN
            RAISE EXCEPTION 'Foreign TableID % is invalid', l_id;
        ELSE
            SELECT MIN(dt.id)
            INTO l_id
            FROM aadictionarycolumn DF
            JOIN aadictionarytable dt
                 ON dt.id = DF.DictionaryTableIDForeign
            WHERE dt.Primarykey IS NULL;

            IF l_id IS NOT NULL
            THEN
                RAISE NOTICE 'Foreign Table % is referenced that has no primary key defined', l_id;
            END IF;
        END IF;

        SELECT MIN(id)
        INTO l_id
        FROM (
                 SELECT ID
                 FROM aadictionarytable
                 GROUP BY id
                 HAVING COUNT(*) > 1) a;

        IF l_id IS NOT NULL
        THEN
            RAISE EXCEPTION 'Dictionary Table ID % is duplicated', l_id;
        END IF;

        SELECT MIN(name)
        INTO l_name
        FROM (
                 SELECT SystemName || '.' || CombinedName AS Name
                 FROM aadictionarycolumn
                 GROUP BY SystemName || '.' || CombinedName
                 HAVING COUNT(*) > 1) a;

        IF l_name IS NOT NULL
        THEN
            RAISE EXCEPTION 'Column Name % is duplicated', l_name;
        END IF;

        SELECT MIN(name)
        INTO l_name
        FROM (
                 SELECT SystemName || '.' || Name AS Name
                 FROM aadictionarytable
                 GROUP BY SystemName || '.' || Name
                 HAVING COUNT(*) > 1) a;

        IF l_name IS NOT NULL
        THEN
            RAISE EXCEPTION 'Table Name % is duplicated', l_name;
        END IF;

        SELECT MIN(sc.SystemName || '.' || sc.CombinedName) AS Name
        INTO l_name
        FROM aadictionarycolumn sc
        JOIN      aadictionarycolumn dc
                  ON dc.id = sc.DictionaryColumnIDNew
        JOIN      aadictionarytable dt
                  ON dt.id = dc.DictionaryTableID
        JOIN      aadictionarytable st
                  ON st.id = sc.DictionaryTableID
        LEFT JOIN tmpDataType sdt
                  ON sdt.DataType = sc.DataType
        LEFT JOIN tmpDataType ddt
                  ON ddt.DataType = dc.DataType
        WHERE COALESCE(sdt.TypeClassification, sc.DataType) =
              COALESCE(ddt.TypeClassification, dc.DataType)
          AND CASE
                  WHEN sc.DataLength = -1 THEN 99999
                  ELSE COALESCE(sc.DataLength, 10)
                  END > CASE
                  WHEN dc.DataLength = -1 THEN 99999
                  ELSE COALESCE(dc.DataLength, 10)
                  END;

        IF l_name IS NOT NULL
        THEN
            RAISE NOTICE 'Destination data length is shorter than source for column %', l_name;
        END IF;

        SELECT MIN(sc.SystemName || '.' || sc.CombinedName) AS Name
        INTO l_name
        FROM aadictionarycolumn sc
        JOIN      aadictionarycolumn dc
                  ON dc.id = sc.DictionaryColumnIDNew
        JOIN      aadictionarytable dt
                  ON dt.id = dc.DictionaryTableID
        JOIN      aadictionarytable st
                  ON st.id = sc.DictionaryTableID
        LEFT JOIN tmpDataType sdt
                  ON sdt.DataType = sc.DataType
        LEFT JOIN tmpDataType ddt
                  ON ddt.DataType = dc.DataType
        WHERE COALESCE(sdt.TypeClassification, sc.DataType) !=
              COALESCE(ddt.TypeClassification, dc.DataType)
          AND sc.source NOT LIKE '%xx%'
          AND dc.purpose = 'data';

        IF l_name IS NOT NULL
        THEN
            RAISE NOTICE 'Destination data type is different than source for column %', l_name;
        END IF;

        SELECT MIN(name)
        INTO l_name
        FROM (
                 SELECT SystemName || '.' || TableName || '.' || label AS Name
                 FROM aadictionarycolumn a
                 GROUP BY SystemName || '.' || TableName || '.' || label
                 HAVING COUNT(*) > 1) a;

        IF l_name IS NOT NULL
        THEN
            RAISE EXCEPTION 'Label Name % is duplicated', l_name;
        END IF;

        SELECT MultilinkName
        INTO l_name
        FROM aadictionarycolumn a
        JOIN LATERAL (SELECT REPLACE(LOWER(a.combinedname), 'sysdictionarytableid', 'rowid') MultilinkName) b
             ON TRUE
        WHERE LOWER(a.purpose) = 'multilink'
        GROUP BY MultilinkName
        HAVING COUNT(*) != 2
        LIMIT 1;

        IF l_name IS NOT NULL
        THEN
            RAISE EXCEPTION 'This multilink column ( % ) does not have a matching sysDictionaryTableID column', l_name;
        END IF;

        SELECT MultilinkName
        INTO l_name
        FROM aadictionarycolumn a
        JOIN LATERAL (SELECT REPLACE(LOWER(a.combinedname), 'sysdictionarytableid', 'rowid') MultilinkName) b
             ON TRUE
        WHERE LOWER(a.purpose) = 'multilink'
          AND b.multilinkname NOT LIKE '%.rowid%'
        LIMIT 1;

        IF l_name IS NOT NULL
        THEN
            RAISE EXCEPTION 'Multilink column % needs to begin with rowid or sysdictionarytableid', l_name;
        END IF;

        SELECT MIN(name)
        INTO l_name
        FROM (
                 SELECT SystemName || '.' || TableName || '.' || label AS Name
                 FROM aadictionarycolumn a
                 GROUP BY SystemName || '.' || TableName || '.' || label
                 HAVING COUNT(*) > 1
        ) a;

        IF l_name IS NOT NULL
        THEN
            RAISE EXCEPTION 'Label Name % is duplicated.  This impacts CreateViews', l_name;
        END IF;

        SELECT a.Name
        INTO l_name
            --  select *
        FROM aadictionarytable a
        LEFT JOIN aadictionarycolumn b
                  ON b.dictionarytableid = a.id
        WHERE b.id IS NULL
        LIMIT 1;

        IF l_name IS NOT NULL
        THEN
            RAISE EXCEPTION 'Table % has no columns defined', l_name;
        END IF;

    END;
$$ LANGUAGE plpgsql;

/*

The following select can be used as follows:
- Identifies for a given table what tables it is going to end up in
    select SourceTableID, SourceName, DestTableID, DestName from
- Looks at a given attribute and sees where that attribute will end up.  These are more for common
attributes that will flow through to every destination table.

SELECT        DestTableName
FROM
(
    SELECT DISTINCT
         st.id SourceTableID
        , st.Name SourceTableName
        , sc.SourceID SourceColumnID
        , sc.SourceName SourceColumnName
        , sc.SourceNewColumnID SourceNewColumnID
        , sc.SourceDataType SourceDataType
        , sc.SourceConversionSQL SourceConversionSQL
        , dt.id DestTableID
        , dt.Name DestTableName
        , dc.DestID DestColumnID
        , dc.DestName DestColumnName
        , dc.DestDataType DestDataType
        , dc.DestDefaultValue DestDefaultValue
    FROM      aadictionarycolumn f3
    JOIN aadictionarycolumn f4 ON f4.id = f3.DictionaryColumnIDNew
    JOIN aadictionarytable dt ON dt.id = f4.DictionaryTableID
    JOIN aadictionarytable st ON st.id = f3.DictionaryTableID
    LEFT JOIN LATERAL
    (
       SELECT sc.id SourceID
                , sc.Name SourceName
                , sc.DictionaryColumnIDNew SourceNewColumnID
                , sc.DataType SourceDataType
                , sc.ConversionSQL SourceConversionSQL
       FROM         aadictionarycolumn sc
       WHERE        sc.NormalizedName LIKE '%crmcontactid%'
                 AND sc.DictionaryTableID = st.id
     LIMIT 1
    ) sc ON TRUE
    LEFT JOIN LATERAL
    (
       SELECT dc.id DestID
                , dc.Name DestName
                , dc.DataType DestDataType
                , dc.DefaultValue DestDefaultValue
       FROM         aadictionarycolumn dc
       WHERE        dc.Name LIKE '%crmcontactid%'
                 AND dc.DictionaryTableID = dt.id
     LIMIT 1
    ) dc ON TRUE
) a
GROUP BY desttablename
HAVING COUNT(*) > 1;


-- Check Source and Destination Data Types

SELECT
  st.id SourceTableID,
  st.name SourceName,
  sc.ID SourceColumnID,
  sc.name SourceName,
  sc.DataType SourceDataType,
  sc.DataLength SourceDataLength,
  sc.ConversionSQL SourceConversionSQL,
  dt.id DestTableID,
  dt.name DestName,
  dc.ID DestColumnID,
  dc.name DestName,
  dc.DataType DestDataType,
  dc.DataLength DestDataLength
FROM aadictionarycolumn sc
  JOIN aadictionarycolumn dc
    ON dc.id = sc.DictionaryColumnIDNew
  JOIN aadictionarytable dt
    ON dt.id = dc.DictionaryTableID
  JOIN aadictionarytable st
    ON st.id = sc.DictionaryTableID
  LEFT JOIN tmpDataType sdt
    ON sdt.DataType = sc.DataType
  LEFT JOIN tmpDataType ddt
    ON ddt.DataType = dc.DataType
where COALESCE(sdt.TypeClassification, sc.DataType) != COALESCE(ddt.TypeClassification, dc.DataType)
WHERE dt.name LIKE '%crmcontactsubtypeperson%'
--where COALESCE(sdt.TypeClassification, sc.DataType) = COALESCE(ddt.TypeClassification, dc.DataType) and case when sc.DataLength = -1 then 99999 else COALESCE(sc.DataLength,0) end > case when dc.DataLength = -1 then 99999 else COALESCE(dc.DataLength,0) end
ORDER BY dc.name;


-- This query identifies foreign keys that have a different type from the primary key of the table they point to

SELECT
  st.id SourceTableID,
  st.name SourceName,
  sc.ID SourceColumnID,
  sc.name SourceName,
  sc.DataType SourceDataType,
  sc.Purpose SourcePurpose,
  ft.id DictionaryTableIDForeign,
  ft.name ForeignName,
  fc.ID ForeignColumnID,
  fc.name ForeignName,
  fc.DataType ForeignDataType,
  'select distinct ' || sc.name || ' from ' || st.name || ' order by ' || sc.name SelectStatement
FROM aadictionarycolumn sc
  JOIN aadictionarytable ft
    ON ft.id = sc.DictionaryTableIDForeign
  JOIN aadictionarytable st
    ON st.id = sc.DictionaryTableID
  LEFT JOIN aadictionarycolumn fc
    ON fc.DictionaryTableID = ft.id
    AND fc.name = ft.PrimaryKey
WHERE sc.DataType != fc.DataType
ORDER BY st.name;

-- Setup new Column Order

SELECT f.id
    , ((ROW_NUMBER() OVER(
       ORDER BY tn.TableName
            , cts.Sequence
            , sn.DataSort
            , f.Name)) * 10) + 10000 AS ColumnOrder
FROM   aadictionarycolumn f
LEFT JOIN aadictionarytable t ON t.id = f.DictionaryTableID
LEFT JOIN
(
    SELECT 'Primary key' Purpose
        , 1 AS Sequence
    UNION ALL
    SELECT 'Temporal'
        , 2
    UNION ALL
    SELECT 'Foreign key'
        , 3
    UNION ALL
    SELECT 'Multilink'
        , 4
    UNION ALL
    SELECT 'Data'
        , 5
    UNION ALL
    SELECT 'Audit'
        , 6
    UNION ALL
    SELECT 'System'
        , 7
) cts ON cts.Purpose = f.purpose
JOIN LATERAL
(
    SELECT COALESCE(f.normalizedname, f.name) ColumnName, f.name) ColumnName
) cn ON TRUE
JOIN LATERAL
(
    SELECT CASE
             WHEN cn.ColumnName LIKE '%Name%'
                 OR cn.ColumnName LIKE '%Description%' THEN 3
             WHEN cn.ColumnName = 'TemporalStartDate' THEN 1
             WHEN cn.ColumnName = 'TemporalEndDate' THEN 2
         ELSE 4
         END DataSort
) sn ON TRUE
JOIN LATERAL
(
    SELECT CASE
             WHEN LEFT(tablename, 3) LIKE 'MED%' THEN 'zz' + f.tablename
         ELSE f.tablename
         END TableName
) tn ON TRUE;

-- Analyze references between tables.  If no references exists, then either foreign keys have not been evaluated correctly or the table contains no useful
-- data in the enterprise model

SELECT        a.id
          , a.Name
          , a.RecordCount
          , b.*
          , c.*
FROM          aadictionarytable a
JOIN LATERAL
(
       SELECT bb.name
       FROM    aadictionarycolumn AS aa
       JOIN aadictionarytable bb ON bb.id = aa.DictionaryTableIDForeign
       JOIN aadictionarytable cc ON cc.id = aa.DictionaryTableID
       WHERE  a.Name = cc.name
       ORDER BY bb.name
) b ON TRUE
--       JOIN LATERAL
--       (
--       	   SELECT cc.name
--       	   FROM    aadictionarycolumn AS aa
--       	   JOIN aadictionarytable bb ON bb.id = aa.DictionaryTableIDForeign
--       	   JOIN aadictionarytable cc ON cc.id = aa.DictionaryTableID
--       	   WHERE  a.Name = bb.name
--       	   ORDER BY bb.name
--       ) c ON TRUE
WHERE b.ParentTables IS NULL
     AND c.ChildTables IS NULL
ORDER BY a.name;


/*

-- List all destination columns that dont have a source

*/

SELECT dc.ID DestColumnID
    , dc.CombinedName DestName
    , dc.DataType DestDataType
    , dc.Purpose DestPurpose
FROM   aadictionarycolumn dc
WHERE  NOT EXISTS
(
    SELECT 1
    FROM         aadictionarycolumn sc
    WHERE        sc.DictionaryColumnIDNew = dc.id
    LIMIT 1
)
      AND dc.SystemName LIKE 'new%'
      AND dc.purpose NOT IN('Audit', 'System', 'Primary key', 'Temporal')
ORDER BY dc.CombinedName;

*/