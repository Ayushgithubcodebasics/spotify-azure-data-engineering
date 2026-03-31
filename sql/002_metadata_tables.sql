IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'metadata')
    EXEC('CREATE SCHEMA metadata');
GO

IF OBJECT_ID('metadata.table_configs', 'U') IS NULL
BEGIN
    CREATE TABLE metadata.table_configs (
        schema_name SYSNAME NOT NULL,
        table_name SYSNAME NOT NULL PRIMARY KEY,
        cdc_column SYSNAME NOT NULL,
        is_active BIT NOT NULL DEFAULT 1
    );
END;
GO

IF OBJECT_ID('metadata.pipeline_watermarks', 'U') IS NULL
BEGIN
    CREATE TABLE metadata.pipeline_watermarks (
        table_name SYSNAME NOT NULL PRIMARY KEY,
        last_load_timestamp DATETIME2(0) NULL,
        last_row_count BIGINT NULL,
        load_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
        last_updated_at DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

MERGE metadata.table_configs AS target
USING (
    SELECT 'dbo' AS schema_name, 'DimUser' AS table_name, 'updated_at' AS cdc_column UNION ALL
    SELECT 'dbo', 'DimArtist', 'updated_at' UNION ALL
    SELECT 'dbo', 'DimTrack', 'updated_at' UNION ALL
    SELECT 'dbo', 'DimDate', 'date' UNION ALL
    SELECT 'dbo', 'FactStream', 'stream_timestamp'
) AS source
ON target.table_name = source.table_name
WHEN MATCHED THEN
    UPDATE SET
        target.schema_name = source.schema_name,
        target.cdc_column = source.cdc_column,
        target.is_active = 1
WHEN NOT MATCHED THEN
    INSERT (schema_name, table_name, cdc_column, is_active)
    VALUES (source.schema_name, source.table_name, source.cdc_column, 1);
GO

MERGE metadata.pipeline_watermarks AS target
USING (
    SELECT 'DimUser' AS table_name UNION ALL
    SELECT 'DimArtist' UNION ALL
    SELECT 'DimTrack' UNION ALL
    SELECT 'DimDate' UNION ALL
    SELECT 'FactStream'
) AS source
ON target.table_name = source.table_name
WHEN NOT MATCHED THEN
    INSERT (table_name, last_load_timestamp, last_row_count, load_status)
    VALUES (source.table_name, CAST('1900-01-01' AS DATETIME2(0)), 0, 'PENDING');
GO

