/*
This template replaces the original fake INSERT-only "incremental" script.
ADF uses the same logic inside the Copy activity source query expression.

Parameters:
    @SchemaName      SYSNAME
    @TableName       SYSNAME
    @CdcColumn       SYSNAME
    @WatermarkDate   DATETIME2(0)
*/
DECLARE @SchemaName SYSNAME      = 'dbo';
DECLARE @TableName SYSNAME       = 'DimUser';
DECLARE @CdcColumn SYSNAME       = 'updated_at';
DECLARE @WatermarkDate DATETIME2(0) = '1900-01-01';

DECLARE @sql NVARCHAR(MAX) =
    N'SELECT * FROM ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) +
    N' WHERE ' + QUOTENAME(@CdcColumn) + N' > @watermark_date';

EXEC sp_executesql
    @sql,
    N'@watermark_date DATETIME2(0)',
    @watermark_date = @WatermarkDate;
GO

