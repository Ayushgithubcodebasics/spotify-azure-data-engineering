IF OBJECT_ID('metadata.usp_update_pipeline_watermark', 'P') IS NOT NULL
    DROP PROCEDURE metadata.usp_update_pipeline_watermark;
GO

CREATE PROCEDURE metadata.usp_update_pipeline_watermark
    @table_name SYSNAME,
    @last_load_timestamp DATETIME2(0) = NULL,
    @last_row_count BIGINT = NULL,
    @load_status VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    MERGE metadata.pipeline_watermarks AS target
    USING (SELECT @table_name AS table_name) AS source
    ON target.table_name = source.table_name
    WHEN MATCHED THEN
        UPDATE SET
            target.last_load_timestamp =
                CASE
                    WHEN @load_status = 'SUCCESS' AND @last_load_timestamp IS NOT NULL
                        THEN @last_load_timestamp
                    ELSE target.last_load_timestamp
                END,
            target.last_row_count =
                CASE
                    WHEN @load_status = 'SUCCESS' AND @last_row_count IS NOT NULL
                        THEN @last_row_count
                    ELSE target.last_row_count
                END,
            target.load_status = @load_status,
            target.last_updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (table_name, last_load_timestamp, last_row_count, load_status, last_updated_at)
        VALUES (
            @table_name,
            CASE WHEN @load_status = 'SUCCESS' THEN @last_load_timestamp ELSE NULL END,
            CASE WHEN @load_status = 'SUCCESS' THEN @last_row_count ELSE NULL END,
            @load_status,
            SYSUTCDATETIME()
        );
END;
GO

