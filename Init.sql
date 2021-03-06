SET QUOTED_IDENTIFIER OFF;
GO

IF SCHEMA_ID('extras') IS NULL
	EXEC ('CREATE SCHEMA extras');
GO


DROP TABLE IF EXISTS extras.index_physical_stats
GO

CREATE TABLE extras.index_physical_stats (
    object_name VARCHAR(255),
    object_id BIGINT,
    index_name VARCHAR(255),
    index_id INT,
    type_desc VARCHAR(255),
    partition_number INT,
    page_count INT,
    avg_fragmentation_in_percent DECIMAL(10, 5),
    avg_page_space_used_in_percent DECIMAL(10, 5),
    capture_mode VARCHAR(10),
    capture_time DATETIME
    )
GO