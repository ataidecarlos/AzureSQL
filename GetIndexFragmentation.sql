DROP PROCEDURE IF EXISTS [dbo].GetIndexFragmentation
GO

CREATE PROCEDURE [dbo].GetIndexFragmentation

AS
BEGIN


-- Temp tables
DROP TABLE IF EXISTS #Objects
DROP TABLE IF EXISTS #index_physical_stats

SELECT * INTO #index_physical_stats FROM index_physical_stats WHERE 1=2
CREATE TABLE #Objects (object_name VARCHAR(255), object_id BIGINT, index_name VARCHAR(255), index_id INT, type TINYINT, type_desc VARCHAR(255))

-- Get a list of tables to be analyzed (U = USER_TABLE)

INSERT INTO #Objects (object_name, object_id, index_name, index_id, type, type_desc)
SELECT
    OBJECT_NAME(object_id) AS 'object_name',
    object_id,
    name AS 'index_name',
    index_id,
    type_desc
FROM sys.indexes
WHERE object_id IN (SELECT object_id FROM sys.objects WHERE type = 'U')
--WHERE object_id IN (1701581100,1541580530)



DECLARE @SQL NVARCHAR(1000),
        @SQLDefinition NVARCHAR(500),
        @object_id BIGINT,
        @index_id INT,
        @partition_number INT,
        @start_time DATETIME,
        @end_time DATETIME,
        @mode NVARCHAR(10)

SET @SQLDefinition = N'@object_id BIGINT,@index_id INT,@partition_number INT,@mode NVARCHAR(10)'

-- TODO | Get all of these from user inputs
SET @mode = 'DETAILED'


-- Loop on all combination of table/index/partition
DECLARE Looper CURSOR FOR 
    SELECT object_id, index_id, partition_number
    FROM sys.dm_db_partition_stats
    WHERE row_count > 0
        AND object_id IN (SELECT object_id FROM #Objects)

OPEN Looper
FETCH NEXT FROM Looper INTO @object_id, @index_id, @partition_number

WHILE (@@FETCH_STATUS<>-1)
BEGIN

    SET @start_time = GETDATE()

    SET @SQL = N'INSERT INTO #index_physical_stats (object_name,object_id,index_name,index_id,type_desc,partition_number,page_count,avg_fragmentation_in_percent,avg_page_space_used_in_percent,start_time,end_time,capture_mode) '
    SET @SQL = @SQL + N' SELECT OBJ.object_name, ST.object_id, OBJ.index_name, ST.index_id, ST.index_type_desc, ST.partition_number, ST.page_count, ST.avg_fragmentation_in_percent, ST.avg_page_space_used_in_percent, NULL, NULL, @mode '
    SET @SQL = @SQL + N' FROM sys.dm_db_indeindex_physical_stats (DB_ID(), @object_id, @index_id, @partition_number, @mode) '
    SET @SQL = @SQL + N' ST JOIN #Objects OBJ ON ST.object_id = OBJ.object_id AND ST.index_id = OBJ.index_id '

    EXECUTE sp_executesql @SQL, @SQLDefinition, @object_id, @index_id, @partition_number, @mode

    SET @end_time = GETDATE()

    -- Doing this in two steps (temp > final table, so that timings are captured correctly )
    INSERT INTO index_physical_stats (object_name,object_id,index_name,index_id,type_desc,partition_number,page_count,avg_fragmentation_in_percent,avg_page_space_used_in_percent,start_time,end_time,capture_mode)
    SELECT object_name,object_id,index_name,index_id,type_desc,partition_number,page_count,avg_fragmentation_in_percent,avg_page_space_used_in_percent,@start_time,@end_time,capture_mode
    FROM #index_physical_stats

    TRUNCATE TABLE #index_physical_stats

    FETCH NEXT FROM Looper INTO @object_id, @index_id, @partition_number
END

CLOSE Looper
DEALLOCATE Looper

DROP TABLE IF EXISTS #Objects
DROP TABLE IF EXISTS #index_physical_stats

END
GO