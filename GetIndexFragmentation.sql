DROP PROCEDURE IF EXISTS extras.GetIndexFragmentation
GO

CREATE PROCEDURE extras.GetIndexFragmentation (@mode SYSNAME = 'LIMITED')
AS
BEGIN

-- Variables
DECLARE @SQL NVARCHAR(1000),
        @SQLDefinition NVARCHAR(500),
        @object_id BIGINT,
        @index_id INT,
        @partition_number INT,
        @capture_time DATETIME

SET @SQLDefinition = N'@object_id BIGINT,@index_id INT,@partition_number INT,@capture_time DATETIME,@mode SYSNAME'

SET @mode = UPPER(@mode)
IF @mode NOT IN ('LIMITED','SAMPLED','DETAILED') AND @mode IS NOT NULL
    SET @mode = 'LIMITED'



-- Find/Add new tables or indexes
EXEC extras.GetTables




-- Loop on all combination of table/index/partition
DECLARE Looper CURSOR FOR 
    SELECT object_id, index_id, partition_number
    FROM sys.dm_db_partition_stats
    WHERE row_count > 0
        AND object_id IN (SELECT DISTINCT object_id FROM extras.index_physical_stats)

OPEN Looper
FETCH NEXT FROM Looper INTO @object_id, @index_id, @partition_number

WHILE (@@FETCH_STATUS<>-1)
BEGIN

    SET @capture_time = GETDATE()

    SET @SQL = N'INSERT INTO extras.index_physical_stats (object_name,object_id,index_name,index_id,type_desc,partition_number,page_count,avg_fragmentation_in_percent,avg_page_space_used_in_percent,capture_time,capture_mode) '
    SET @SQL = @SQL + N' SELECT OBJ.object_name, ST.object_id, OBJ.index_name, ST.index_id, ST.index_type_desc, ST.partition_number, ST.page_count, ST.avg_fragmentation_in_percent, ST.avg_page_space_used_in_percent, @capture_time, @mode '
    SET @SQL = @SQL + N' FROM sys.dm_db_index_physical_stats(DB_ID(), @object_id, @index_id, @partition_number, @mode) ST '
    SET @SQL = @SQL + N' JOIN extras.index_physical_stats OBJ ON ST.object_id = OBJ.object_id AND ST.index_id = OBJ.index_id AND OBJ.capture_mode IS NULL'

    EXECUTE sp_executesql @SQL, @SQLDefinition, @object_id, @index_id, @partition_number, @capture_time, @mode

    FETCH NEXT FROM Looper INTO @object_id, @index_id, @partition_number
END

CLOSE Looper
DEALLOCATE Looper

END
GO