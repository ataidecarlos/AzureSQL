DROP TABLE IF EXISTS #user_objects
CREATE TABLE #user_objects (object_id BIGINT, index_id INT, partition_number INT)
GO

DROP PROCEDURE IF EXISTS extras.GetIndexFragmentation
GO

CREATE PROCEDURE extras.GetIndexFragmentation (@mode SYSNAME = 'LIMITED', @user_defined BIT = 0, @cleanup BIT = 1)
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

-- Temp tables
DROP TABLE IF EXISTS #objects
CREATE TABLE #objects (object_name VARCHAR(255), object_id BIGINT, index_name VARCHAR(255), index_id INT, partition_number INT)


-- Clear the table before the new results?
IF @cleanup  = 1
BEGIN
    TRUNCATE TABLE extras.index_physical_stats
END


-- Analyse only a list of objects provided by the user
-- Analyse all objects -> DEFAULT BEHAVIOR
IF @user_defined = 1
BEGIN
    -- Loop on the provided list
    INSERT INTO #objects (object_name, object_id, index_name, index_id,partition_number)
    SELECT
        OBJECT_NAME(IDX.object_id) AS 'object_name',
        IDX.object_id,
        IDX.name AS 'index_name',
        IDX.index_id,
        DB.partition_number
    FROM #user_objects UOBJ
        JOIN sys.indexes IDX
            ON UOBJ.object_id = IDX.object_id
            AND (UOBJ.index_id = IDX.index_id OR UOBJ.index_id IS NULL)
        JOIN sys.dm_db_partition_stats DB
            ON UOBJ.object_id = DB.object_id
            AND (UOBJ.index_id = DB.index_id OR UOBJ.index_id IS NULL)
            AND (UOBJ.partition_number = DB.partition_number OR UOBJ.partition_number IS NULL)
    WHERE IDX.object_id IN (SELECT object_id FROM sys.objects WHERE type = 'U')
        AND DB.row_count > 0
    GROUP BY OBJECT_NAME(IDX.object_id), IDX.object_id, IDX.name, IDX.index_id, DB.partition_number

END
ELSE
BEGIN
    -- Loop on all combination of table/index/partition
    -- TODO | Add support for Views
    INSERT INTO #objects (object_name, object_id, index_name, index_id,partition_number)
    SELECT
        OBJECT_NAME(IDX.object_id) AS 'object_name',
        IDX.object_id,
        IDX.name AS 'index_name',
        IDX.index_id,
        DB.partition_number
    FROM sys.indexes IDX
        JOIN sys.dm_db_partition_stats DB
            ON IDX.object_id = DB.object_id
            AND IDX.index_id = DB.index_id
    WHERE IDX.object_id IN (SELECT object_id FROM sys.objects WHERE type = 'U')
        AND DB.row_count > 0

END


-- Loop on all combination of table/index/partition
DECLARE Looper CURSOR FOR 
    SELECT object_id, index_id, partition_number FROM #objects


OPEN Looper
FETCH NEXT FROM Looper INTO @object_id, @index_id, @partition_number

WHILE (@@FETCH_STATUS<>-1)
BEGIN

    SET @capture_time = GETDATE()

    SET @SQL = N'INSERT INTO extras.index_physical_stats (object_name,object_id,index_name,index_id,type_desc,partition_number,page_count,avg_fragmentation_in_percent,avg_page_space_used_in_percent,capture_time,capture_mode) '
    SET @SQL = @SQL + N' SELECT OBJ.object_name, ST.object_id, OBJ.index_name, ST.index_id, ST.index_type_desc, ST.partition_number, ST.page_count, ST.avg_fragmentation_in_percent, ST.avg_page_space_used_in_percent, @capture_time, @mode '
    SET @SQL = @SQL + N' FROM sys.dm_db_index_physical_stats(DB_ID(), @object_id, @index_id, @partition_number, @mode) ST '
    SET @SQL = @SQL + N' JOIN #objects OBJ ON ST.object_id = OBJ.object_id AND ST.index_id = OBJ.index_id AND ST.partition_number = OBJ.partition_number'

    EXECUTE sp_executesql @SQL, @SQLDefinition, @object_id, @index_id, @partition_number, @capture_time, @mode

    FETCH NEXT FROM Looper INTO @object_id, @index_id, @partition_number
END

CLOSE Looper
DEALLOCATE Looper


-- Tempdb cleanup
DROP TABLE IF EXISTS #objects

END
GO

-- Tempdb cleanup
DROP TABLE IF EXISTS #user_objects
GO