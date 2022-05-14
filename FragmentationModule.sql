DROP PROCEDURE IF EXISTS [dbo].GetFragmentation
GO

CREATE PROCEDURE [dbo].GetFragmentation

AS
BEGIN

-- Get a list of tables to be analyzed (U = USER_TABLE)
CREATE TABLE #Objects (object_name VARCHAR(255), object_id BIGINT, index_name VARCHAR(255), index_id INT, type TINYINT, type_desc VARCHAR(255))
INSERT INTO #Objects (object_name, object_id, index_name, index_id, type, type_desc)
SELECT
    OBJECT_NAME(object_id) AS 'object_name',
    object_id,
    name AS 'index_name',
    index_id,
    type,
    type_desc
FROM sys.indexes
WHERE object_id IN (SELECT object_id FROM sys.objects WHERE type = 'U')

DECLARE @object_name VARCHAR(255),
        @object_id BIGINT

DECLARE Looper CURSOR FOR 
    SELECT OBJECT_NAME(object_id), object_id, partition_number, index_id, row_count, used_page_count
    FROM sys.dm_db_partition_stats
    WHERE row_count > 0
        AND object_id IN (SELECT object_id FROM #Objects)

OPEN Looper
FETCH NEXT FROM Looper INTO @object_name, @object_id

WHILE (@@FETCH_STATUS<>-1)
BEGIN
    SET @txtout = @txtout + '  ' + @runtime + @task_state + @wait_category + @wait_duration_ms + @request_elapsed_time + @blocked_tasks + @command + @query + CHAR(13) + CHAR(10)
    FETCH NEXT FROM Looper INTO @object_name, @object_id
END

CLOSE Looper
DEALLOCATE Looper



-- 0 = HEAP
-- 1 = CLUSTERED
-- 2 = NONCLUSTERED

END
GO


