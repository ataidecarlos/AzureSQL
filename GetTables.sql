DROP PROCEDURE IF EXISTS extras.GetTables
GO

CREATE PROCEDURE extras.GetTables

AS
BEGIN


-- Temp tables
DROP TABLE IF EXISTS #Objects
CREATE TABLE #Objects (object_name VARCHAR(255), object_id BIGINT, index_name VARCHAR(255), index_id INT)

-- Get a list of tables to be analyzed (U = USER_TABLE)
INSERT INTO #Objects (object_name, object_id, index_name, index_id)
SELECT
    OBJECT_NAME(object_id) AS 'object_name',
    object_id,
    name AS 'index_name',
    index_id
FROM sys.indexes
WHERE object_id IN (SELECT object_id FROM sys.objects WHERE type = 'U')


-- Insert only if the table is not already on the list
INSERT INTO extras.index_physical_stats (object_name,object_id,index_name,index_id,capture_mode)
SELECT OBJ.object_name, OBJ.object_id, OBJ.index_name, OBJ.index_id,'INIT'
FROM #Objects OBJ
    LEFT JOIN extras.index_physical_stats IDX ON OBJ.object_id = IDX.object_id AND OBJ.index_id = IDX.index_id
WHERE IDX.object_id IS NULL OR IDX.index_id IS NULL


-- Cleanup
DROP TABLE IF EXISTS #Objects

END
GO