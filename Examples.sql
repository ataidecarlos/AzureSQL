-- To find object_ids
SELECT name, object_id FROM sys.objects WHERE [type] = 'U'




-- Get the fragmentation for all objects in the database
EXEC extras.GetIndexFragmentation @mode = 'LIMITED', @user_defined = 0, @cleanup = 1


------------------------------------------------------------------------------------------------


CREATE TABLE #user_objects (object_id BIGINT, index_id INT, partition_number INT)

-- Get fragmentation for all indexes in this object_id
INSERT INTO #user_objects (object_id) VALUES (1717581157)
INSERT INTO #user_objects (object_id) VALUES (1701581100)

-- Get fragmentation for all partitions in this object_id/index_id
INSERT INTO #user_objects (object_id, index_id) VALUES (1749581271,1)

-- Get fragmentation for specified partition in this object_id/index_id
INSERT INTO #user_objects (object_id, index_id) VALUES (1749581271,1,1)

-- Get the fragmentation
EXEC extras.GetIndexFragmentation @mode = 'LIMITED', @user_defined = 1, @cleanup = 1


-- Cleanup
DROP TABLE #user_objects