-- test environment
CREATE KEYSPACE IF NOT EXISTS writing_test
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE writing_test;