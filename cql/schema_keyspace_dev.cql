-- development environment
CREATE KEYSPACE IF NOT EXISTS writing
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE writing;