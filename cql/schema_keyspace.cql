-- test environment
CREATE KEYSPACE IF NOT EXISTS writing_test
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE writing_test;

-- development environment
CREATE KEYSPACE IF NOT EXISTS writing
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE writing;

-- production environment
CREATE KEYSPACE IF NOT EXISTS writing
WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '3' };
USE writing;
