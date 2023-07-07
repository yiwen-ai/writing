-- production environment
CREATE KEYSPACE IF NOT EXISTS writing
WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '3' };
USE writing;
