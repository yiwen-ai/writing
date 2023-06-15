-- production environment
CREATE KEYSPACE IF NOT EXISTS writing
WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '3' };

-- development environment
CREATE KEYSPACE IF NOT EXISTS writing
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };

CREATE TABLE IF NOT EXISTS writing.creation (
    id               BLOB,        -- creation id, 12 bytes XID, https://docs.rs/xid/latest/xid/
    gid              BLOB,        -- group id, creation belong to
    status           TINYINT,     -- int8, -2: Deleted, -1: Archived, 0: Draft, 1: Review, 2: Approved
    rating           TINYINT,     -- int8, 0: General Audience, 1: Parental Guidance, 2: Parents Strongly Cautioned, 3: Restricted; 4: Adults Only; 127: Banned.
    version          SMALLINT,    -- creation version
    language         ASCII,       -- creation's language, ISO 639-3
    creator          BLOB,        -- user id who create the creation.
    created_at       BIGINT,      -- create at
    updated_at       BIGINT,      -- update at
    active_languages SET<ASCII>,  -- active languages by translation, ISO 639-3
    original_url     TEXT,        -- original web url (https://xxxxxxx)
    genre            LIST<TEXT>, -- genre
    title            TEXT,        -- title
    description      TEXT,        -- description
    cover            TEXT,        -- cover url
    keywords         LIST<TEXT>,  -- keywords
    labels           LIST<TEXT>,  -- labels for creation management
    authors          LIST<TEXT>,  -- authors
    reviewers        LIST<BLOB>,  -- reviewers
    summary          TEXT,        -- summary
    content          BLOB,        -- content in CBOR format
    license          TEXT,        -- license url
    PRIMARY KEY (id)
) WITH caching = {'enabled': 'true'}
    AND comment = 'creations'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX creations_by_group ON writing.creation (gid);           -- global secondary indexe
CREATE INDEX creations_by_id_status ON writing.creation ((id), status); -- local secondary index

CREATE TABLE IF NOT EXISTS writing.publication (
    id           BLOB,       -- creation id
    language     ASCII,      -- publication's language, ISO 639-3
    version      SMALLINT,   -- creation version
    status       TINYINT,    -- int8, -2: Deleted, -1: Archived, 0: Normal, 1: Approved, 2: Published
    creator      BLOB,       -- user id who create the publication.
    created_at   BIGINT,     -- create at
    published_at BIGINT,     -- published at
    model        ASCII,      -- AI model name, default to empty
    title        TEXT,       -- title
    description  TEXT,       -- description
    keywords     LIST<TEXT>, -- keywords
    summary      TEXT,       -- summary
    content      BLOB,       -- content in CBOR format.
    PRIMARY KEY (id, language, version)
) WITH CLUSTERING ORDER BY (language ASC, version DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'publications'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE TABLE IF NOT EXISTS writing.publication_draft (
    gid          BLOB,       -- group id, publication draft belong to
    id           BLOB,       -- draft id
    pid          BLOB,       -- publication id
    language     ASCII,      -- draft's language, ISO 639-3
    version      SMALLINT,   -- publication version
    status       TINYINT,    -- int8, -2: Deleted, -1: Rejected, 0: Draft, 1: Review, 2: Accepted
    creator      BLOB,       -- user id who create the publication.
    created_at   BIGINT,     -- create at
    model        ASCII,      -- AI model name, default to empty
    title        TEXT,       -- title
    description  TEXT,       -- description
    keywords     LIST<TEXT>, -- keywords
    summary      TEXT,       -- summary
    content      BLOB,       -- content in CBOR format.
    PRIMARY KEY (gid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'publications'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 31536000; -- 3600 * 24 * 365 seconds, 1 year

CREATE TABLE IF NOT EXISTS writing.comment (
    pid         BLOB,    -- publication id comment to, 12 bytes XID
    id          BLOB,    -- comment id, 12 bytes XID
    language    ASCII,   -- comment's language
    status      TINYINT, -- int8, -2: Deleted, -1: Rejected, 0: Review, 1: Approved
    creator     BLOB,    -- comment's creator id, 12 bytes XID
    created_at  BIGINT,  -- create at, unix time with second precision.
    updated_at  BIGINT,  -- update at, can't update after approved.
    approved_at BIGINT,  -- approved at.
    content     BLOB,    -- comment content in CBOR format.
    reply       BLOB,    -- optional, comment id reply to
    PRIMARY KEY (pid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'publication''s comments'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE TABLE IF NOT EXISTS writing.collection (
    uid         BLOB,        -- user id who create the collection, 12 bytes XID
    id          BLOB,        -- collection id, 12 bytes XID
    pid         BLOB,        -- publication id, 12 bytes XID
    version     SMALLINT,    -- publication version
    status      TINYINT,     -- int8, -2: Deleted, -1: Archived, 0: Normal, 1: Preferred
    updated_at  BIGINT,      -- update at, unix time with second precision.
    genre       LIST<ASCII>, -- publication (creation) genre
    title       TEXT,        -- publication title
    description TEXT,        -- publication description
    summary     TEXT,        -- publication summary
    labels      LIST<TEXT>,  -- labels for collections management
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND  caching = {'enabled': 'false'}
    AND comment = 'user''s collections'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;
