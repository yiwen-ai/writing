CREATE TABLE IF NOT EXISTS creation_index (
    id               BLOB,        -- creation id, 12 bytes XID, https://docs.rs/xid/latest/xid/
    gid              BLOB,        -- group id, creation belong to
    rating           TINYINT,     -- int8, 0: General Audience, 1: Parental Guidance, 2: Parents Strongly Cautioned, 3: Restricted; 4: Adults Only; 127: Banned.
    PRIMARY KEY (id)
) WITH caching = {'enabled': 'true'}
    AND comment = 'creations index'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

-- this table should be replaced by ytsaurus
CREATE TABLE IF NOT EXISTS content (
    id         BLOB,     -- content id, 12 bytes XID, https://docs.rs/xid/latest/xid/
    gid        BLOB,     -- group id, creation belong to
    cid        BLOB,     -- creation id, 12 bytes XID
    status     TINYINT,  -- int8, -1: Deleted, 0: Normal
    version    SMALLINT, -- creation version
    language   TEXT,     -- creation's language, ISO 639-3
    updated_at BIGINT,   -- update at
    length     INT,      -- content size in bytes
    hash       BLOB,     -- SHA3 256
    content    BLOB,     -- content in CBOR format
    PRIMARY KEY (id)
) WITH caching = {'enabled': 'true'}
    AND comment = 'creations'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE TABLE IF NOT EXISTS creation (
    gid              BLOB,        -- group id, creation belong to
    id               BLOB,        -- creation id, 12 bytes XID
    status           TINYINT,     -- int8, -1: Archived, 0: Draft, 1: Review, 2: Approved
    version          SMALLINT,    -- creation version
    language         TEXT,        -- creation's language, ISO 639-3
    creator          BLOB,        -- user id who create the creation.
    created_at       BIGINT,      -- create at, unix time, ms
    updated_at       BIGINT,      -- update at
    original_url     TEXT,        -- original web url (https://xxxxxxx)
    genre            LIST<TEXT>,  -- genre
    title            TEXT,        -- title
    cover            TEXT,        -- cover url
    keywords         LIST<TEXT>,  -- keywords
    labels           LIST<TEXT>,  -- labels for creation management
    authors          LIST<TEXT>,  -- authors
    reviewers        LIST<BLOB>,  -- reviewers
    summary          TEXT,        -- summary
    content          BLOB,        -- content id, xid
    license          TEXT,        -- license url
    PRIMARY KEY (gid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'creations'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX creation_gid_url ON creation ((gid), original_url);
CREATE INDEX creation_gid_status ON creation ((gid), status);

CREATE TABLE IF NOT EXISTS publication (
    gid           BLOB,       -- group id, publication belong to
    cid           BLOB,       -- creation id
    language      TEXT,       -- publication's language, ISO 639-3
    version       SMALLINT,   -- creation version
    status        TINYINT,    -- int8, -1: Rejected, 0: Review, 1: Approved, 2: Published
    creator       BLOB,       -- user id who create the publication.
    created_at    BIGINT,     -- create at
    updated_at    BIGINT,     -- update at
    model         TEXT,       -- AI model name, default to empty
    original_url  TEXT,       -- original web url (https://xxxxxxx)
    from_language TEXT,       -- translate from
    genre         LIST<TEXT>, -- genre
    title         TEXT,       -- title
    cover         TEXT,       -- cover url
    keywords      LIST<TEXT>, -- keywords
    authors       LIST<TEXT>, -- authors
    summary       TEXT,       -- summary
    content       BLOB,       -- content id, xid
    license       TEXT,       -- license url
    PRIMARY KEY (gid, cid, language, version)
) WITH CLUSTERING ORDER BY (cid DESC, language ASC, version DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'publications'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX publication_cid ON publication (cid);
CREATE INDEX publication_url ON publication (original_url);
CREATE INDEX publication_gid_status ON publication ((gid), status);

CREATE TABLE IF NOT EXISTS publication_index (
    cid        BLOB,     -- creation id
    language   TEXT,     -- publication's language, ISO 639-3
    original   BOOLEAN,  -- is original
    version    SMALLINT, -- creation version
    gid        BLOB,     -- group id, publication belong to
    PRIMARY KEY (cid, language)
) WITH CLUSTERING ORDER BY (language ASC)
    AND caching = {'enabled': 'true'}
    AND comment = 'published publications index'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX publication_index_gid ON publication_index (gid);

CREATE TABLE IF NOT EXISTS bookmark (
    uid         BLOB,        -- user id who create the bookmark, 12 bytes XID
    id          BLOB,        -- bookmark id, 12 bytes XID
    kind        TINYINT,     -- int8, 0: Publication, 1: Collection
    cid         BLOB,        -- publication id or collection id, 12 bytes XID
    gid         BLOB,        -- group id, publication belong to
    language    TEXT,        -- language
    version     SMALLINT,    -- version
    updated_at  BIGINT,      -- update at, unix time with second precision.
    title       TEXT,        -- title
    labels      LIST<TEXT>,  -- labels for bookmarks management
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND  caching = {'enabled': 'false'}
    AND comment = 'user''s bookmarks'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX bookmark_uid_cid ON bookmark ((uid), cid);

CREATE TABLE IF NOT EXISTS deleted_creation (
    gid              BLOB,        -- group id, creation belong to
    id               BLOB,        -- creation id, 12 bytes XID
    status           TINYINT,     -- int8, -1: Archived, 0: Draft, 1: Review, 2: Approved
    version          SMALLINT,    -- creation version
    language         TEXT,        -- creation's language, ISO 639-3
    creator          BLOB,        -- user id who create the creation.
    created_at       BIGINT,      -- create at, unix time, ms
    updated_at       BIGINT,      -- update at
    original_url     TEXT,        -- original web url (https://xxxxxxx)
    genre            LIST<TEXT>,  -- genre
    title            TEXT,        -- title
    cover            TEXT,        -- cover url
    keywords         LIST<TEXT>,  -- keywords
    labels           LIST<TEXT>,  -- labels for creation management
    authors          LIST<TEXT>,  -- authors
    reviewers        LIST<BLOB>,  -- reviewers
    summary          TEXT,        -- summary
    content          BLOB,        -- content id, xid
    license          TEXT,        -- license url
    PRIMARY KEY (gid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'creations'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 10}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 17280000; -- 60*60*24*200, 17280000, 200 days，安全要求应该大于半年

CREATE TABLE IF NOT EXISTS deleted_publication (
    gid           BLOB,       -- group id, publication belong to
    cid           BLOB,       -- creation id
    language      TEXT,       -- publication's language, ISO 639-3
    version       SMALLINT,   -- creation version
    status        TINYINT,    -- int8, -1: Rejected, 0: Review, 1: Approved, 2: Published
    creator       BLOB,       -- user id who create the publication.
    created_at    BIGINT,     -- create at
    updated_at    BIGINT,     -- update at
    model         TEXT,       -- AI model name, default to empty
    original_url  TEXT,       -- original web url (https://xxxxxxx)
    from_language TEXT,       -- translate from
    genre         LIST<TEXT>, -- genre
    title         TEXT,       -- title
    cover         TEXT,       -- cover url
    keywords      LIST<TEXT>, -- keywords
    authors       LIST<TEXT>, -- authors
    summary       TEXT,       -- summary
    content       BLOB,       -- content id, xid
    license       TEXT,       -- license url
    PRIMARY KEY (gid, cid, language, version)
) WITH CLUSTERING ORDER BY (cid DESC, language ASC, version DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'publications'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 10}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 17280000;
