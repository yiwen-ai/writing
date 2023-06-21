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

CREATE TABLE IF NOT EXISTS creation (
    gid              BLOB,        -- group id, creation belong to
    id               BLOB,        -- creation id, 12 bytes XID, https://docs.rs/xid/latest/xid/
    status           TINYINT,     -- int8, -1: Archived, 0: Draft, 1: Review, 2: Approved
    rating           TINYINT,     -- int8, 0: General Audience, 1: Parental Guidance, 2: Parents Strongly Cautioned, 3: Restricted; 4: Adults Only; 127: Banned.
    version          SMALLINT,    -- creation version
    language         TEXT,        -- creation's language, ISO 639-3
    creator          BLOB,        -- user id who create the creation.
    created_at       BIGINT,      -- create at, unix time, ms
    updated_at       BIGINT,      -- update at
    active_languages SET<TEXT>,   -- active languages by translation, ISO 639-3
    original_url     TEXT,        -- original web url (https://xxxxxxx)
    genre            LIST<TEXT>,  -- genre
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
    PRIMARY KEY (gid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'creations'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX creation_gid_status ON creation ((gid), status);

CREATE TABLE IF NOT EXISTS publication (
    id           BLOB,       -- creation id
    language     TEXT,       -- publication's language, ISO 639-3
    version      SMALLINT,   -- creation version
    status       TINYINT,    -- int8, -1: Archived, 0: Normal, 1: Approved, 2: Published
    creator      BLOB,       -- user id who create the publication.
    created_at   BIGINT,     -- create at
    updated_at   BIGINT,     -- update at
    model        TEXT,       -- AI model name, default to empty
    original_url TEXT,       -- original web url (https://xxxxxxx)
    genre        LIST<TEXT>, -- genre
    title        TEXT,       -- title
    description  TEXT,       -- description
    cover        TEXT,       -- cover url
    keywords     LIST<TEXT>, -- keywords
    authors      LIST<TEXT>, -- authors
    summary      TEXT,       -- summary
    content      BLOB,       -- content in CBOR format.
    license      TEXT,       -- license url
    PRIMARY KEY (id, language, version)
) WITH CLUSTERING ORDER BY (language ASC, version DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'publications'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE TABLE IF NOT EXISTS publication_draft (
    gid          BLOB,       -- group id, publication draft belong to
    id           BLOB,       -- draft id
    cid          BLOB,       -- creation id
    language     TEXT,       -- draft's language, ISO 639-3
    version      SMALLINT,   -- creation version
    status       TINYINT,    -- int8, -1: Rejected, 0: Draft, 1: Review, 2: Accepted
    creator      BLOB,       -- user id who create the publication.
    created_at   BIGINT,     -- create at
    updated_at   BIGINT,     -- update at
    model        TEXT,       -- AI model name, default to empty
    original_url TEXT,       -- original web url (https://xxxxxxx)
    genre        LIST<TEXT>, -- genre
    title        TEXT,       -- title
    description  TEXT,       -- description
    cover        TEXT,       -- cover url
    keywords     LIST<TEXT>, -- keywords
    authors      LIST<TEXT>, -- authors
    summary      TEXT,       -- summary
    content      BLOB,       -- content in CBOR format.
    license      TEXT,       -- license url
    PRIMARY KEY (gid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'publications'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX publication_draft_gid_status ON publication_draft ((gid), status);

CREATE TABLE IF NOT EXISTS comment (
    cid         BLOB,    -- creation id comment to, 12 bytes XID
    id          BLOB,    -- comment id, 12 bytes XID
    language    TEXT,    -- comment's language
    status      TINYINT, -- int8, -1: Rejected, 0: Review, 1: Approved
    creator     BLOB,    -- comment's creator id, 12 bytes XID
    created_at  BIGINT,  -- create at, unix time with second precision.
    updated_at  BIGINT,  -- update at, can't update after approved.
    approved_at BIGINT,  -- approved at.
    content     BLOB,    -- comment content in CBOR format.
    reply       BLOB,    -- optional, comment id reply to
    PRIMARY KEY (cid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'publication''s comments'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX comment_cid_status ON comment ((cid), status);

CREATE TABLE IF NOT EXISTS collection (
    uid         BLOB,        -- user id who create the collection, 12 bytes XID
    id          BLOB,        -- collection id, 12 bytes XID
    cid         BLOB,        -- creation id, 12 bytes XID
    language    TEXT,        -- creation language
    version     SMALLINT,    -- creation version
    status      TINYINT,     -- int8, -1: Archived, 0: Normal, 1: Preferred, 2: Pinned
    updated_at  BIGINT,      -- update at, unix time with second precision.
    genre       LIST<TEXT>,  -- publication (creation) genre
    title       TEXT,        -- publication title
    description TEXT,        -- publication description
    cover       TEXT,        -- cover url
    summary     TEXT,        -- publication summary
    labels      LIST<TEXT>,  -- labels for collections management
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND  caching = {'enabled': 'false'}
    AND comment = 'user''s collections'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX collection_uid_status ON collection ((uid), status);

CREATE TABLE IF NOT EXISTS deleted_creation (
    gid              BLOB,        -- group id, creation belong to
    id               BLOB,        -- creation id, 12 bytes XID, https://docs.rs/xid/latest/xid/
    status           TINYINT,     -- int8, -1: Archived, 0: Draft, 1: Review, 2: Approved
    rating           TINYINT,     -- int8, 0: General Audience, 1: Parental Guidance, 2: Parents Strongly Cautioned, 3: Restricted; 4: Adults Only; 127: Banned.
    version          SMALLINT,    -- creation version
    language         TEXT,        -- creation's language, ISO 639-3
    creator          BLOB,        -- user id who create the creation.
    created_at       BIGINT,      -- create at
    updated_at       BIGINT,      -- update at
    active_languages SET<TEXT>,   -- active languages by translation, ISO 639-3
    original_url     TEXT,        -- original web url (https://xxxxxxx)
    genre            LIST<TEXT>,  -- genre
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
    PRIMARY KEY (gid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'creations'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 10}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 17280000; -- 60*60*24*200, 17280000, 200 days，安全要求应该大于半年

CREATE TABLE IF NOT EXISTS deleted_publication (
    id           BLOB,       -- creation id
    language     TEXT,       -- publication's language, ISO 639-3
    version      SMALLINT,   -- creation version
    status       TINYINT,    -- int8, -1: Archived, 0: Normal, 1: Approved, 2: Published
    creator      BLOB,       -- user id who create the publication.
    created_at   BIGINT,     -- create at
    updated_at   BIGINT,     -- update at
    model        TEXT,       -- AI model name, default to empty
    original_url TEXT,       -- original web url (https://xxxxxxx)
    genre        LIST<TEXT>, -- genre
    title        TEXT,       -- title
    description  TEXT,       -- description
    cover        TEXT,       -- cover url
    keywords     LIST<TEXT>, -- keywords
    authors      LIST<TEXT>, -- authors
    summary      TEXT,       -- summary
    content      BLOB,       -- content in CBOR format.
    license      TEXT,       -- license url
    PRIMARY KEY (id, language, version)
) WITH CLUSTERING ORDER BY (language ASC, version DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'publications'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 10}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 17280000;

CREATE TABLE IF NOT EXISTS deleted_publication_draft (
    gid          BLOB,       -- group id, publication draft belong to
    id           BLOB,       -- draft id
    cid          BLOB,       -- creation id
    language     TEXT,       -- draft's language, ISO 639-3
    version      SMALLINT,   -- creation version
    status       TINYINT,    -- int8, -1: Rejected, 0: Draft, 1: Review, 2: Accepted
    creator      BLOB,       -- user id who create the publication.
    created_at   BIGINT,     -- create at
    updated_at   BIGINT,     -- update at
    model        TEXT,       -- AI model name, default to empty
    original_url TEXT,       -- original web url (https://xxxxxxx)
    genre        LIST<TEXT>, -- genre
    title        TEXT,       -- title
    description  TEXT,       -- description
    cover        TEXT,       -- cover url
    keywords     LIST<TEXT>, -- keywords
    authors      LIST<TEXT>, -- authors
    summary      TEXT,       -- summary
    content      BLOB,       -- content in CBOR format.
    license      TEXT,       -- license url
    PRIMARY KEY (gid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'publications'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 10}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 17280000;

CREATE TABLE IF NOT EXISTS deleted_comment (
    cid         BLOB,    -- creation id comment to, 12 bytes XID
    id          BLOB,    -- comment id, 12 bytes XID
    language    TEXT,    -- comment's language
    status      TINYINT, -- int8, -1: Rejected, 0: Review, 1: Approved
    creator     BLOB,    -- comment's creator id, 12 bytes XID
    created_at  BIGINT,  -- create at, unix time with second precision.
    updated_at  BIGINT,  -- update at, can't update after approved.
    approved_at BIGINT,  -- approved at.
    content     BLOB,    -- comment content in CBOR format.
    reply       BLOB,    -- optional, comment id reply to
    PRIMARY KEY (cid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'false'}
    AND comment = 'publication''s comments'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 10}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 17280000;

CREATE TABLE IF NOT EXISTS deleted_collection (
    uid         BLOB,        -- user id who create the collection, 12 bytes XID
    id          BLOB,        -- collection id, 12 bytes XID
    cid         BLOB,        -- creation id, 12 bytes XID
    language    TEXT,        -- creation language
    version     SMALLINT,    -- creation version
    status      TINYINT,     -- int8, -1: Archived, 0: Normal, 1: Preferred, 2: Pinned
    updated_at  BIGINT,      -- update at, unix time with second precision.
    genre       LIST<TEXT>,  -- publication (creation) genre
    title       TEXT,        -- publication title
    description TEXT,        -- publication description
    cover       TEXT,        -- cover url
    summary     TEXT,        -- publication summary
    labels      LIST<TEXT>,  -- labels for collections management
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND  caching = {'enabled': 'false'}
    AND comment = 'user''s collections'
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 10}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 17280000;