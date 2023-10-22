CREATE TABLE IF NOT EXISTS creation_index (
    id      BLOB,    -- creation id, 12 bytes XID, https://docs.rs/xid/latest/xid/
    gid     BLOB,    -- group id, creation belong to
    rating  TINYINT, -- int8, 0: General Audience, 1: Parental Guidance, 2: Parents Strongly Cautioned, 3: Restricted; 4: Adults Only; 127: Banned.
    price   BIGINT,  -- -1: Free forever， 0: Free, > 0: Price in Yiwen Coin
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

CREATE TABLE IF NOT EXISTS pub_index (
    day        INT,      -- publication birthday
    cid        BLOB,     -- creation id
    language   TEXT,     -- publication's language, ISO 639-3
    original   BOOLEAN,  -- is original
    version    SMALLINT, -- creation version
    gid        BLOB,     -- group id, publication belong to
    PRIMARY KEY (day, cid, language)
) WITH CLUSTERING ORDER BY (cid DESC, language ASC)
    AND caching = {'enabled': 'true'}
    AND comment = 'published publications index'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX pub_index_day_gid ON pub_index ((day), gid);
CREATE INDEX pub_index_gid ON pub_index (gid);

CREATE TABLE IF NOT EXISTS collection (
    day            INT,      -- collection birthday
    id             BLOB,     -- collection id, 12 bytes XID
    gid            BLOB,     -- group id, collection belong to
    status         TINYINT,  -- int8, -1: Archived, 0: Private, 1: Internal, 2: Public
    rating         TINYINT,  -- int8, 0: General Audience, 1: Parental Guidance, 2: Parents Strongly Cautioned, 3: Restricted; 4: Adults Only; 127: Banned.
    mid            BLOB,     -- message id, xid, collection's title and summary
    cover          TEXT,     -- cover url
    updated_at     BIGINT,   -- update at, unix time with second precision.
    price          BIGINT,   -- -1: Free forever， 0: Free, > 0: Price in Yiwen Coin
    creation_price BIGINT,   -- -1: Free forever， 0: Free, > 0: Price in Yiwen Coin
    PRIMARY KEY (day, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'collections'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX collection_day_gid ON collection ((day), gid);

CREATE TABLE IF NOT EXISTS collection_children (
    id   BLOB,    -- parent collection id, 12 bytes XID
    cid  BLOB,    -- child collection or creation id
    kind TINYINT, -- int8, 0: creation, 1: publication, 2: collection
    ord  DOUBLE,  -- order value
    PRIMARY KEY (id, cid)
) WITH CLUSTERING ORDER BY (cid DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'collection children'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX collection_children_cid ON collection_children (cid);

CREATE TABLE IF NOT EXISTS collection_subscription (
    uid        BLOB,   -- user id who subscribe the collection
    cid        BLOB,   -- collection id, 12 bytes XID
    txn        BLOB,   -- latest subscription transaction id, 12 bytes XID
    updated_at BIGINT, -- updated at, unix time, ms
    expire_at  BIGINT, -- subscription expire at, unix time, second!!
    PRIMARY KEY (uid, cid)
) WITH CLUSTERING ORDER BY (cid DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'collection''s subscription'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX collection_subscription_cid ON collection_subscription (cid);

CREATE TABLE IF NOT EXISTS creation_subscription (
    uid        BLOB,   -- user id who subscribe the publication
    cid        BLOB,   -- creation id, 12 bytes XID
    txn        BLOB,   -- latest subscription transaction id, 12 bytes XID
    updated_at BIGINT, -- updated at, unix time, ms
    expire_at  BIGINT, -- subscription expire at, unix time, second!!
    PRIMARY KEY (uid, cid)
) WITH CLUSTERING ORDER BY (cid DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'creation''s subscription'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX creation_subscription_cid ON creation_subscription (cid);

CREATE TABLE IF NOT EXISTS bookmark (
    uid         BLOB,        -- user id who create the bookmark, 12 bytes XID
    id          BLOB,        -- bookmark id, 12 bytes XID
    kind        TINYINT,     -- int8, 0/1: Creation/Publication, 2: Collection
    cid         BLOB,        -- publication id or collection id, 12 bytes XID
    gid         BLOB,        -- group id, publication belong to
    language    TEXT,        -- language
    version     SMALLINT,    -- version
    updated_at  BIGINT,      -- update at, unix time with second precision.
    title       TEXT,        -- title
    labels      LIST<TEXT>,  -- labels for bookmarks management
    payload     BLOB,        -- payload in CBOR format
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'user''s bookmarks'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX bookmark_uid_cid ON bookmark ((uid), cid);
CREATE INDEX bookmark_cid ON bookmark (cid);

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

CREATE TABLE IF NOT EXISTS message (
    day        INT,    -- message birthday
    id         BLOB,   -- message id, 12 bytes XID
    attach_to  BLOB,   -- attach to, group id, collection id, etc.
    kind       TEXT,   -- attach kind: group, collection, etc.
    created_at BIGINT, -- created at, unix time, ms
    updated_at BIGINT, -- updated at, unix time, ms
    context    TEXT,   -- contextual definition for translation
    language   TEXT,   -- message's language, ISO 639-3
    languages  SET<TEXT>, -- exists languages, ISO 639-3
    version    SMALLINT,   -- creation version
    message    BLOB,   -- original message in CBOR format
    abk        BLOB,   -- Abkhazian message
    aar        BLOB,   -- Afar message
    afr        BLOB,   -- Afrikaans message
    aka        BLOB,   -- Akan message
    sqi        BLOB,   -- Albanian message
    amh        BLOB,   -- Amharic message
    ara        BLOB,   -- Arabic message
    arg        BLOB,   -- Aragonese message
    hye        BLOB,   -- Armenian message
    asm        BLOB,   -- Assamese message
    ava        BLOB,   -- Avaric message
    aze        BLOB,   -- Azerbaijani message
    bam        BLOB,   -- Bambara message
    bak        BLOB,   -- Bashkir message
    eus        BLOB,   -- Basque message
    bel        BLOB,   -- Belarusian message
    ben        BLOB,   -- Bengali message
    bis        BLOB,   -- Bislama message
    bos        BLOB,   -- Bosnian message
    bre        BLOB,   -- Breton message
    bul        BLOB,   -- Bulgarian message
    mya        BLOB,   -- Burmese message
    cat        BLOB,   -- Catalan message
    cha        BLOB,   -- Chamorro message
    che        BLOB,   -- Chechen message
    zho        BLOB,   -- Chinese message
    chu        BLOB,   -- Church Slavic message
    chv        BLOB,   -- Chuvash message
    cor        BLOB,   -- Cornish message
    cos        BLOB,   -- Corsican message
    hrv        BLOB,   -- Croatian message
    ces        BLOB,   -- Czech message
    dan        BLOB,   -- Danish message
    div        BLOB,   -- Dhivehi message
    nld        BLOB,   -- Dutch message
    dzo        BLOB,   -- Dzongkha message
    eng        BLOB,   -- English message
    epo        BLOB,   -- Esperanto message
    est        BLOB,   -- Estonian message
    ewe        BLOB,   -- Ewe message
    fao        BLOB,   -- Faroese message
    fin        BLOB,   -- Finnish message
    fra        BLOB,   -- French message
    ful        BLOB,   -- Fulah message
    glg        BLOB,   -- Galician message
    lug        BLOB,   -- Ganda message
    kat        BLOB,   -- Georgian message
    deu        BLOB,   -- German message
    guj        BLOB,   -- Gujarati message
    hat        BLOB,   -- Haitian message
    hau        BLOB,   -- Hausa message
    heb        BLOB,   -- Hebrew message
    hin        BLOB,   -- Hindi message
    hun        BLOB,   -- Hungarian message
    isl        BLOB,   -- Icelandic message
    ibo        BLOB,   -- Igbo message
    ind        BLOB,   -- Indonesian message
    ina        BLOB,   -- Interlingua message
    iku        BLOB,   -- Inuktitut message
    gle        BLOB,   -- Irish message
    ita        BLOB,   -- Italian message
    jpn        BLOB,   -- Japanese message
    jav        BLOB,   -- Javanese message
    kal        BLOB,   -- Kalaallisut message
    kan        BLOB,   -- Kannada message
    kas        BLOB,   -- Kashmiri message
    kaz        BLOB,   -- Kazakh message
    khm        BLOB,   -- Khmer message
    kik        BLOB,   -- Kikuyu message
    kin        BLOB,   -- Kinyarwanda message
    kir        BLOB,   -- Kirghiz message
    kor        BLOB,   -- Korean message
    kua        BLOB,   -- Kuanyama message
    kur        BLOB,   -- Kurdish message
    lao        BLOB,   -- Lao message
    lav        BLOB,   -- Latvian message
    lim        BLOB,   -- Limburgan message
    lin        BLOB,   -- Lingala message
    lit        BLOB,   -- Lithuanian message
    lub        BLOB,   -- Luba-Katanga message
    ltz        BLOB,   -- Luxembourgish message
    mkd        BLOB,   -- Macedonian message
    mlg        BLOB,   -- Malagasy message
    msa        BLOB,   -- Malay message
    mal        BLOB,   -- Malayalam message
    mlt        BLOB,   -- Maltese message
    glv        BLOB,   -- Manx message
    mri        BLOB,   -- Maori message
    mar        BLOB,   -- Marathi message
    ell        BLOB,   -- Modern Greek message
    mon        BLOB,   -- Mongolian message
    nav        BLOB,   -- Navajo message
    nep        BLOB,   -- Nepali message
    nde        BLOB,   -- North Ndebele message
    sme        BLOB,   -- Northern Sami message
    nor        BLOB,   -- Norwegian message
    nno        BLOB,   -- Norwegian Nynorsk message
    nya        BLOB,   -- Nyanja message
    oci        BLOB,   -- Occitan message
    ori        BLOB,   -- Oriya message
    orm        BLOB,   -- Oromo message
    oss        BLOB,   -- Ossetian message
    pan        BLOB,   -- Panjabi message
    fas        BLOB,   -- Persian message
    pol        BLOB,   -- Polish message
    por        BLOB,   -- Portuguese message
    pus        BLOB,   -- Pushto message
    que        BLOB,   -- Quechua message
    ron        BLOB,   -- Romanian message
    roh        BLOB,   -- Romansh message
    run        BLOB,   -- Rundi message
    rus        BLOB,   -- Russian message
    smo        BLOB,   -- Samoan message
    sag        BLOB,   -- Sango message
    san        BLOB,   -- Sanskrit message
    gla        BLOB,   -- Scottish Gaelic message
    srp        BLOB,   -- Serbian message
    sna        BLOB,   -- Shona message
    iii        BLOB,   -- Sichuan Yi message
    snd        BLOB,   -- Sindhi message
    sin        BLOB,   -- Sinhala message
    slk        BLOB,   -- Slovak message
    slv        BLOB,   -- Slovenian message
    som        BLOB,   -- Somali message
    nbl        BLOB,   -- South Ndebele message
    sot        BLOB,   -- Southern Sotho message
    spa        BLOB,   -- Spanish message
    sun        BLOB,   -- Sundanese message
    swa        BLOB,   -- Swahili message
    ssw        BLOB,   -- Swati message
    swe        BLOB,   -- Swedish message
    tgl        BLOB,   -- Tagalog message
    tah        BLOB,   -- Tahitian message
    tgk        BLOB,   -- Tajik message
    tam        BLOB,   -- Tamil message
    tat        BLOB,   -- Tatar message
    tel        BLOB,   -- Telugu message
    tha        BLOB,   -- Thai message
    bod        BLOB,   -- Tibetan message
    tir        BLOB,   -- Tigrinya message
    ton        BLOB,   -- Tonga message
    tso        BLOB,   -- Tsonga message
    tsn        BLOB,   -- Tswana message
    tur        BLOB,   -- Turkish message
    tuk        BLOB,   -- Turkmen message
    uig        BLOB,   -- Uighur message
    ukr        BLOB,   -- Ukrainian message
    urd        BLOB,   -- Urdu message
    uzb        BLOB,   -- Uzbek message
    ven        BLOB,   -- Venda message
    vie        BLOB,   -- Vietnamese message
    cym        BLOB,   -- Welsh message
    fry        BLOB,   -- Western Frisian message
    wol        BLOB,   -- Wolof message
    xho        BLOB,   -- Xhosa message
    yid        BLOB,   -- Yiddish message
    yor        BLOB,   -- Yoruba message
    zul        BLOB,   -- Zulu message
    PRIMARY KEY (day, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'phrases and sentences'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;
