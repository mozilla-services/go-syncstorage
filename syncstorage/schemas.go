package syncstorage

const SCHEMA_0 = `
	CREATE TABLE BSO (
	  CollectionId	 INTEGER NOT NULL,
	  Id             VARCHAR(64) NOT NULL,
	  SortIndex      INTEGER DEFAULT 0,

	  Payload        TEXT NOT NULL DEFAULT '',
	  PayloadSize    INTEGER NOT NULL DEFAULT 0,

	  Modified       bigint NOT NULL,

	  -- default TTL of 2100000000 is in the current setup, keeping
	  -- the same incase of introducing bugs
	  TTL            INTEGER NOT NULL DEFAULT 2100000000,

	  PRIMARY KEY (CollectionId, Id)
	);

	CREATE TABLE Collections (
	  -- storage an integer to save some space
	  Id		INTEGER PRIMARY KEY ASC,
	  Name      VARCHAR(32) UNIQUE,

	  -- Size is a cached value of sum(BSO.PayloadSize)
	  Size		INTEGER NOT NULL DEFAULT 0,
	  Modified  INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE KeyValues (
		Key VARCHAR(32) NOT NULL,
		Value VARCHAR(32) NOT NULL,
		PRIMARY KEY (Key)
	);

	INSERT INTO KeyValues (Key, Value) VALUES ("SCHEMA_VERSION", 0);
	INSERT INTO Collections (Name) VALUES
		("bookmarks"),
		("history"),
		("forms"),
		("prefs"),
		("tabs"),
		("passwords"),
		("crypto"),
		("client"),
		("keys"),
		("meta");
	`
