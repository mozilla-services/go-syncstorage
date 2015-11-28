package syncstorage

const SCHEMA_0 = `
	CREATE TABLE BSO (
	  CollectionId	 INTEGER NOT NULL,
	  Id             VARCHAR(64) NOT NULL,

	  SortIndex      INTEGER DEFAULT 0,

	  Payload        TEXT NOT NULL DEFAULT '',
	  PayloadSize    INTEGER NOT NULL DEFAULT 0,

	  -- milliseconds since unix epoch. Sync 1.5 spec says it shoud
	  -- be a float of seconds since epoch accurate to two decimal places
	  -- convert it in the API response, but work with it as an int
	  Modified       INTEGER NOT NULL,

	  TTL            INTEGER NOT NULL,

	  PRIMARY KEY (CollectionId, Id)
	);

	CREATE TABLE Collections (
	  -- storage an integer to save some space
	  Id		INTEGER PRIMARY KEY ASC,
	  Name      VARCHAR(32) UNIQUE,

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
