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
	  Id		INTEGER PRIMARY KEY ASC AUTOINCREMENT,
	  Name      VARCHAR(32) UNIQUE,

	  Modified  INTEGER NOT NULL DEFAULT 0
	);

	INSERT INTO Collections (Id, Name) VALUES
		( 1, "clients"),
		( 2, "crypto"),
		( 3, "forms"),
		( 4, "history"),
		( 5, "keys"),
		( 6, "meta"),
		( 7, "bookmarks"),
		( 8, "prefs"),
		( 9, "tabs"),
		(10, "passwords"),
		(11, "addons"),
		-- forces new collections to start at 100
		(99, "-push-");

	CREATE TABLE KeyValues (
		Key VARCHAR(32) NOT NULL,
		Value VARCHAR(32) NOT NULL,
		PRIMARY KEY (Key)
	);

	INSERT INTO KeyValues (Key, Value) VALUES ("SCHEMA_VERSION", 0);
	`
