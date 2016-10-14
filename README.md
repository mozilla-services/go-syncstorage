[![CircleCI](https://circleci.com/gh/mozilla-services/go-syncstorage.svg?style=svg)](https://circleci.com/gh/mozilla-services/go-syncstorage)

# Mozilla Sync 1.5 Storage Server in Go

Go-syncstorage is the next generation sync storage server. It was built to solve the data degradation problem with the python+mysql implementation. Logical separation of data is now physical separation of data.  In go-syncstorage each user gets their own sqlite database. Many indexes were harmed in the making of this product.

## Installing and Running it

The server is distributed as a Docker container. Latest builds and releases can be found on [Dockerhub](https://hub.docker.com/r/mozilla/go-syncstorage).

Running the server is easy:

```bash
$ docker pull mozilla/go-syncstorage:latest
$ docker run -it \
  -e "PORT=8000" \                           [1]
  -e "SECRETS=secret0,secret1,secret2" \     [2]
  -e "DATA_DIR=/data" \                      [3]
  -v "/host/data/path:/data" \               [4]
  mozilla/go-syncstorage
```

Only three configurations are required: `PORT`, `SECRETS` and `DATA_DIR`.

1. `PORT` - where to listen for HTTP requests
2. `SECRETS` - CSV of secrets preshared with the [token service](https://github.com/mozilla-services/tokenserver/)
3. `DATA_DIR` - where to save files (relative to inside the container)
4. A volume mount so data is saved on the docker host machine

## More Configuration

The server has a few knobs that can be tweaked.

| Env. Var | Info |
|---|---|
| `HOST` | Address to listen on. Defaults to `0.0.0.0`. |
| `PORT` | Port to listen on |
| `DATA_DIR` | Where to save DB files. Use an absolute path. `:memory:` is also valid and saves sqlite databases in RAM only. Recommended only during testing and development. |
| `SECRETS` | Comma separated list of shared secrets. Secrets are tried in order and allows for secret rotation without downtime. |
| `LOG_LEVEL`| Log verbosity, allowed: `fatal`,`error`,`warn`,`debug`,`info`|
| `LOG_MOZLOG` | Can be `true` or `false`. Outputs logs in [mozlog](https://github.com/mozilla-services/Dockerflow/blob/master/docs/mozlog.md) format. |
| `LOG_DISABLE_HTTP` | Can be `true` or `false`. Disables logging of HTTP requests. |
| `HOSTNAME` | Set a hostname value for mozlog output |
| `LIMIT_MAX_REQUESTS_BYTES` | The maximum size in bytes of the overall HTTP request body that will be accepted by the server. |
| `LIMIT_MAX_BSO_GET_LIMIT` |  Max BSOs that can be returned per GET request. Default: 2500. |
| `LIMIT_MAX_POST_BYTES` |  Maximum size of a POST request. Default: 2097152 (2MB). |
| `LIMIT_MAX_POST_RECORDS` |  Maximum number of BSOs per POST request. Default 100. |
| `LIMIT_MAX_TOTAL_BYTES` |  Maximum total size of a POST batch job. Default: 26,214,400 (20MB). |
| `LIMIT_MAX_TOTAL_RECORDS` | Maximum total BSOs in a POST batch job. Default 1000. |
| `LIMIT_MAX_BATCH_TTL` | Maximum TTL for a batch to remain uncommitted in seconds. Default 7200 (2 hours). |
| `INFO_CACHE_SIZE` | Cache size in MB for `<uid>/info/collections` and `<uid>/info/configuration`. Default 0 (disabled) | 

## Advanced Configuration

| Env. Var | Info |
|---|---|
| `POOL_NUM` | Number of DB pools. Defaults to number of CPUs.  |
| `POOL_SIZE` | Number of open DB files per pool. Defaults to `25`.  |
| `POOL_VACUUM_KB` | Threshold of free space in kilobytes to trigger a database vacuum. Defaults to `0` (disabled). |
| `POOL_PURGE_MIN_HOURS	` | Minimum hours before purging BSOs, Batches, etc for a user. Defaults to `168` (1 week) |
| `POOL_PURGE_MAX_HOURS	` | Max hours before purging. Defaults to `336` (2 weeks). |

go-syncstorage limits the number of open SQLite database files to keep memory usage constant. This allows a small server to handle thousands of users for a small performance hit.

Multiplying `POOL_NUM x POOL_SIZE` gives the maximum number of open files. The product should to large enough so pools are not starved and have to clean up too often. A sign things are too small is when `sql: database is closed` errors appear in the logs.

A low level lock is used in each pool when opening and closing files. Having a larger `POOL_NUM` decreases lock contention.

When a pool reaches `POOL_SIZE` number of open files it will close the least recently used database. Having a larger `POOL_SIZE` reduces open/close disk IO. It also increases memory usage.

Tweaking these values from default won't provide significant performance gains in production. However, a `POOL_NUM=1` and `POOL_SIZE=1` is useful for testing the overhead of opening and closing databases files.

The `POOL_PURGE_MIN_HOURS` and `POOL_PURGE_MAX_HOURS` define a time range to trigger a purge job for a user. The default range is between 168 and 336 hours. This means a user will have a purge job run only once every one to two weeks. A large range spreads evens out IO load.

The `POOL_VACUUM_KB` sets the threshold before a vacuum is run. Purging of batches and BSOs free up database pages but not disk space. A vacuum will rewrite the database, defragment it and free up disk space. Depending on the number of records it can take seconds to vacuum a database.

### Sqlite3 Tweaks 

| Env. Var | Info |
|---|---|
| `SQLITE3_CACHE_SIZE` | Sets sqlite's internal cache size for each open DB. Busy servers open/close the db files often so a smaller cache size may be more efficient. Follows the [PRAGMA cache_size](https://www.sqlite.org/pragma.html#pragma_cache_size) rules. Positive integers are number of pages to cache, negative numbers are KB of RAM to use for cache. Default 0 (no cache)|


## Data Storage

When deploying choose the EXT4 filesystem. EXT4 is an extent based filesystem and may help improve performance for magnetic storage media.

go-syncstorage gives each user gets their own sqlite database. On a production server that enough files to be a real burden for a human when troubleshooting. Thus, files are created into a directory structure like this:

```
/data-dir/
   00/
   01/
   34/
     21/
       100001234.db
   ...
   99/
```

* Two levels of subdirectories, each with 100 subdirectories for total of 10,000 sub-directories.
* The user, `100001234`, is located at `34/21/100001234.db`. The path starts at the reverse of their id. Their id is used for the actual database name.
* Using the reverse order helps evenly balance the number of files per directory.

Using this scheme, one million users will only have 10,000 files per directory. This is a relatively low number that CLI tools like `ls` will have no trouble with. Always optimize for the proper care and feed of your sysadmins.


## Other Releases

A linux binary is also available as build artifacts from [Circle CI](https://circleci.com/gh/mozilla-services/go-syncstorage).

# License

See [LICENSE](LICENSE.txt).
