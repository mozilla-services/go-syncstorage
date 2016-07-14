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

## Advanced Configuration
Things that probably shouldn't be touched:

| Env. Var | Info |
|---|---|
| `POOL_NUM` | Number of DB pools. Defaults to number of CPUs.  |
| `POOL_SIZE` | Number of open DB files per pool. Defaults to `25`.  |

go-syncstorage limits the number of open SQLite database files to keep memory usage constant. This allows a small server to handle thousands of users for a small performance hit.

Multiplying `POOL_NUM x POOL_SIZE` gives the maximum number of open files.

A low level lock is used in each pool when opening and closing files. Having a larger `POOL_NUM` decreases lock contention.

When a pool reaches `POOL_SIZE` number of open files it will close the least recently used database. Having a larger `POOL_SIZE` reduces open/close disk IO. It also increases memory usage.

Tweaking these values from default won't provide significant performance gains in production. However, a `POOL_NUM=1` and `POOL_SIZE=1` is useful for testing the overhead of opening and closing databases files.

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
