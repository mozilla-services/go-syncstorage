About
-----
generate-hawk-header creates a Hawk authentication string with a valid token. This
can be used to make requests to a sync storage server with curl.

For example:

```
URL="http://localhost:8888/1.5/12345/info/configuration"; \
	HAWK=$(go run ./main.go "$URL" "SECRET"); \
	curl -vH "Authorization: $HAWK" "$URL"
```

Remember: 

1. The URL must be an absolute url. This includes the correct protocol schema, sync node hostname, port and path. 
2. The correct secret for the sync node must used

