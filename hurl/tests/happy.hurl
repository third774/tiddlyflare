#########################
# Create a new wiki.

POST http://127.0.0.1:8787/-_-/v1/wikis.Create
Tiddlyflare-Api-Key: tf_key_TENANT1111_sometoken
```json
{
    "name": "notes-1",
    "wikiType": "tw5"
}
```
HTTP 201
[Captures]
wikiUrl: jsonpath "$.redirectUrl"

#########################
# Assert that we get some HTML back.

GET http://127.0.0.1:8787/{{ wikiUrl }}
Tiddlyflare-Api-Key: tf_key_TENANT1111_sometoken
HTTP 200
[Captures]
wikiHtmlBytes: bytes
[Asserts]
bytes count > 2000000
xpath "string(/html/head/title)" contains "My TiddlyWiki"

#########################
# Assert that we get the same HTML back a 2nd time.

GET http://127.0.0.1:8787/{{ wikiUrl }}
Tiddlyflare-Api-Key: tf_key_TENANT1111_sometoken
HTTP 200
[Asserts]
bytes == {{ wikiHtmlBytes }}

#########################
# Assert that we support the TiddlyWiki endpoints.

OPTIONS http://127.0.0.1:8787/{{ wikiUrl }}
HTTP 200
[Asserts]
header "dav" == "tw5/put"
