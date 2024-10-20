POST http://127.0.0.1:8787/-_-/v1/wikis.Create
Tiddlyflare-Api-Key: tf_key_TENANT1111_sometoken
```json
{
    "name": "notes-1",
    "wikiType": "tw5"
}
```
HTTP 200
[Captures]
wikiUrl: jsonpath "$.redirectUrl"

GET http://127.0.0.1:8787/{{ wikiUrl }}
Tiddlyflare-Api-Key: tf_key_TENANT1111_sometoken
HTTP 200
