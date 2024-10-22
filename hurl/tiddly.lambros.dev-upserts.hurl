#########################
# Create a new wiki.

POST {{ BASEURL }}/-_-/v1/wikis.Create
Tiddlyflare-Api-Key: {{ TIDDLYFLARE_API_KEY }}
```json
{
    "name": "testing-notes-01",
    "wikiType": "tw5"
}
```
HTTP 201
[Captures]
wikiUrl: jsonpath "$.redirectUrl"

#########################
# Assert that we get some HTML back.

GET {{ BASEURL }}{{ wikiUrl }}
Tiddlyflare-Api-Key: {{ TIDDLYFLARE_API_KEY }}
HTTP 200
[Captures]
wikiHtmlBytes: bytes
[Asserts]
bytes count > 2000000
xpath "string(/html/head/title)" contains "My TiddlyWiki"
