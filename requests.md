curl -X POST http://localhost:5001/create \
   -H "Content-Type: application/json" \
   -d '{"key":"foo","value":"bar"}'

curl -X GET http://localhost:5001/read \
   -H "Content-Type: application/json" \
   -d '{"key":"foo"}'

curl -X PATCH http://localhost:5001/update \
   -H "Content-Type: application/json" \
   -d '{"key":"foo","value":"baz"}'

curl -X DELETE http://localhost:5001/delete \
   -H "Content-Type: application/json" \
   -d '{"key":"foo"}'
