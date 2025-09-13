## How to debug
```shell
go run ./cmd/hub
```

## API calls
### Subscribe markets
```shell
curl -X POST http://localhost:8080/api/v1/mkt -H "Content-Type: application/json" -d '{"pairs": [ {"c1": "KRW", "c2": "BTC"}, {"c1": "KRW", "c2": "XRP"} ] }'
```

### UnSubscribe markets
```shell
curl -X DELETE http://localhost:8080/api/v1/mkt -H "Content-Type: application/json" -d '{"pairs": [ {"c1": "KRW", "c2": "BTC"}, {"c1": "KRW", "c2": "XRP"} ] }'
```
