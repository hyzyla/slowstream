# SlowStream

- [x] pydantic body
- [x] raw body
- [x] startup/shutdown
```python
@app.startup
def connect_db(_app: ConsumerApp) -> Iterator[None]:
    print('Connect DB...')
    try:
        yield
    finally:
        print("Disconnecting DB...")
```
- [ ] middlewares
- [ ] error handling
```python
@app.middleware
def error_middleware(handler, message):
    try:
        handler(message)
    except BaseException:
        print("Some error :plak-plak")
```
- [ ] test
- [ ] blueprint/router/sub-consumer
- [ ] async functions
- [ ] reliable (do not loosing messages)
- [ ] concurrent topic execution
- [ ] sentry integration
- [ ] pytest integration
- [ ] fastapi integration
