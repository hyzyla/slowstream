import asyncio
from typing import Iterator, Callable

from slowstream import ConsumerApp, Message
from pydantic import BaseModel


app = ConsumerApp(
    bootstrap_servers='localhost:29092',
    group_id="group_1",
)


class Notification(BaseModel):
    email: str
    subject: str
    content: str


@app.startup
def connect_db(_app: ConsumerApp) -> Iterator[None]:
    print('Connect DB...')
    try:
        yield
    finally:
        print("Disconnecting DB...")


@app.middleware
def error_middleware(handler: Callable[[Message], None], message: Message):
    try:
        handler(message)
    except BaseException:
        print("Some error :plak-plak")


@app.subscribe("send-email-notification")
def send_email_notification(notification: Notification) -> None:
    print(f'topic=send_email_notification, type={type(notification)}, value={notification}')


@app.subscribe("async-topic")
async def async_consume() -> None:
    print(f'topic=async_consume')
    await asyncio.sleep(1)


@app.subscribe("empty-params")
def empty_params() -> None:
    print(f'topic=empty_params')


@app.subscribe("advanced-message")
def advanced_body(message: Message) -> None:
    print(f'topic=advanced_body, type={type(message)}, value={message}')


if __name__ == '__main__':
    app.run()