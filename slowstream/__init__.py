import contextlib
import inspect

from dataclasses import dataclass
from typing import List, Any, Dict, Callable, Optional, Iterator, ContextManager

import confluent_kafka as kafka
from pydantic import BaseModel



@dataclass
class Message:
    topic: str
    key: bytes
    value: bytes



ConsumerHandler = Callable


StartupHandler = Callable[['ConsumerApp'], Iterator[Any]]


RawConsumerHandler = Callable[[Message], None]
ConsumerMiddleware = Callable[[RawConsumerHandler, Message], None]


class Handler:
    def __init__(
        self,
        func: ConsumerHandler,
        middleware: List[ConsumerMiddleware],
    ) -> None:
        self.func = func
        self.parameters = {}
        signature = inspect.signature(self.func)
        for parameter in signature.parameters.values():
            if parameter.annotation is not None:
                self.parameters[parameter.name] = parameter.annotation

    def execute(self, message: Message) -> None:
        kwargs = {}
        for param_name, param_type in self.parameters.items():
            if type(param_type) == Message:
                kwargs[param_name] = message
            elif issubclass(param_type, BaseModel):
                kwargs[param_name] = param_type.parse_raw(message.value)
            else:
                ValueError(f"Unknown type: {param_type}")
        
        self.func(**kwargs)


class ConsumerApp:

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
    ) -> None:
        self._config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        self._handlers: Dict[str, Handler] = {}
        self.__consumer: Optional[kafka.Consumer] = None
        self._startup_handlers: List[ContextManager] = []
        self._middlewares: List[Callable[[RawConsumerHandler, Message], None]] = []

    @contextlib.contextmanager
    def _context_manager(self) -> Iterator[None]:
        try:
            self._start_consumer()
            yield
        finally:
            self._stop_consumer()

    def _start_consumer(self) -> None:
        self.__consumer = kafka.Consumer(self._config)

    def _stop_consumer(self) -> None:
        consumer = self.__consumer
        if consumer is not None:
            consumer.close()

    @property
    def _consumer(self) -> kafka.Consumer:
        if self.__consumer is None:
            raise ValueError("Consumer is not started. Use .run method to run consumer")
        return self.__consumer

    @property
    def topics(self) -> List[str]:
        return list(self._handlers.keys())

    def _consume(self) -> None:
        self._consumer.subscribe(self.topics)

        while True:
            messages: List[kafka.Message] = self._consumer.consume()
            for raw_message in messages:
                if raw_message.error():
                    print("Consumer error: {}".format(raw_message.error()))
                    continue

                message = Message(
                    key=raw_message.key(),
                    topic=raw_message.topic(),
                    value=raw_message.value(),
                )
                handler: Handler = self._handlers[message.topic]
                handler.execute(message)

    def run(self):

        with contextlib.ExitStack() as stack:

            # register consumer context manager itself
            stack.enter_context(cm=self._context_manager())

            # register all startup handlers
            for handler in self._startup_handlers:
                stack.enter_context(cm=handler)

            self._consume()

    # ==== setup methods ====

    def add_startup_handler(self, handler: ContextManager) -> None:

        self._startup_handlers.append(handler)

    def startup(self, handler: Callable[['ConsumerApp'], Iterator[Any]]) -> StartupHandler:
        h = contextlib.contextmanager(handler)
        self.add_startup_handler(handler=h(self))
        return handler

    def middleware(self, handler):
        return handler

    def add_subscriber(self, topic: str, handler: ConsumerHandler) -> None:
        if topic in self._handlers:
            raise ValueError("Topic is already registered")
        self._handlers[topic] = Handler(func=handler)

    def subscribe(self, topic: str) -> Callable[[ConsumerHandler], None]:

        def wrapped(handler: ConsumerHandler) -> None:
            self.add_subscriber(topic=topic, handler=handler)

        return wrapped
