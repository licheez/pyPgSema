from typing import Callable, Coroutine, Any, Optional

from injector import Binder, singleton

from pvway_pgsema.module.sema_module import SemaModule
from pvway_pgsema.services.sema_config import SemaConfig


class PvWayPgSemaConfigurer:
    def __init__(self,
                 schema_name: str,
                 table_name: str,
                 get_cs_async: Callable[[], Coroutine[Any, Any, str]],
                 log_exception: Callable[[Exception], None],
                 log_info: Optional[Callable[[str], None]] = None):
        self.__schema_name = schema_name
        self.__table_name = table_name
        self.__get_cs_async = get_cs_async
        self.__log_exception = log_exception
        self.__log_info = log_info

    def __configure_sema(self, binder: Binder) -> None:
        config = SemaConfig(
            self.__schema_name,
            self.__table_name,
            self.__get_cs_async,
            self.__log_exception,
            self.__log_info
        )
        binder.bind(SemaConfig, to=config, scope=singleton)
        print('Configuring sema...')

    def install(self, binder: Binder) -> None:
        """
        :param binder: A Binder object used to install configurations and modules for the sema.
        """
        binder.install(self.__configure_sema)
        binder.install(SemaModule())

