from typing import Callable, Coroutine, Any, Optional
from injector import Binder, singleton

from pvway_pgsema.module.sema_module import SemaModule
from pvway_pgsema.services.sema_config import SemaConfig


def pvway_pgsema_install(
        binder: Binder,
        schema_name: str,
        table_name: str,
        get_cs_async: Callable[[], Coroutine[Any, Any, str]],
        log_exception: Callable[[Exception], None],
        log_info: Optional[Callable[[str], None]]) -> None:
    """
    :param binder: A Binder object used to install the necessary configurations and modules for the sema.
    :param schema_name: The name of the schema to be configured in the sema setup.
    :param table_name: The name of the table within the specified schema to be configured.
    :param get_cs_async: An asynchronous callable that returns a coroutine yielding a string, used for obtaining a configuration string.
    :param log_exception: A callable function that takes an Exception as an argument, used for logging exceptions.
    :param log_info: An optional callable function that takes a string as an argument, used for logging informational messages.
    :return: This function does not return any value.
    """
    binder.install(
        configure_sema_config(
            schema_name=schema_name,
            table_name=table_name,
            get_cs_async=get_cs_async,
            log_exception=log_exception,
            log_info=log_info
        ))
    binder.install(SemaModule())


def configure_sema_config(
        schema_name: str,
        table_name: str,
        get_cs_async: Callable[[], Coroutine[Any, Any, str]],
        log_exception: Callable[[Exception], None],
        log_info: Optional[Callable[[str], None]]):
    def configure(binder: Binder) -> None:
        config = SemaConfig(
            schema_name,
            table_name,
            get_cs_async,
            log_exception,
            log_info)
        binder.bind(SemaConfig, to=config, scope=singleton)

    print('in configure_sema_config')
    return configure
