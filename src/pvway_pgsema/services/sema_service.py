import asyncio

from pvway_sema_abs.semaphore_service import SemaphoreService

from pvway_pgsema.di.pvway_pgsema_config import PvWayPgSemaConfig
from injector import inject


class SemaService(SemaphoreService):
    @inject
    def __init__(self, config: PvWayPgSemaConfig):
        print('in SemaService.init')
        self._schema_name = config.schema_name
        self._table_name = config.table_name
        self._get_cs_async = config.get_cs_async
        self._log_exception = config.log_exception
        self._log_info = config.log_info

    def __private_method(self):
        pass

    def print_config(self):
        cs = asyncio.run(self._get_cs_async())
        print(self._schema_name)
        print(self._table_name)
        print(cs)
        self._log_exception(Exception('some exception'))
        self._log_info('some info')
