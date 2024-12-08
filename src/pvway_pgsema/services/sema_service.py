import asyncio
import datetime

from typing import Callable, Optional, TypeVar

from asyncpg import Connection
from asyncpg import connect

from injector import inject

from pvway_sema_abs.semaphore_service import SemaphoreService
from pvway_sema_abs.semaphore_info import SemaphoreInfo
from pvway_sema_abs.semaphore_status_enu import SemaphoreStatusEnu

from pvway_pgsema.di.pvway_pgsema_config import PvWayPgSemaConfig
from pvway_pgsema.model.db_semaphore import DbSemaphore


# noinspection SqlNoDataSourceInspection
class SemaService(SemaphoreService):
    @inject
    def __init__(self, config: PvWayPgSemaConfig):
        print('in SemaService.init')
        self._schema_name = config.schema_name
        self._table_name = config.table_name
        self._get_cs_async = config.get_cs_async
        self._log_exception = config.log_exception
        self._log_info = config.log_info

    __name_field = "Name"
    __owner_field = "Owner"
    __timeout_in_seconds_field = "TimeOutInSeconds"
    __create_date_utc_field = "CreateDateUtc"
    __update_date_utc_field = "UpdateDateUtc"

    def __private_method(self):
        pass

    def print_config(self):
        cs = asyncio.run(self._get_cs_async())
        print(self._schema_name)
        print(self._table_name)
        print(cs)
        self._log_exception(Exception('some exception'))
        self._log_info('some info')

    async def acquire_semaphore_async(
            self,
            name: str, owner: str, timeout: datetime.timedelta) -> SemaphoreInfo:

        # name = DaoHelper.truncate_then_escape(name, 50)

        cs = await self._get_cs_async()
        cn: Connection = await connect(cs)
        try:
            await self.__create_table_if_not_exists_async(cn)
        finally:
            await cn.close()

        utc_now = datetime.datetime.now(datetime.UTC)

        return DbSemaphore(
            SemaphoreStatusEnu.ACQUIRED,
            name,
            owner,
            timeout,
            utc_now, utc_now)


    async def touch_semaphore_async(
            self,
            name: str) -> None:
        pass

    async def release_semaphore_async(
            self,
            name: str) -> None:
        pass

    async def get_semaphore_async(
            self,
            name: str) -> SemaphoreInfo:
        utc_now = datetime.datetime.now(datetime.UTC)
        return DbSemaphore(
            SemaphoreStatusEnu.ACQUIRED,
            'name',
            'owner',
            datetime.timedelta(minutes=5),
            utc_now, utc_now)

    T = TypeVar('T')

    async def isolate_work_async(
            self,
            semaphore_name: str,
            owner: str,
            timeout: datetime.timedelta,
            work_async: Callable[[], asyncio.Future[T]],
            notify: Optional[Callable[[str], None]] = None,
            sleep_between_attempts: datetime.timedelta =
                datetime.timedelta(seconds=15)) -> T:
        """
        :param semaphore_name: The name of the semaphore to be used for synchronizing access.
        :param owner: The identifier for the entity attempting to gain access to the semaphore.
        :param timeout: The duration to wait for acquiring the semaphore before giving up.
        :param work_async: An asynchronous callable that performs the work requiring isolated access.
        :param notify: An optional callable to be invoked with status notifications, typically used for logging or alerts.
        :param sleep_between_attempts: The duration to sleep between attempts to acquire the semaphore when it is unavailable.
        :return: The result of the work executed within the isolated context.
        """
        pass


    async def __create_table_if_not_exists_async(
            self, cn: Connection):
        exists_command_text = (
                "SELECT 1 FROM information_schema.tables "
                + f"   WHERE table_schema = '{self._schema_name}' "
                + f"   AND   table_name = '{self._table_name}' "
        )

        rs = await cn.fetchrow(exists_command_text)
        table_exists = (rs is not None)

        if table_exists:
            return

        self._log_info("schema or table does not exists yet")

        # need to be db owner for this to work
        try:
            self._log_info(f"creating schema {self._schema_name} "
                           + f"if it does not yet exists")
            create_schema_text = f"CREATE SCHEMA IF NOT EXISTS \"{self._schema_name}\""

            await cn.execute(create_schema_text)

            self._log_info(f"creating table {self._table_name}")
            create_command_text = (
                    f"CREATE TABLE \"{self._schema_name}\".\"{self._table_name}\" ("
                    + f" \"{self.__name_field}\" character varying (50) PRIMARY KEY, "
                    + f" \"{self.__owner_field}\" character varying (128) NOT NULL, "
                    + f" \"{self.__timeout_in_seconds_field}\" integer NOT NULL, "
                    + f" \"{self.__create_date_utc_field}\" timestamp NOT NULL, "
                    + f" \"{self.__update_date_utc_field}\" timestamp NOT NULL"
                    + ")"
            )
            await cn.execute(create_command_text)

        except Exception as e:
            self._log_exception(e)
            raise
