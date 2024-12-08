import asyncio
import datetime

from typing import Callable, Optional, TypeVar

from asyncpg import Connection, exceptions
from asyncpg import connect

from injector import inject

from pvway_sema_abs.semaphore_service import SemaphoreService
from pvway_sema_abs.semaphore_info import SemaphoreInfo
from pvway_sema_abs.semaphore_status_enu import SemaphoreStatusEnu

from pvway_pgsema.di.pvway_pgsema_config import PvWayPgSemaConfig
from pvway_pgsema.helpers.dao_helper import DaoHelper
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
    __create_date_wo_tz_field = "CreateDateWoTz"
    __update_date_wo_tz_field = "UpdateDateWoTz"

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

        name = DaoHelper.truncate_then_escape(name, 50)
        utc_now = datetime.datetime.now(datetime.UTC)

        cs = await self._get_cs_async()
        cn: Connection = await connect(cs)
        try:
            await self.__create_table_if_not_exists_async(cn)

            owner = DaoHelper.truncate_then_escape(owner, 50)
            sql_utc_now = DaoHelper.get_timestamp(utc_now)
            timeout_in_seconds = timeout.total_seconds()

            insert_command_text = (
                f"INSERT INTO \"{self._schema_name}\".\"{self._table_name}\" "
                f"   (\"{self.__name_field}\", \"{self.__owner_field}\", "
                f"    \"{self.__timeout_in_seconds_field}\", "
                f"    \"{self.__create_date_wo_tz_field}\", "
                f"    \"{self.__update_date_wo_tz_field}\") "
                f"VALUES ('{name}', '{owner}', "
                f"        {timeout_in_seconds}, "
                f"        '{sql_utc_now}', '{sql_utc_now}')"
            )
            await cn.execute(insert_command_text)

            # INSERT SUCCEEDED
            self.__log_info(f"'{owner}' acquired semaphore '{name}'")

            return DbSemaphore(
                SemaphoreStatusEnu.ACQUIRED,
                name,
                owner,
                timeout,
                utc_now, utc_now)

        except exceptions.UniqueViolationError:
            # INSERT FAILED
            try:
                self.__log_info(f"semaphore '{name}' was not acquired")
                f_semaphore = await self.__get_semaphore_async(cn, name)
                if f_semaphore is None:
                    self.__log_info(f"{name} was released in the mean time")
                    # the semaphore was released (deleted) in the meantime
                    return DbSemaphore(
                        SemaphoreStatusEnu.RELEASE_IN_THE_MEAN_TIME,
                        name, None, timeout,
                        utc_now, utc_now)

                # the semaphore was found
                time_elapsed = utc_now - f_semaphore.update_date_utc
                # if the elapsed time is less than the timeout limit
                # consider the semaphore is still valid
                if time_elapsed <= timeout:
                    self.__log_info(
                        f"semaphore '{name}' is still in use "
                        f"by '{f_semaphore.owner}'")
                    return DbSemaphore.from_semaphore_info(
                        SemaphoreStatusEnu.OWNED_BY_SOMEONE_ELSE,
                        f_semaphore
                    )

                # the elapsed time is greater than the timeout limit
                # force the release of the semaphore
                self.__log_info(f"semaphore '{name}' was released by force")
                await self.__release_semaphore_async(cn, name)
                return DbSemaphore.from_semaphore_info(
                    SemaphoreStatusEnu.FORCED_RELEASE,
                    f_semaphore
                )

            except Exception as e:
                self._log_exception(e)
                raise

        finally:
            await cn.close()

    async def touch_semaphore_async(
            self,
            name: str) -> None:
        name = DaoHelper.truncate_then_escape(name, 50)
        cs = await self._get_cs_async()
        cn: Connection = await connect(cs)
        utc_now = datetime.datetime.now(datetime.UTC)
        sql_utc_now = DaoHelper.get_timestamp(utc_now)
        update_text = (
            f"UPDATE \"{self._schema_name}\".\"{self._table_name}\" "
            f"SET \"{self.__update_date_wo_tz_field}\" = {sql_utc_now} "
            f"WHERE \"{self.__name_field}\" = '{name}'"
        )
        try:
            await cn.execute(update_text)
            self.__log_info(f"semaphore '{name}' was touched")
        except Exception as e:
            self.__log_info(update_text)
            self._log_exception(e)
            raise
        finally:
            await cn.close()

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

    async def __release_semaphore_async(
            self, cn: Connection, name: str) -> None:

        delete_text = (
            f"DELETE FROM \"{self._schema_name}\".\"{self._table_name}\" "
            f"WHERE \"{self.__name_field}\" = '{name}'"
        )
        await cn.execute(delete_text)

    async def __get_semaphore_async(
            self, cn: Connection, name: str) -> DbSemaphore | None:
        select_text = (
            "SELECT "
            f" \"{self.__owner_field}\", "
            f" \"{self.__timeout_in_seconds_field}\", "
            f" \"{self.__create_date_wo_tz_field}\", "
            f" \"{self.__update_date_wo_tz_field}\" "
            f"FROM \"{self._schema_name}\".\"{self._table_name}\" "
            f"WHERE \"{self.__name_field}\" = '{name}'"
        )
        rs = await cn.fetchrow(select_text)
        if rs is None:
            return None

        owner = rs[self.__owner_field]
        timeout_in_seconds = rs[self.__timeout_in_seconds_field]
        create_date = rs[self.__create_date_wo_tz_field]
        update_date = rs[self.__update_date_wo_tz_field]

        timeout = datetime.timedelta(seconds=timeout_in_seconds)
        return DbSemaphore(
            name=name,
            status=SemaphoreStatusEnu.ACQUIRED,
            owner=owner,
            timeout=timeout,
            create_date_wo_tz=create_date,
            update_date_wo_tz=update_date
        )

    def __log_info(self, info: str) -> None:
        if self._log_info:
            self._log_info(info)

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

        self.__log_info("schema or table does not exists yet")

        # need to be db owner for this to work
        try:
            self.__log_info(f"creating schema {self._schema_name} "
                            + f"if it does not yet exists")
            create_schema_text = f"CREATE SCHEMA IF NOT EXISTS \"{self._schema_name}\""

            await cn.execute(create_schema_text)

            self.__log_info(f"creating table {self._table_name}")
            create_command_text = (
                    f"CREATE TABLE \"{self._schema_name}\".\"{self._table_name}\" ("
                    + f" \"{self.__name_field}\" character varying (50) PRIMARY KEY, "
                    + f" \"{self.__owner_field}\" character varying (128) NOT NULL, "
                    + f" \"{self.__timeout_in_seconds_field}\" integer NOT NULL, "
                    + f" \"{self.__create_date_wo_tz_field}\" timestamp without time zone NOT NULL, "
                    + f" \"{self.__update_date_wo_tz_field}\" timestamp without time zone NOT NULL"
                    + ")"
            )
            await cn.execute(create_command_text)

        except Exception as e:
            self._log_exception(e)
            raise
