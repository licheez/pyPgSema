import asyncio
import datetime
import logging

from injector import Injector

from pvway_pgsema.di.pvway_pgsema_di import PvWayPgSemaConfigurer
from pvway_pgsema.services.sema_service import SemaService


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


async def __get_cs_async():
    return 'postgresql://postgres:postgres@localhost:5432/postgres'


def __log_exception(e: Exception) -> None:
    logging.error(e)


def __log_info(info: str) -> None:
    logging.info(info)


def main():
    print('in main')

    # Configure the logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s')

    injector = Injector()

    configurer = PvWayPgSemaConfigurer(
        schema_name='pySemaLab',
        table_name='semaphore',
        get_cs_async=__get_cs_async,
        log_exception=__log_exception,
        log_info=__log_info)
    configurer.install(injector.binder)

    print('configuration complete')
    svc = injector.get(SemaService)
    print('sema_svc available')
    svc.print_config()

    sema_name = 'name'

    si = asyncio.run(svc.acquire_semaphore_async(
        sema_name, 'owner',
        datetime.timedelta(minutes=5)))
    __log_info(f"{si}")

    asyncio.run(svc.touch_semaphore_async(sema_name))

    fSi = asyncio.run(svc.get_semaphore_async(sema_name))
    __log_info(f"{fSi}")

    asyncio.run(svc.release_semaphore_async(sema_name))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    main()
