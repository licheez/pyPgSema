from injector import Binder, singleton

from pvway_pgsema.services.sema_config import SemaConfig


def configure_sema_config(
        schema_name: str, table_name: str):
    def configure(binder: Binder) -> None:
        config = SemaConfig(schema_name, table_name)
        binder.bind(SemaConfig, to=config, scope=singleton)

    print('in configure_sema_config')
    return configure
