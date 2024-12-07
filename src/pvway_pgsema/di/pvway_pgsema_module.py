from injector import singleton, provider, Module

from pvway_pgsema.di.pvway_pgsema_config import PvWayPgSemaConfig
from pvway_pgsema.services.sema_service import SemaService


class PvwayPgSemaModule(Module):
    @provider
    @singleton
    def provide_sema_service(self, config: PvWayPgSemaConfig) -> SemaService:
        print('in provide_sema_service')
        return SemaService(config)
