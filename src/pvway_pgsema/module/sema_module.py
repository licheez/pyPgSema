from injector import singleton, provider, Module

from pvway_pgsema.services.sema_config import SemaConfig
from pvway_pgsema.services.sema_service import SemaService


class SemaModule(Module):
    @provider
    @singleton
    def provide_sema_service(self, config: SemaConfig) -> SemaService:
        print('in provide_sema_service')
        return SemaService(config)
