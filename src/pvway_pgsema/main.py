from injector import Injector

from pvway_pgsema.di.pvway_sema_di import configure_sema_config
from pvway_pgsema.module.sema_module import SemaModule
from pvway_pgsema.services.sema_service import SemaService


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def main():
    print('in main')
    injector = Injector([
        configure_sema_config(
            schema_name='schema',
            table_name= 'table'),
        SemaModule()
    ])
    print('configuration complete')
    sema_svc = injector.get(SemaService)
    print('sema_svc available')
    sema_svc.print_config()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    main()
