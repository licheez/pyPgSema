from typing import Callable, Coroutine, Any, Optional

class SemaConfig:
    def __init__(
            self,
            schema_name: str,
            table_name: str,
            get_cs_async: Callable[[], Coroutine[Any, Any, str]],
            log_exception: Callable[[Exception], None],
            log_info: Optional[Callable[[str], None]] = None
    ):
        print('in SemaConfig.init')
        self.schema_name = schema_name
        self.table_name = table_name
        self.get_cs_async = get_cs_async
        self.log_exception = log_exception
        self.log_info = log_info
