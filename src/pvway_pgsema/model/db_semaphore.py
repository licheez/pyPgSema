from datetime import datetime, timedelta, timezone
from typing import Optional

from pvway_sema_abs.semaphore_info import SemaphoreInfo
from pvway_sema_abs.semaphore_status_enu import SemaphoreStatusEnu


class DbSemaphore(SemaphoreInfo):
    @property
    def status(self) -> SemaphoreStatusEnu:
        return self._status

    @property
    def name(self) -> str:
        return self._name

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def timeout(self) -> timedelta:
        return self._timeout

    @property
    def expires_at_utc(self) -> datetime:
        return self._update_date_utc + self._timeout

    @property
    def create_date_utc(self) -> datetime:
        return self._create_date_utc

    @property
    def update_date_utc(self) -> datetime:
        return self._update_date_utc

    def __init__(self,
                 status: SemaphoreStatusEnu,
                 name: str,
                 owner: Optional[str],
                 timeout: timedelta,
                 create_date_wo_tz: datetime,
                 update_date_wo_tz: datetime):
        self._status: SemaphoreStatusEnu = status
        self._name: str = name
        self._owner: str = owner if owner else "unknown owner"
        self._timeout: timedelta = timeout
        self._create_date_utc: datetime = (
            create_date_wo_tz.replace(tzinfo=timezone.utc))
        self._update_date_utc: datetime = (
            update_date_wo_tz.replace(tzinfo=timezone.utc))

    @classmethod
    def from_semaphore_info(
            cls,
            status: SemaphoreStatusEnu,
            si: SemaphoreInfo):
        create_date_wo_tz = si.create_date_utc.replace(tzinfo=None)
        update_date_wo_tz = si.update_date_utc.replace(tzinfo=None)
        return cls(
            status,
            si.name, si.owner, si.timeout,
            create_date_wo_tz, update_date_wo_tz
        )

    def __str__(self):
        create_dt = self.create_date_utc.isoformat()
        update_dt = self.update_date_utc.isoformat()
        expire_dt = self.expires_at_utc.isoformat()
        return f"semaphore '{self.name}' is owned by '{self.owner}'. \n" + \
            f"It was created at {create_dt} UTC, \n" + \
            f"with a timeout of {self.timeout}. \n" + \
            f"It was updated the last time at {update_dt} UTC \n" + \
            f"and is due to expire at {expire_dt} UTC."
