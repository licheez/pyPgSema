from datetime import datetime, timedelta
from typing import Optional

from pvway_sema_abs.semaphore_info import SemaphoreInfo
from pvway_sema_abs.semaphore_status_enu import SemaphoreStatusEnu


class DbSemaphore(SemaphoreInfo):
    def __init__(self,
                 status: SemaphoreStatusEnu,
                 name: str,
                 owner: Optional[str],
                 timeout: timedelta,
                 create_date_utc: datetime,
                 update_date_utc: datetime):
        self.Status: SemaphoreStatusEnu = status
        self.Name: str = name
        self.Owner: str = owner if owner else "unknown owner"
        self.Timeout: timedelta = timeout
        self.CreateDateUtc: datetime = create_date_utc
        self.UpdateDateUtc: datetime = update_date_utc

    @property
    def expires_at_utc(self) -> datetime:
        return self.UpdateDateUtc + self.Timeout

    def __str__(self):
        create_dt = self.CreateDateUtc.isoformat()
        update_dt = self.UpdateDateUtc.isoformat()
        expire_dt = self.expires_at_utc.isoformat()
        return f"semaphore '{self.Name}' owned by '{self.Owner}' " + \
            f"created at {create_dt} UTC, " + \
            f"with a timeout of {self.Timeout} " + \
            f"has been updated the last time at {update_dt} UTC " + \
            f"and is due to expire at {expire_dt} UTC"
