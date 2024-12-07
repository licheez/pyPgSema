from datetime import datetime


class DaoHelper:
    @staticmethod
    def truncate_then_escape(value: str, max_len: int) -> str:
        """
        :param value: The string value to be potentially truncated and escaped.
        :param max_len: The maximum allowed length for the string. If the string exceeds this length, it will be truncated.
        :return: A string that is truncated to the specified maximum length and escaped.
        """
        if not value:
            return value
        val = value if len(value) <= max_len else value[:max_len - 3] + "..."
        return DaoHelper.escape(val)

    @staticmethod
    def escape(value: str) -> str:
        """
        :param value: The input string that may contain single quotes which need to be escaped.
        :return: A new string with all single quotes escaped by doubling them. This is often used to prevent SQL injection attacks by ensuring that single quotes within the input string are correctly escaped before being used in SQL statements.
        """
        return value.replace("'", "''")

    @staticmethod
    def get_timestamp(utc_now: datetime) -> str:
        """
        :param utc_now: The current UTC datetime that needs to be formatted.
        :return: A string representation of the given datetime
        in the format 'YYYY-MM-DD HH:MM:SS.sss'.
        """
        return f"'{utc_now:%Y-%m-%d %H:%M:%S.%f}'"[:-3]
