import datetime
from typing import Union


class Utils:
    @staticmethod
    def convert_pg_ts(ts_in_microseconds: int) -> datetime.datetime:
        ts = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
        return ts + datetime.timedelta(microseconds=ts_in_microseconds)

    @staticmethod
    def convert_bytes_to_int(in_bytes: bytes) -> int:
        return int.from_bytes(in_bytes, byteorder='big', signed=True)

    @staticmethod
    def convert_bytes_to_utf8(in_bytes: Union[bytes, bytearray]) -> str:
        return in_bytes.decode('utf-8')
