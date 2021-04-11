import hashlib
import base64
from datetime import timezone
import datetime
import math


def hash_string(string: str) -> str:
    hashstr = hashlib.sha3_224(string.encode()).digest()
    b64 = base64.urlsafe_b64encode(hashstr).decode()
    return b64[:len(b64) - 2]


def get_timestamp():
    dt = datetime.datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    return math.floor(utc_timestamp)


if __name__ == '__main__':
    s = get_timestamp()
    print(s)
