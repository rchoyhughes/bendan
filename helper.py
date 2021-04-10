import hashlib
import base64


def hash_string(string: str) -> str:
    hashstr = hashlib.sha3_224(string.encode()).digest()
    b64 = base64.urlsafe_b64encode(hashstr).decode()
    return b64[:len(b64) - 2]


if __name__ == '__main__':
    s = hash_string('usernamepassword')
    print(s)
