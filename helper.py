import hashlib
import codecs


def hash_string(string: str):
    hexstr = hashlib.sha3_224(string.encode()).hexdigest()
    b64 = codecs.encode(codecs.decode(hexstr, 'hex'), 'base64').decode()
    return b64


if __name__ == '__main__':
    s = hash_string('usernamepassword')
    print(s)
