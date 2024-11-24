import hashlib


def get_md5_hash(bytes: bytes):
    md5_hash = hashlib.md5()
    md5_hash.update(bytes)
    return md5_hash.hexdigest()
