import hashlib
import requests
import os

def shorturl(url):
    chars = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    _hex = 0x7FFFFFF & int(str(hashlib.md5(url.encode()).hexdigest()), 16)
    code = ""
    for i in range(9):
        index = 0x0000003D & _hex
        code += chars[index]
        _hex = _hex >> 3
    return code


def get_proxy():
    # PROXY_POOL_HOST = os.environ.get("PROXY_POOL_HOST", "172.18.0.5")
    PROXY_POOL_HOST = "http://172.18.0.5"
    return requests.get(PROXY_POOL_HOST+":5010/get/?type=https").json()

def delete_proxy(proxy):
    # PROXY_POOL_HOST = os.environ.get("PROXY_POOL_HOST", "172.18.0.5")
    PROXY_POOL_HOST = "http://172.18.0.5"
    requests.get(PROXY_POOL_HOST+":5010/delete/?proxy=%s",proxy)
