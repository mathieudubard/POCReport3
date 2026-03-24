import base64
import json
import struct
import urllib.parse
import logging
import requests
import six
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers

log = logging.getLogger(__name__)


class SsoClient:
    def __init__(self, sso_url: str):
        self.sso_url = (sso_url or "").rstrip("/")

    def get_public_key(self):
        url = urllib.parse.urljoin(f"{self.sso_url}/", "auth/certs")
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        jwks = json.loads(r.content.decode("utf-8"))
        return {jwk["kid"]: _get_authentication_public_key(jwk) for jwk in jwks["keys"]}


def _get_authentication_public_key(authentication_public_license):
    modulus = _base64_to_long(authentication_public_license["n"])
    exponent = _base64_to_long(authentication_public_license["e"])
    numbers = RSAPublicNumbers(exponent, modulus)
    public_key = numbers.public_key(backend=default_backend())
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _base64_to_long(data):
    if isinstance(data, six.text_type):
        data = data.encode("ascii")
    _d = base64.urlsafe_b64decode(bytes(data) + b"==")
    return intarr2long(struct.unpack("%sB" % len(_d), _d))


def intarr2long(arr):
    return int("".join(["%02x" % byte for byte in arr]), 16)
