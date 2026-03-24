import logging
from typing import Optional, Tuple

import jwt
from fastapi.exceptions import HTTPException
from fastapi.security import HTTPBearer
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED

log = logging.getLogger(__name__)


class JWTBearer(HTTPBearer):
    options = {
        "verify_signature": True,
        "verify_exp": True,
        "verify_nbf": True,
        "verify_iat": True,
        "verify_aud": False,
        "require_exp": True,
        "require_iat": True,
        "require_nbf": False,
    }

    def __init__(self, jwks: dict, auto_error: bool = True):
        super().__init__(auto_error=auto_error)
        self.jwks = jwks

    async def __call__(self, request: Request) -> Optional[str]:
        if request.method == "OPTIONS":
            return None

        authorization = request.headers.get("Authorization")
        scheme, param = _get_authorization_scheme_param(authorization)
        if not authorization or scheme.lower() != "bearer":
            if self.auto_error:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="Bearer token is not provided",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            return None

        return self._decode(param)

    def _decode(self, token):
        e = None
        for _k, jwk in self.jwks.items():
            try:
                return jwt.decode(token, key=jwk, options=self.options, algorithms="RS256")
            except jwt.exceptions.InvalidSignatureError as err:
                log.info("JWK cannot be used for token: %s", err)
                e = err
            except Exception as err:
                log.info('Error "%s" when parsing token', err)
                e = err
        if e:
            raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail=f"Invalid authorization token: {e}")
        return None


def _get_authorization_scheme_param(authorization_header_value: str) -> Tuple[str, str]:
    if not authorization_header_value:
        return "", ""
    scheme, _, param = authorization_header_value.partition(" ")
    return scheme, param
