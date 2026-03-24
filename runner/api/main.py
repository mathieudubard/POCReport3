import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

import runner.config as cf
from config import config as model_config
from commonfastapi.health import router as health_router
from commonfastapi.security import JWTBearer
from commonfastapi.sso import SsoClient
from runner.api import process

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

jwks: dict = {}
sso_client = SsoClient(cf.sso_url)


@asynccontextmanager
async def lifespan(app: FastAPI):
    model_config.configureLogger()
    model_config.processConfigurations(None, None, True)
    if cf.sso_url:
        try:
            jwks.clear()
            jwks.update(sso_client.get_public_key())
            log.info("Loaded %d SSO JWK(s)", len(jwks))
        except Exception as e:
            log.warning("SSO cert fetch failed at startup: %s", e)
    else:
        log.warning("url.sso is empty — configure conf/application.conf or GLOBAL_SSO_API_SERVICE_URL")
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Hanmi Model Report Generator API",
    description="Interactive: JWT + live S3 inputs; optional returnReportsInResponse merges report JSON into the HTTP body.",
    version="1.0.0",
    openapi_tags=[{"name": "Model report generator"}],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(
    process.router,
    prefix=f"{cf.api_prefix}/v1",
    tags=["Model report generator"],
    dependencies=[Depends(JWTBearer(jwks))],
)


def main():
    uvicorn.run("runner.api.main:app", host="0.0.0.0", port=8080, reload=False, log_level="info")


if __name__ == "__main__":
    main()
