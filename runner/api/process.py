import argparse
import json
import logging
import os
import shutil
import tempfile
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from model.run import run_model_batch

log = logging.getLogger(__name__)

router = APIRouter()


class HanmiRunRequest(BaseModel):
    """Exactly one of mrpS3Key (S3 key to modelRunParameter.json) or modelRunParameter (inline JSON)."""

    settingsPatch: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Merged into modelRunParameter.settings when using inline modelRunParameter.",
    )
    mrpS3Key: Optional[str] = Field(default=None, description="S3 object key for modelRunParameter.json")
    modelRunParameter: Optional[Dict[str, Any]] = Field(default=None, description="Full model run parameters object")

    @model_validator(mode="after")
    def exactly_one_source(self):
        key_ok = self.mrpS3Key is not None and str(self.mrpS3Key).strip() != ""
        inline_ok = self.modelRunParameter is not None
        if key_ok == inline_ok:
            raise ValueError("Provide exactly one of mrpS3Key or modelRunParameter")
        return self


def _prepare_args(
    jwt_token: str,
    proxy: Optional[str],
    *,
    local_path: Optional[str],
    s3_key: Optional[str],
) -> argparse.Namespace:
    ns = argparse.Namespace()
    ns.jwt = jwt_token
    ns.unpw = [None, None]
    ns.local = local_path
    ns.s3 = s3_key
    ns.proxyjwt = None
    ns.proxyunpw = [None, None]
    ns.keeptemp = False
    return ns


@router.post("/execute", summary="Run model report generator", response_model=None)
async def execute(
    request: Request,
    body: HanmiRunRequest,
    authorization: str = Header(..., description="Bearer JWT for Cappy / S3"),
    x_forwarded_access_token: Optional[str] = Header(None, alias="X-Forwarded-Access-Token"),
):
    auth = (x_forwarded_access_token or authorization or "").strip()
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authorization must be Bearer JWT")
    jwt_token = auth.split(None, 1)[1].strip()

    proxy = request.headers.get("Proxy-Authorization") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")

    tmp_dir = None
    try:
        if body.modelRunParameter is not None:
            mrp = dict(body.modelRunParameter)
            settings = dict(mrp.get("settings") or {})
            if body.settingsPatch:
                settings.update(body.settingsPatch)
            mrp["settings"] = settings
            tmp_dir = tempfile.mkdtemp(prefix="hanmi_mrp_")
            mrp_path = os.path.join(tmp_dir, "modelRunParameter.json")
            with open(mrp_path, "w", encoding="utf-8") as f:
                json.dump(mrp, f)
            args = _prepare_args(jwt_token, proxy, local_path=tmp_dir, s3_key=None)
        else:
            key = str(body.mrpS3Key).strip()
            args = _prepare_args(jwt_token, proxy, local_path=None, s3_key=key)

        exit_code, model = run_model_batch(args, return_model=True)
        if exit_code != 0:
            log.error("Model run failed with exit_code=%s", exit_code)
            raise HTTPException(status_code=500, detail="Model run failed")

        payload = getattr(model, "report_response_payload", None) or {"reports": {}}
        return {"exitCode": 0, **payload}
    finally:
        if tmp_dir and os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
