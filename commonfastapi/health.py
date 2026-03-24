import logging

from fastapi import APIRouter

import runner.config as cf

log = logging.getLogger(__name__)

prefix = cf.api_prefix
router = APIRouter()


@router.get(f"{prefix}/v1/ping", summary="Health check", description="Service uptime")
async def ping():
    return "OK"
