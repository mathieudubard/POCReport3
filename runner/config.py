import logging
import os

from pyhocon import ConfigFactory

log = logging.getLogger(__name__)

path = os.path.join(os.path.dirname(__file__), "..", "conf", "application.conf")
conf = ConfigFactory.parse_file(path)


def get_config_item(item_path: str):
    try:
        return conf.get(item_path)
    except Exception:
        return None


api_prefix = get_config_item("api.prefix") or "/model-report-generator"
sso_url = get_config_item("url.sso") or os.environ.get("GLOBAL_SSO_API_SERVICE_URL", "")
