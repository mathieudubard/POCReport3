import configparser
import os
import logging
from logging.config import fileConfig


CONFIG_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
LOGGING_CONFIGURATION_FILE = os.path.join(CONFIG_DIRECTORY, 'logging.ini')
ENV_CONFIGURATION_FILE = os.path.join(CONFIG_DIRECTORY, 'local.ini')
LOG_FILE = os.path.join(os.path.dirname(CONFIG_DIRECTORY), 'model' , 'log.log')  # Will be created/overwritten

# moodyscappy Cappy: pass URLs in credentials (see model/run.py); env alone is unreliable on some hosts (e.g. Domino).
# Cappy is proprietary — names inferred from repo local.ini and runtime errors (None/auth/certs, None/resources).
FALLBACK_QA_MOODYS_SSO_URL = "https://qa-api.sso.moodysanalytics.net/sso-api/"
# Same QA Rafa infra base as config/local.ini [DEFAULT] / model-conf-qa.ini (not the S3 bucket name).
FALLBACK_QA_MOODYS_TENANT_URL = "https://qa-api.rafa.moodysanalytics.net/infra/1.0/"

_log = logging.getLogger(__name__)


def _cappy_echo_info(fmt, *args):
    """Log + print so bare runtimes see Cappy lines without tuning logging.ini (config must not import ``model``)."""
    text = fmt % args if args else fmt
    _log.info(text)
    print(text, flush=True)


def _cappy_echo_error(fmt, *args):
    text = fmt % args if args else fmt
    _log.error(text)
    print(text, flush=True)


def resolve_sso_url_for_cappy():
    """
    SSO base URL for Cappy JWT validation (``/auth/certs``). Matches config/local.ini when env is loaded.
    Order: ``MOODYS_SSO_URL``, ``GLOBAL_SSO_API_SERVICE_URL``, then QA fallback.
    """
    for key in ("MOODYS_SSO_URL", "GLOBAL_SSO_API_SERVICE_URL"):
        v = (os.environ.get(key) or "").strip()
        if v:
            _cappy_echo_info("[Cappy] sso_url resolved from env %s=%r", key, v)
            return v
    _cappy_echo_info("[Cappy] sso_url using QA fallback (no MOODYS_SSO_URL / GLOBAL_SSO_API_SERVICE_URL)")
    return FALLBACK_QA_MOODYS_SSO_URL


def resolve_tenant_url_for_cappy(jwt=None):
    """
    Tenant / infra API base for Cappy (Rafa ``.../infra/...``; used for tenant lookup inside moodyscappy).

    Waterfall (mirrors :func:`resolve_sso_url_for_cappy` style):

    1. ``MOODYS_TENANT_URL``, then ``GLOBAL_TENANT_INFRA_URL`` if platforms inject the latter only.
    2. Else JWT: claims / ``iss`` host map — see ``model/jwt_tenant.py``.
    3. Else QA backstop: ``FALLBACK_QA_MOODYS_TENANT_URL`` (same as ``config/local.ini`` QA infra base).

    The backstop is **not** the S3 bucket; it is the Rafa infra API root. Set ``MOODYS_TENANT_URL`` for non-QA.
    """
    from model.jwt_tenant import (
        decode_jwt_payload_unverified,
        normalize_infra_base_url,
        tenant_infra_url_from_claims_with_source,
    )

    for key in ("MOODYS_TENANT_URL", "GLOBAL_TENANT_INFRA_URL"):
        v = (os.environ.get(key) or "").strip()
        if v:
            out = normalize_infra_base_url(v)
            _cappy_echo_info("[Cappy] tenant_url resolved from env %s=%r", key, out)
            return out

    jwt_str = (jwt or "").strip()
    if jwt_str:
        claims = decode_jwt_payload_unverified(jwt_str)
        url, source = tenant_infra_url_from_claims_with_source(claims)
        if url:
            out = normalize_infra_base_url(url)
            _cappy_echo_info("[Cappy] tenant_url resolved from JWT: %s -> %r", source, out)
            return out
        out = normalize_infra_base_url(FALLBACK_QA_MOODYS_TENANT_URL)
        _cappy_echo_info(
            "[Cappy] tenant_url using QA fallback (JWT present but no claim/iss mapping; source=%r) -> %r. "
            "Set MOODYS_TENANT_URL or extend model/jwt_tenant.py issuer map.",
            source,
            out,
        )
        return out

    out = normalize_infra_base_url(FALLBACK_QA_MOODYS_TENANT_URL)
    _cappy_echo_info(
        "[Cappy] tenant_url using QA fallback (no MOODYS_TENANT_URL / GLOBAL_TENANT_INFRA_URL and no JWT) -> %r",
        out,
    )
    return out


DO_NOT_LOG_MODULES = ['matplotlib', 's3transfer.utils', 's3transfer.futures', 's3transfer.tasks']  # Put noisy module names here if they are unneccessarily cluttering the logs


def _getConfigParser(config_file):
    config = configparser.ConfigParser()
    config.optionxform = lambda key: key  # Preserve case for keys (default is str.lower())
    config.read(config_file)
    return config

def _loadSection(config_parser, section, overwrite=True):
    for k, v in config_parser[section].items():
        if k not in os.environ or overwrite:
            os.environ[k] = v

def _loadAll(config_file, overwrite=True):
    """Load all sections in a given config file, optionally overwriting existing env variables"""
    config = _getConfigParser(config_file)
    for section in config:
        _loadSection(config, section, overwrite)

def configureLogger(log_level=None, config_file=LOGGING_CONFIGURATION_FILE, log_file=LOG_FILE):
    [logging.getLogger(logger).addFilter(lambda rec: False) for logger in DO_NOT_LOG_MODULES]
    log_file = os.path.abspath(log_file).replace('\\', '/')  # logging.config.fileConfig is particular about escape chars (unavoidable on Windows)
    logging.config.fileConfig(config_file, defaults={'logfilename': log_file})
    logging.captureWarnings(True)
    root_logger = logging.getLogger()
    if log_level:
        if log_level == 'DISABLED':
            logging.captureWarnings(False)
            logging.disable()
        else:
            root_logger.setLevel(log_level)
            for handler in root_logger.handlers:
                handler.setLevel(log_level)

def processConfigurations(optional_config=None, optional_additions=None, overwrite_existing=None):
    _loadAll(ENV_CONFIGURATION_FILE, overwrite=overwrite_existing)
    if optional_config is not None:
        _loadAll(optional_config, overwrite=True)
    if optional_additions is not None:
        _loadAll(optional_additions, overwrite=False)
