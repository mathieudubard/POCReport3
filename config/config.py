import configparser
import os
import logging
from logging.config import fileConfig


CONFIG_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
LOGGING_CONFIGURATION_FILE = os.path.join(CONFIG_DIRECTORY, 'logging.ini')
ENV_CONFIGURATION_FILE = os.path.join(CONFIG_DIRECTORY, 'local.ini')
LOG_FILE = os.path.join(os.path.dirname(CONFIG_DIRECTORY), 'model' , 'log.log')  # Will be created/overwritten

# moodyscappy Cappy: pass ``sso_url`` in credentials (see model/run.py); must not rely on env alone on all hosts.
FALLBACK_QA_MOODYS_SSO_URL = "https://qa-api.sso.moodysanalytics.net/sso-api/"


def resolve_sso_url_for_cappy():
    """
    SSO base URL for Cappy JWT validation (``/auth/certs``). Matches config/local.ini when env is loaded.
    Order: ``MOODYS_SSO_URL``, ``GLOBAL_SSO_API_SERVICE_URL``, then QA fallback.
    """
    for key in ("MOODYS_SSO_URL", "GLOBAL_SSO_API_SERVICE_URL"):
        v = (os.environ.get(key) or "").strip()
        if v:
            return v
    return FALLBACK_QA_MOODYS_SSO_URL
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
