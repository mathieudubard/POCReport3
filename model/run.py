import argparse
import logging
import os
import sys
# Adding package directory necessary for some imports to work in local mode
MODEL_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
PACKAGE_DIRECTORY = os.path.dirname(MODEL_DIRECTORY)
sys.path.append(PACKAGE_DIRECTORY)
from config import config
from model.cappy_log import cappy_echo_info, cappy_echo_warning


def _cappy_auth_mode(creds):
    if not creds:
        return "none"
    if creds.get("jwt"):
        return "jwt"
    if creds.get("username") is not None:
        return "username_password"
    return "unknown"


def run_model_batch(args, return_model=False):
    """
    Execute the model in ManagedBatch style: JWT or user/pass, MRP from S3 (-s) or local (-L).
    This is the single batch entry point used by the CLI; a future Interactive/API layer can
    call it after building args (or a dedicated API session) — see docs/HANMI_BATCH_AND_INTERACTIVE.md.

    If return_model is True, returns (exit_code, model) so callers can read model.report_response_payload
    (when settings.returnReportsInResponse is set). Otherwise returns exit_code only.
    """
    logger = logging.getLogger(__name__)
    model_run_parameters_path = args.s3 if args.s3 else args.local
    local_mode = bool(args.local)
    credentials = {
        'jwt': args.jwt,
        'username': args.unpw[0],
        'password': args.unpw[1],
        # Cappy reads these from kwargs; env alone is not always applied (e.g. some Domino runtimes).
        'sso_url': config.resolve_sso_url_for_cappy(),
        'tenant_url': config.resolve_tenant_url_for_cappy(jwt=args.jwt),
    }
    if not args.proxyjwt and args.proxyunpw == [None, None]:
        proxy_credentials = {}
    else:
        proxy_credentials = {
            'jwt': args.proxyjwt,
            'username': args.proxyunpw[0],
            'password': args.proxyunpw[1],
            'sso_url': os.environ.get('PROXY_TOKEN_URL'),
            'tenant_url': config.resolve_tenant_url_for_cappy(jwt=args.proxyjwt),
        }
        if not (proxy_credentials.get("sso_url") or "").strip():
            cappy_echo_warning(
                logger,
                "[Cappy] proxy sso_url is unset (PROXY_TOKEN_URL empty); Cappy may fail proxy auth",
            )

    cappy_echo_info(
        logger,
        "[Cappy] credentials built for batch: main auth=%s sso_url=%r tenant_url=%r",
        _cappy_auth_mode(credentials),
        credentials.get("sso_url"),
        credentials.get("tenant_url"),
    )
    if proxy_credentials:
        cappy_echo_info(
            logger,
            "[Cappy] credentials built for batch: proxy auth=%s sso_url=%r tenant_url=%r",
            _cappy_auth_mode(proxy_credentials),
            proxy_credentials.get("sso_url"),
            proxy_credentials.get("tenant_url"),
        )

    model = None
    try:
        from model import Model

        print("[Model run] mode={}, path={}".format("local" if local_mode else "S3", model_run_parameters_path))
        logger.info('Running Model')
        model = Model(credentials, proxy_credentials, model_run_parameters_path, local_mode)
        model.run()
        print("[Model run] Completed successfully")
        logger.info('Model execution completed')
        exit_code = 0
    except Exception as e:
        print("[Model run] FAILED: {}".format(e))
        logger.error(f'Model failed with exception: {sys.exc_info()}', exc_info=True)
        logger.debug(e)
        exit_code = 1
    if model is not None:
        model.cleanUp(log_file=config.LOG_FILE, keep_temp=args.keeptemp)
    logger.info(f'Exit code: {exit_code}')
    logging.shutdown()
    if return_model:
        return exit_code, model
    return exit_code


def _parseInputArguments():
    parser = argparse.ArgumentParser(description='Submit test cases to the CMM API')
    parser.add_argument('-d', '--usedefaults', help='Do not overwrite system env variables with included configuration files', action='store_false')
    parser.add_argument('-l', '--loglevel', help='Set log level for console and logfile output', choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'DISABLED'])
    parser.add_argument('-k', '--keeptemp', help='Do not clear temp directories and files after model run', action='store_true')
    cfgs = parser.add_mutually_exclusive_group()
    cfgs.add_argument('-o', '--overwrite', help='Overwrite configurations with custom configuration file', metavar=('CUSTOM_CONFIG_PATH'))
    cfgs.add_argument('-c', '--config', help='Add custom configurations without overwriting system variables', metavar=('CUSTOM_CONFIG_PATH'))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('-s', '--s3', help='Run model with data hosted on S3 (default behavior)', metavar=('MODEL_PARAMS_S3_KEY'))
    mode.add_argument('-L', '--local', help='Run model with data from local test folder', metavar=('TEST_FOLDER_PATH'))
    credentials = parser.add_mutually_exclusive_group(required=True)
    credentials.add_argument('-j', '--jwt', help='Log in using JSON web token', metavar=('JWT'))
    credentials.add_argument('-u', '--unpw', help='Log in using username and password', nargs=2, metavar=('USERNAME', 'PASSWORD'), default=[None, None])
    proxy = parser.add_mutually_exclusive_group()
    proxy.add_argument('-t', '--proxyjwt', help='Use proxy user JWT for API access', metavar=('JWT'))
    proxy.add_argument('-p', '--proxyunpw', help='Use proxy username and password for API access', nargs=2, metavar=('USERNAME', 'PASSWORD'), default=[None, None])
    return parser.parse_args()


def _runModel(args):
    """Backward-compatible name for batch execution; prefer :func:`run_model_batch`."""
    return run_model_batch(args)


def main():
    args = _parseInputArguments()
    config.configureLogger(args.loglevel)
    config.processConfigurations(args.overwrite, args.config, args.usedefaults)
    exit_code = run_model_batch(args)
    sys.exit(exit_code)

if __name__ == '__main__':
    main()
