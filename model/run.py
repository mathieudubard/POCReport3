import argparse
import logging
import os
import sys
# Adding package directory necessary for some imports to work in local mode
MODEL_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
PACKAGE_DIRECTORY = os.path.dirname(MODEL_DIRECTORY)
sys.path.append(PACKAGE_DIRECTORY)
from config import config
from model import Model


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
    logger = logging.getLogger(__name__)
    model_run_parameters_path = args.s3 if args.s3 else args.local
    local_mode = bool(args.local)
    credentials = {'jwt': args.jwt, 'username': args.unpw[0], 'password': args.unpw[1]}
    if not args.proxyjwt and args.proxyunpw == [None, None]:
        proxy_credentials = {}
    else:
        proxy_credentials = {'jwt': args.proxyjwt, 'username': args.proxyunpw[0], 'password': args.proxyunpw[1], 'sso_url': os.environ.get('PROXY_TOKEN_URL')}
    try:
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
    try:
        model.cleanUp(log_file=config.LOG_FILE, keep_temp=args.keeptemp)
    except UnboundLocalError:
        pass  # An authentication error will prevent instantiation of Model object, and UnboundLocalError unnecessarily clutters call stack
    logger.info(f'Exit code: {exit_code}')
    logging.shutdown()
    return exit_code

def main():
    args = _parseInputArguments()
    config.configureLogger(args.loglevel)
    config.processConfigurations(args.overwrite, args.config, args.usedefaults)
    exit_code = _runModel(args)
    sys.exit(exit_code)

if __name__ == '__main__':
    main()
