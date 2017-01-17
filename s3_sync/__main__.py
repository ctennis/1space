import argparse
import json
import logging
import logging.handlers
import os
import time
import traceback

from container_crawler import ContainerCrawler


def setup_logger(console=False, log_file=None, level='INFO'):
    logger = logging.getLogger('s3-sync')
    logger.setLevel(level)
    formatter = logging.Formatter(
        '[%(asctime)s] %(name)s [%(levelname)s]: %(message)s')
    if console:
        handler = logging.StreamHandler()
    elif log_file:
        handler = logging.handlers.RotatingFileHandler(log_file,
                                                       maxBytes=1024*1024*100,
                                                       backupCount=5)
    else:
        raise RuntimeError('log file must be set')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def load_swift(once=False):
    logger = logging.getLogger('s3-sync')

    while True:
        try:
            import swift  # NOQA
            break
        except ImportError as e:
            if once:
                raise e
            else:
                logger.warning('Failed to load swift: %s' % str(e))
                time.sleep(5)


def load_config(conf_file):
    with open(conf_file, 'r') as f:
        return json.load(f)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Swift-S3 synchronization daemon')
    parser.add_argument('--config', metavar='conf', type=str, required=True,
                        help='path to the configuration file')
    parser.add_argument('--once', action='store_true',
                        help='run once')
    parser.add_argument('--log-level', metavar='level', type=str,
                        default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='logging level; defaults to info')
    parser.add_argument('--console', action='store_true',
                        help='log messages to console')
    return parser.parse_args()


def main():
    args = parse_args()
    if not os.path.exists(args.config):
        print 'Configuration file does not exist'
        exit(0)

    conf = load_config(args.config)
    setup_logger(console=args.console, level=args.log_level.upper(),
                 log_file=conf.get('log_file'))

    # Swift may not be loaded when we start. Spin, waiting for it to load
    load_swift(args.once)
    from .s3_sync import SyncContainer
    logger = logging.getLogger('s3-sync')
    logger.debug('Starting S3Sync')
    try:
        crawler = ContainerCrawler(conf, SyncContainer, logger)
        if args.once:
            crawler.run_once()
        else:
            crawler.run_always()
    except Exception as e:
        logger.error("S3Sync failed: %s" % repr(e))
        logger.error(traceback.format_exc(e))
        exit(1)

if __name__ == '__main__':
    main()
