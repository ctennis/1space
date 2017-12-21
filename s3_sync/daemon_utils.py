"""
Copyright 2017 SwiftStack

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import json
import logging
import logging.handlers
import os
import sys
import time


MAX_LOG_SIZE = 100 * 1024 * 1024


def setup_logger(console=False, log_file=None, level='INFO'):
    logger = logging.getLogger('s3-sync')
    logger.setLevel(level)
    formatter = logging.Formatter(
        '[%(asctime)s] %(name)s [%(levelname)s]: %(message)s')
    if console:
        handler = logging.StreamHandler()
    elif log_file:
        handler = logging.handlers.RotatingFileHandler(log_file,
                                                       maxBytes=MAX_LOG_SIZE,
                                                       backupCount=5)
    else:
        raise RuntimeError('log file must be set')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger = logging.getLogger('boto3')
    logger.setLevel(level)
    logger.addHandler(handler)
    logger = logging.getLogger('botocore')
    logger.setLevel(level)
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


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Swift-S3 synchronization daemon')
    parser.add_argument('--config', metavar='conf', type=str, required=True,
                        help='path to the configuration file')
    parser.add_argument('--once', action='store_true',
                        help='run once')
    parser.add_argument('--log-level', metavar='level', type=str,
                        choices=['debug', 'info', 'warning', 'error'],
                        help='logging level; defaults to info')
    parser.add_argument('--console', action='store_true',
                        help='log messages to console')
    return parser.parse_args(args)


def setup_context():
    args = parse_args(sys.argv[1:])
    if not os.path.exists(args.config):
        print 'Configuration file does not exist'
        exit(0)

    conf = load_config(args.config)
    if not args.log_level:
        args.log_level = conf.get('log_level', 'info')
    setup_logger(console=args.console, level=args.log_level.upper(),
                 log_file=conf.get('log_file'))

    # Swift may not be loaded when we start. Spin, waiting for it to load
    load_swift(args.once)
    return args, conf
