import logging
import os
import socket
import string
import sys
from argparse import ArgumentParser
from functools import partial
from itertools import imap
from multiprocessing import Pool
from urlparse import urlparse

import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
DUPLICATE = 'DUPLICATE'


def get_file_name(url):
    result = urlparse(url)
    return '_'.join(result.path.split('/'))[1:]


def download(url, timeout):
    fn = get_file_name(url)
    if os.path.exists(fn):
        return url, DUPLICATE, None

    try:
        with open(fn) as im_file:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            im_file.write(r.content)
        return url, SUCCESS, None
    except Exception as e:
        logger.exception('Failed to download %s', url)
        if os.path.exists(fn):
            os.remove(fn)
        return url, FAILURE, e.message


def main():
    parser = ArgumentParser()
    parser.add_argument('--concurrency', '-c', type=int, default=100)
    parser.add_argument('--timeout', '-t', type=int, default=socket.getdefaulttimeout())
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    p = Pool(args.concurrency)
    func = partial(download, timeout=args.timeout)
    urls = imap(string.strip, sys.stdin)
    for url, status, e in tqdm(p.imap_unordered(func, urls)):
        if args.verbose:
            print status + ':', url
            if e:
                print e


if __name__ == '__main__':
    main()
