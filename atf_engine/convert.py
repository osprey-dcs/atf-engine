import json
import logging
import subprocess as SP
from glob import glob, escape
from pathlib import Path

_log = logging.getLogger(__name__)

def getargs():
    from argparse import ArgumentParser
    P = ArgumentParser()
    P.add_argument('-v', '--verbose', dest='level', default=logging.INFO,
                   action='store_const', const=logging.DEBUG,
                   help='Enable extra application logging')
    P.add_argument('input', type=Path,
                   help='Input JSON header file')
    P.add_argument('output', type=Path,
                   help='Output JSON header file.  Data files placed relative.')
    return P

def main(args):
    _log.debug('Read %s', args.input)
    with args.input.open('r') as F:
        info = json.load(F)

    _log.debug('Discover .dat')

if __name__=='__main__':
    args = getargs().parse_args()
    logging.basicConfig(level=args.level)
    main(args)
