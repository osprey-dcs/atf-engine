#!/usr/bin/env python3
#
# Workaround to correct .hdr files containing erronous unsigned *Direction values.
#
# see https://github.com/osprey-dcs/atf-engine/issues/1

import json
import struct

def getargs():
    from argparse import ArgumentParser
    P = ArgumentParser(usage='Correct ResponseDirection and ReferenceDirection with incorrect unsigned values')
    P.add_argument('input',
                   help='Input .hdr file')
    P.add_argument('output',
                   help='Output .hdr file')
    return P

def main(args):
    with open(args.input, 'r') as F:
        J = json.load(F)

    for sig in J['Signals']:
        for k in ('ResponseDirection', 'ReferenceDirection'):
            D = sig.get(k)
            if D is not None and D>10000: # valid values [-3, 3]
                D, = struct.unpack('>i', struct.pack('>I', D))
                sig[k] = D

    with open(args.output, 'w') as F:
        json.dump(J, F, indent='  ')

if __name__=="__main__":
    main(getargs().parse_args())
