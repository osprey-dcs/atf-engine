import asyncio
import json
import logging
import time
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor

from ._convert import convert2j

_log = logging.getLogger(__name__)

def getargs():
    from argparse import ArgumentParser

    P = ArgumentParser()
    P.add_argument('-v', '--verbose', dest='level', default=logging.INFO,
                   action='store_const', const=logging.DEBUG,
                   help='Enable extra application logging')
    P.add_argument('-d', '--debug', action='store_true',
                   help='Enable extra asyncio logging')
    P.add_argument('--fileConverter', dest='ignored',
                   help='Location of FileReformatter2 executable (no longer used)')
    P.add_argument('input', type=Path,
                   help='Input JSON header file')
    P.add_argument('output', type=Path,
                   help='Output JSON header file.  Data files placed relative.')
    return P

async def main(args):
    loop = asyncio.get_running_loop()

    _log.debug('Read %s', args.input)
    with args.input.open('r') as F:
        info = json.load(F)

    # build index of (chas, chan) -> offset in Signals list
    idxCH = {}
    for i,sig in enumerate(info['Signals']):
        idxCH[(sig['Address']['Chassis'], sig['Address']['Channel'])] = i

    outdir = args.output.parent
    _log.debug('Output to %s', outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # place temp dir on the output file system so that moving files is cheap
    with TemporaryDirectory(dir=outdir) as scratch:
        scratch = Path(scratch)

        jfiles:{(int,int):Path} = {}

        with ThreadPoolExecutor(max_workers=len(info['Chassis'])) as pool:
            async with asyncio.TaskGroup() as sched:
                jobs = []
                for chas in info['Chassis']:
                    async def process_chas(chas):
                        _log.debug('Process chassis %r', chas)
                        n = chas['Chassis']
                        dat:list = chas['Dat']
                        dat = [str(args.input.parent / d) for d in dat]

                        chas_scratch = scratch / f'CH{n:02d}'
                        chas_scratch.mkdir()

                        T0 = time.monotonic()
                        chas['Errors'] = errs = await loop.run_in_executor(pool, convert2j, dat, chas_scratch)
                        Td = time.monotonic() - T0
                        for err in errs:
                            print(f'Error: Chas {n} : {err}')

                        for c in range(32):
                            chanj = chas_scratch / f'CH{c:02d}.j' # channel zero indexed
                            if chanj.exists(): # missing j files below
                                jfiles[(n, c+1)] = chanj # chas and chan now one indexed

                        _log.debug('Complete chassis %r in %f sec', chas, Td)
                        return len(errs)

                    jobs.append(sched.create_task(process_chas(chas)))
        # all jobs complete, all .j files created under scratch
            total_errors = sum([j.result() for j in jobs])

        _log.debug('Collecting')

        for sig in info['Signals']:
            chas, chan = sig['Address']['Chassis'], sig['Address']['Channel']
            if (chas, chan) not in jfiles:
                raise RuntimeError(f'Missing j for {chas}, {chan}')

        # adjust .dat file paths
        for chas in info['Chassis']:
            dats = []
            for dat in chas['Dat']:
                dats.append(
                    # Path.relative_to() does not like having to traverse up and back down
                    #(args.input.parent.absolute() / dat).relative_to(args.output.parent.absolute())
                    os.path.join(
                        os.path.relpath(args.input.parent, args.output.parent),
                        dat,
                    )
                )
            chas['Dat'] = dats

        # from now start to modify outdir
        # move j files out of scratch and update json info

        outdir:Path = args.output.parent
        outdir.mkdir(parents=True, exist_ok=True)

        for sig in info['Signals']:
            chas, chan = sig['Address']['Chassis'], sig['Address']['Channel']
            inj = jfiles[(chas, chan)]

            outj = outdir / f"{args.output.stem}-CH{chas:02d}" / f"ch{chan}.j"
            outj.parent.mkdir(exist_ok=True)

            inj.rename(outj) # since both are on the same filesystem, this should be fast meta-data update

            sig['OutDataFile'] = str(outj.relative_to(outdir))

        _log.debug('Done with scratch')
    # done with scratch
    _log.debug('Writing JSON')

    with args.output.open('w') as F:
        json.dump(info, F, indent='  ')

    _log.debug('Done')

    return 1 if total_errors else 0

if __name__=='__main__':
    args = getargs().parse_args()
    logging.basicConfig(level=args.level)
    sys.exit(asyncio.run(main(args), debug=args.debug))
