import asyncio
import json
import logging
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

_log = logging.getLogger(__name__)

def findexe(s):
    R = shutil.which(s)
    if R is None:
        raise RuntimeError(f'Executable not found: {s}')
    return R

def getargs():
    from argparse import ArgumentParser

    P = ArgumentParser()
    P.add_argument('-v', '--verbose', dest='level', default=logging.INFO,
                   action='store_const', const=logging.DEBUG,
                   help='Enable extra application logging')
    P.add_argument('-d', '--debug', action='store_true',
                   help='Enable extra asyncio logging')
    P.add_argument('--fileConverter', type=findexe,
                   help='Location of FileReformatter2 executable')
    P.add_argument('input', type=Path,
                   help='Input JSON header file')
    P.add_argument('output', type=Path,
                   help='Output JSON header file.  Data files placed relative.')
    return P

async def runProc(*args, **kws):
    'Run child to completion'
    cmd = ' '.join([repr(a) for a in args])
    _log.debug('Run: %s # %r', cmd, kws)
    P = await asyncio.create_subprocess_exec(*args, **kws)
    try:
        await P.wait()
    except asyncio.CancelledError:
        _log.error('Killing: %d, %s', P.pid, cmd)
        P.kill()
        raise
    else:
        if P.returncode!=0:
            raise RuntimeError(f'Error from {args!r}')
    _log.debug('Success: %s', cmd)


async def main(args):
    exe = args.fileConverter or findexe('FileReformatter2')

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

        frags = []
        async with asyncio.TaskGroup() as tg:
            for chas in info['Chassis']:
                n = chas['Chassis']
                dat = chas['Dat']
                assert len(dat)==1, dat

                chas_scratch = scratch / f'CH{n:02d}'
                chas_scratch.mkdir()
                frag = chas_scratch / 'out.hdr'
                frags.append(frag)
                tg.create_task(runProc(
                    exe,
                    '--chassis', str(n),
                    '--output', str(frag),
                    str(args.input),
                    str(args.input.parent.joinpath(dat[0])),
                ))
        # all tasks complete successfully

        for frag in frags:
            with frag.open('r') as F:
                finfo = json.load(F)

            for sig in finfo['Signals']:
                idx = idxCH[(sig['Address']['Chassis'], sig['Address']['Channel'])]
                assert info['Signals'][idx]['Address']==sig['Address']

                chan_data = frag.parent.joinpath(sig['OutDataFile'])
                assert chan_data.exists(), chan_data

                chas_dir = outdir / f"{args.output.stem}-CH{sig['Address']['Chassis']:02d}"
                chan_file = chas_dir / f"ch{sig['Address']['Channel']}{chan_data.suffix}"

                chas_dir.mkdir(exist_ok=True)
                chan_file = chan_data.rename(chan_file) # move (not copy) out of tempdir

                info['Signals'][idx]['OutDataFile'] = str(chan_file.relative_to(args.output.parent))

    with args.output.open('w') as F:
        json.dump(info, F, indent='  ')


if __name__=='__main__':
    args = getargs().parse_args()
    logging.basicConfig(level=args.level)
    asyncio.run(main(args), debug=args.debug)
