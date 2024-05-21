
import asyncio
import json
import logging
import shutil
import time
import glob
from pathlib import Path
from tempfile import TemporaryDirectory

from p4p.nt import NTScalar, NTEnum
from p4p.client.asyncio import Context
from p4p.server import Server
from p4p.server.asyncio import SharedPV

from .pvcache import PVCache, PVEncoder

_log = logging.getLogger(__name__)

class Engine:
    def __init__(self, prefix:str, base:Path):
        self.outbase = base
        self.ctxt = Context(nt=False)
        self.cond = asyncio.Condition()
        self.cache = PV = PVCache(self.ctxt, cond=self.cond)

        self._run_stop = SharedPV(nt=NTEnum(),
                                 initial={'index':0, 'choices':['Stop', 'Run', 'Abort']})
        self._run_stop.put(self.onRunStop) # set onPut handler
        self._status = SharedPV(nt=NTEnum(),
                               initial={'index':0, 'choices':['Not Ready', 'Ready']})
        self._last_msg = SharedPV(nt=NTScalar('s'), initial='startup')
        self._last_name = SharedPV(nt=NTScalar('s'), initial='')
        self._last_out = SharedPV(nt=NTScalar('s'), initial='')
        self.serv_pvs = {
            f'{prefix}CTRL:Run-SP': self._run_stop,
            f'{prefix}SA:READY_': self._status,
            f'{prefix}CTRL:LastName-I': self._last_name,
            f'{prefix}CTRL:LastMsg-I': self._last_msg,
            f'{prefix}CTRL:LastFile-I': self._last_out,
        }

        # ready input
        self.ready = PV(f'{prefix}SA:READY')
        # ADC run output
        self.acq = PV(f'{prefix}ACQ:enable')

        self.FileDir = [f'{prefix}{node:02d}:FileDir-SP' for node in range(1,33)]
        self.FileBase = [f'{prefix}{node:02d}:FileBase-SP' for node in range(1,33)]
        self.Record = [f'{prefix}{node:02d}:Record-Sel' for node in range(1,33)]

        self.info = {
            'AcquisitionId': PV(f'{prefix}SA:DESC'),
            #'Role1Name': PV(f'{prefix}SA:OPER'),
            'CCCR': PV(f'{prefix}SA:FILE'),
            'CCCR_SHA256': PV(f'{prefix}SA:FILEHASH'),
            'SampleRate': PV(f'{prefix}ACQ:rate.RVAL'), # Hz
            'AcquisitionStartDate': None,
            'AcquisitionEndDate': None,
            'Signals': [
                {
                    'Address': {'Chassis':node, 'Channel':ch},
                    'SigNum': (node-1)*32 + ch,
                    'Inuse': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:USE'),
                    'Name': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:NAME'),
                    'Desc': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:DESC'),
                    'Egu': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:EGU'),
                    'Slope': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:SLO'),
                    'Intercept': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:OFF'),
                    'Coupling': PV(f'{prefix}{node:02d}:ACQ:coupling:{ch:02d}'),
                    'ResponseNode': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:RESPNODE'),
                    'ResponseDirection': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:RESPDIR.RVAL', signed=True),
                    'Type': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:SDTYP.RVAL'),
                    'LastCal': PV(f'{prefix}{node:02d}:SA:Ch{ch:02d}:TCAL'), # posix time
                    'ReferenceNode':0,
                    'ReferenceDirection':0,
                }
                for node in range(1, 33)
                for ch in range(1, 33)
            ],
            'Chassis': [],
        }

        self.ready_to_go = False
        self._statusT = asyncio.create_task(self.watch_status(), name='Status Task')
        self._sequenceT = None
        self._sequenceStop = None
        _log.debug('Engine ctor complete')

    async def __aenter__(self):
        return self

    async def __aexit__(self,A,B,C):
        _log.debug('Engine joining')
        for T in (self._statusT, self._sequenceT):
            if T is None:
                continue
            _log.debug('Engine join %r', T)
            T.cancel()
            try:
                await T
            except asyncio.CancelledError:
                pass
        _log.debug('Engine joined')

    def onRunStop(self, pv, op):
        val = op.value()
        _log.debug('onPut(%r)', val)
        if val==0:
            if self._sequenceStop is not None:
                self._sequenceStop.set()
                _log.debug('onPut(%r) request stop', val)
        elif val==1:
            if self._sequenceT is None and self.ready_to_go:
                self._sequenceT = asyncio.create_task(self.sequence(), name='Sequence')
                _log.debug('onPut(%r) starting sequence', val)
        elif val==2:
            if self._sequenceT is not None and not self._sequenceT.cancelled():
                self._sequenceT.cancel()
                _log.debug('onPut(%r) interrupt sequence', val)
        else:
            _log.warning('onPut(%r) unexpected', val)
            op.done(error='unexpected value')
            return
        _log.debug('onPut(%r) done', val)
        op.done()

    async def watch_status(self):
        while True:
            try:
                async with self.cache:
                    await self.cache.wait()
                    _log.debug('Recompute status')
                    prev, ready = self.ready_to_go, True
                    if not self.cache.all_connected():
                        if _log.isEnabledFor(logging.DEBUG):
                            _log.debug('not all_connected.  remaining: %r ...', self.cache.disconnected()[:10])
                        ready = False
                    elif not (self.ready.value=='Ready'):
                        _log.debug('not ready: %r', self.ready.value)
                        ready = False
                    elif not (self.acq.value=='Disable'):
                        _log.debug('not disabled: %r', self.acq.value)
                        ready = False
                    self.ready_to_go = ready
                    if prev!=ready:
                        _log.debug('status change %r -> %r', prev, ready)
                        self._status.post(int(ready))
            except asyncio.CancelledError:
                raise
            except:
                _log.exception('oops!')
                await time.sleep(10) # at least slow down the log spam...

    async def sequence(self):
        try:
            self._run_stop.post(1)
            _log.debug('Sequence starting')
            assert self._sequenceT==asyncio.current_task(), (self._sequenceT, asyncio.current_task())
            self._sequenceStop = asyncio.Event()
            await self._sequence()
            self._last_msg.post('Success')
        except asyncio.CancelledError:
            self._last_msg.post('Abort')
            raise
        except:
            _log.exception("oops")
            self._last_msg.post('Failure')
        finally:
            _log.debug('Cleanup after sequence')
            try:
                self._run_stop.post(0)
                self._status.post(0)
                async with asyncio.timeout(5.0): # bound time of cleanup.  eg. during cancel()
                    await self.ctxt.put(self.acq.name, {'value.index':0})
                    await self.ctxt.put(self.Record, [{'value.index':0}]*32)
                    await self.ctxt.put(self.FileDir, [{'value':''}]*32)
                    await self.ctxt.put(self.FileBase, [{'value':''}]*32)
            finally:
                self._sequenceT = self._sequenceStop = None

    async def _sequence(self):
        assert self.ready_to_go

        Tstart = time.time()
        T = time.localtime(Tstart) # customer requests localtime for string representations...

        # snapshot full info tree.
        # Round trip uses PVEncoder to grab current value, or throw if any Disconnected
        jmeta = json.dumps(self.info, cls=PVEncoder, indent='  ')
        info = json.loads(jmeta)

        # filter inuse signals and chassis
        info['Signals'] = Signals = [S for S in info['Signals'] if S['Inuse']=='Yes']
        if len(Signals)==0:
            raise RuntimeError('No signals in use, check CCCR')
        Chassis = {S['Address']['Chassis'] for S in Signals} # {1->32}
        _log.debug('Recording with %d chassis', len(Chassis))

        desc = info['AcquisitionId'] # base ID w/o datetime
        info['AcquisitionStartDate'] = time.strftime('%Y%m%d %H%M%S%z', T)

        assert desc.strip()==desc, desc

        # /data/YYYY/mm/YYYYmmDD-HHMMSS-desc/
        rundir = self.outbase \
            / time.strftime('%Y', T) \
            / time.strftime('%m', T) \
            / time.strftime(f'%Y%m%d-%H%M%S-{desc}', T)
        rundir.mkdir(parents=True, exist_ok=False)
        _log.info('Output directory: %s', rundir)

        fprefix = time.strftime(f'{desc}-%Y%m%d-%H%M%S', T)
        CHprefix = [f'{fprefix}-CH{ch:02d}-' for ch in range(1,33)]

        self._last_name.post(fprefix)

        await self.ctxt.put(self.FileDir, [{'value':str(rundir)}]*32)
        await self.ctxt.put(self.FileBase, [{'value':p} for p in CHprefix])
        await self.ctxt.put(self.Record, [{'value.index':chas in Chassis} for chas in range(1,33)])
        _log.debug('Recording paths are set')

        # write out only meta-data before any .dat written for context if something goes wrong...
        hdr = rundir / f'{fprefix}.hdr'
        with hdr.open('x') as F: # must not already exist
            F.write(jmeta)
            _log.debug('Wrote preliminary JSON %s', F.name)

        await self.ctxt.put(self.acq.name, {'value.index':1})
        _log.info('Acquiring...')
        self._last_msg.post('Acquire') # everything up to this point should happen quickly

        await self._sequenceStop.wait()
        _log.info('Stop Acquire...')
        self._last_msg.post('Stopping...') # acknowledge stop command

        await self.ctxt.put(self.acq.name, {'value.index':0})
        _log.debug('Stopped Acquire...')

        # need to wait for in-flight packets to land on disk.
        # TODO: how to do this properly?
        await asyncio.sleep(5.0)

        # find .dat files
        info['Chassis'] = []
        for chas in Chassis:
            _log.debug('look for chassis %d %s*.dat', chas, CHprefix[chas-1])
            dats = glob.glob(str(rundir / f'{CHprefix[chas-1]}*.dat'))
            if len(dats)!=1:
                _log.warning('Not 1 .dat for chassis %d: %r', chas, dats)

            info['Chassis'].append({
                'Chassis': chas,
                'Dat': dats,
            })

        with hdr.open('w') as F: # must not already exist
            json.dump(info, F, indent=' ')
            _log.debug('Wrote second JSON %s', F.name)

        self._last_msg.post('Post-process')


#        with TemporaryDirectory(dir=str(rundir)) as tempdir:
#            tempdir = Path(tempdir)
#            Tasks = []
#            for ch in Chassis:

        # state Convert
        # write meta-only .hdr

        # mkdir 32x /tmp
        # launch converters

        # read partial headers
        # move 1024x channel .j

        # write final .hdr

        # Done

def getargs():
    from argparse import ArgumentParser
    P = ArgumentParser()
    P.add_argument('--prefix', default='FDAS:',
                   help='Global PV name prefix')
    P.add_argument('--root', type=Path, default=Path('/data'),
                   help='Data directory root')
    P.add_argument('-v', '--verbose', dest='level', default=logging.INFO,
                   action='store_const', const=logging.DEBUG,
                   help='Enable extra application logging')
    P.add_argument('-d', '--debug', action='store_true',
                   help='Enable extra asyncio logging')
    return P

async def main(args):
    _log.info('Starting w/ %r', args.prefix)
    logging.getLogger('p4p').level = logging.INFO
    import signal
    loop = asyncio.get_running_loop()

    async with Engine(prefix=args.prefix, base=args.root) as E:
        with Server(providers=[E.serv_pvs]):
            done = asyncio.Event()
            loop.add_signal_handler(signal.SIGINT, done.set)
            loop.add_signal_handler(signal.SIGTERM, done.set)

            _log.debug('Running')
            await done.wait()
            _log.debug('Stopping')
