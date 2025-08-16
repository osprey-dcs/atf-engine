
import asyncio
import logging
from fnmatch import fnmatch
from pathlib import Path

from inotify.constants import IN_ALL_EVENTS, IN_MODIFY, IN_CLOSE_WRITE
from inotify.adapters import Inotify, _DEFAULT_TERMINAL_EVENTS, TerminalEventException

_log = logging.getLogger(__name__)

class AInotify(Inotify):
    '''PyInotify circa 0.2.10 does not include asyncio support.
    '''
    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)

    async def aevent_gen(self, terminal_events=_DEFAULT_TERMINAL_EVENTS):
        loop = asyncio.get_running_loop()
        try:
            while True:
                ready = loop.create_future()
                # replaces previous reader for this FD
                loop.add_reader(self._Inotify__inotify_fd, ready.set_result, None)

                await ready

                # _handle_inotify_event() must call read(fd) exactly once,
                # so we can get away with not setting FD to non-blocking
                for (header, type_names, path, filename) in self._handle_inotify_event(self._Inotify__inotify_fd):
                    e = (header, type_names, path, filename)
                    for type_name in type_names:
                        if type_name in terminal_events:
                            raise TerminalEventException(type_name, e)

                    yield e

        finally:
            loop.remove_reader(self._Inotify__inotify_fd)

class DatCleaner:
    '''Watch for FS operations in a directory on files matching given patterns
       Remove all but the most recent N files.

    >>> patterns = ['some*.dat', 'other*.dat']
    >>> D = DatCleaner('/tmp', patterns)
    >>> with D:
           ... accumulate files, and close all!
    >>> for pat, dats in D.tracked(): # perserves order of patterns
        print(pat, dats)
    '''

    def __init__(self, base:Path, patterns:[str]):
        self._base = Path(base)
        self._patterns = [(pat, []) for pat in patterns]
        self._T = None

    def getCount(self) -> int:
        raise NotImplementedError()

    async def __aenter__(self):
        assert self._T is None, self._T
        self._T = asyncio.create_task(self._handle())

    async def __aexit__(self,A,B,C):
        self._T.cancel()
        try:
            await self._T
        except asyncio.CancelledError:
            pass
        finally:
            self._T = None

    async def _handle(self):
        _log.debug('Tracking in %r', self._base)
        I = AInotify()
        I.add_watch(str(self._base), IN_ALL_EVENTS&~IN_MODIFY) # exclude chatty modify event

        async for evt, evtnames, path, file in I.aevent_gen():
            if not (evt.mask & IN_CLOSE_WRITE):
                continue

            C = self.getCount()

            for pat, trk in self._patterns:
                if not fnmatch(file, pat):
                    _log.debug('mis-match %r, %r',pat, file)
                    continue

                _log.debug('Close event %r, %r, %s : %r', pat, file, C, trk)
                trk.append(file)

                if C>0:
                    while len(trk)>C:
                        rm = trk.pop(0)
                        _log.debug('Delete %r', rm)
                        (self._base / rm).unlink(missing_ok=True)

                break # treat patterns as non-overlapping

        _log.debug('End Tracking in %r', self._base)

    def tracked(self) -> [(str, str)]:
        # cross check the accumulated delta with the full list.
        # must be the same entries, also order which we can not check here
        for pat, trk in self._patterns:
            full = set(self._base.glob(pat))
            trk = {self._base / t for t in trk}
            assert full==trk, (full, trk)

        return self._patterns
