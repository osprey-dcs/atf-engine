
import asyncio
import json
import logging
from pathlib import Path
from weakref import WeakValueDictionary

from p4p.client.asyncio import Context, Disconnected

_log = logging.getLogger(__name__)

class PVEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, PVEntry):
            o = o.read() # throw on Disconnected
        elif isinstance(o, Path):
            o = str(o)
        else:
            o = super().default(o)
        return o

class PVCache:
    def __init__(self, ctxt:Context, cond:asyncio.Condition = None):
        self.ctxt = ctxt
        self._C = WeakValueDictionary()
        self._cond = cond or asyncio.Condition()

    def __call__(self, pv:str, signed=None) -> 'PVEntry':
        try:
            R = self._C[pv]
            assert signed is None or signed is R.signed
        except KeyError:
            self._C[pv] = R = PVEntry(self, pv, signed=signed)
        return R

    # delegate to asyncio.Condition
    async def __aenter__(self):
        await self._cond.__aenter__()
        return self
    async def __aexit__(self,A,B,C):
        await self._cond.__aexit__(A,B,C)

    async def wait(self):
        await self._cond.wait()

    def all_connected(self):
        return all([e._value is not None for e in self._C.values()])

    def disconnected(self):
        return [k for k,e in self._C.items() if e._value is None]

class PVEntry:
    def __init__(self, cache:PVCache, pv:str, signed=None):
        self.name, self.__cache, self.signed = pv, cache, signed
        self._value = None
        self._S = cache.ctxt.monitor(pv, self.__update, notify_disconnect=True)

    async def __update(self, V):
        if isinstance(V, Exception):
            self._value = None
            if not isinstance(V, Disconnected):
                _log.exception(self.name)

        else:
            self._value = V

        async with self.__cache._cond:
            self.__cache._cond.notify_all()

    @property
    def value(self):
        V = self._value
        if V is not None:
            V = V.value
            if hasattr(V, 'choices'):
                V = V.choices[V.index]
            elif isinstance(V, str):
                V = V.strip()
            return V
        else:
            return None

    def read(self):
        R = self.value
        if R is None:
            raise ValueError(f'{self.name} Disconnect')
        return R
