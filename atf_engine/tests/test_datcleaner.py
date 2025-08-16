
import asyncio
from pathlib import Path

import pytest

from ..datcleaner import DatCleaner

@pytest.mark.asyncio
async def test_dat(tmp_path:Path):
    patterns = ["a*.dat", "b*.dat"]
    D = DatCleaner(tmp_path, patterns)
    D.getCount = lambda: 2
    async with D:
        (tmp_path / "canary.dat").write_text("Testing")
        await asyncio.sleep(0)
        (tmp_path / "afile.dat").write_text("afile")
        await asyncio.sleep(0)
        (tmp_path / "bfile.dat").write_text("bfile")
        await asyncio.sleep(0)
        (tmp_path / "another.dat").write_text("another")
        await asyncio.sleep(0)
        (tmp_path / "bother.dat").write_text("bother")
        await asyncio.sleep(0)
        with (tmp_path / "afinal.dat").open('w') as F:
            F.write("afinal")
            F.flush()
            await asyncio.sleep(0.5) # wait for inotify open/write events (but not close!)
            assert (tmp_path / "afile.dat").exists()
        await asyncio.sleep(0.5)

    assert D.tracked()==[
        ('a*.dat', ["another.dat", "afinal.dat"]),
        ('b*.dat', ["bfile.dat", "bother.dat"]),
    ]

    assert (tmp_path / "canary.dat").exists()
    assert not (tmp_path / "afile.dat").exists()
    assert (tmp_path / "bfile.dat").exists()
    assert (tmp_path / "another.dat").exists()
    assert (tmp_path / "bother.dat").exists()
    assert (tmp_path / "afinal.dat").exists()
