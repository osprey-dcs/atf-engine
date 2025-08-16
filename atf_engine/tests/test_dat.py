
import struct
from array import array
from pathlib import Path

from .._convert import convert2j

def make_packets(nsamp:int,
                 seqno:int=0,
                 limits:bool=True,
                 ) -> [bytes]:
    mtu = 1500-40 # 40 is placeholder for IP+UDP headers
    pkts = []
    for i, n in enumerate(range(nsamp)):
        if not pkts or (i%32==0 and len(pkts[-1])>mtu-16-3*32):
            pkts.append(
                struct.pack('>IIQII', 0, 0xffffffff, seqno, 0x12345678, 10*seqno)
            )
            seqno += 1
            if limits:
                pkts[-1] += struct.pack('>IIII', 0x11111111,0x22222222,0x44444444,0x88888888)

        pkts[-1] += struct.pack('>i',n)[1:]

    pkts = [
        struct.pack('>2sHIII', b'PS', 0x4e42 if limits else 0x4e41, len(body), 42, 42) + body
        for body in pkts
    ]

    return pkts

def test_packets():
    pkts = make_packets(14*32)
    assert len(pkts)==1
    assert len(pkts[0])==16+0x568
    assert pkts[0]== b''.join([
        b'PSNB\x00\x00\x05h\x00\x00\x00*\x00\x00\x00*',
        b'\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x124Vx\x00\x00\x00\x00',
        b'\x11\x11\x11\x11""""DDDD\x88\x88\x88\x88',
    ]+[struct.pack('BBB', (x>>16)&0xff, (x>>8)&0xff, (x>>0)&0xff ) for x in range(14*32)])

    pkts2 = make_packets(15*32)
    assert len(pkts2)==2
    assert pkts2 == [
        pkts[0],
        b''.join([
            b'PSNB\x00\x00\x00\x88\x00\x00\x00*\x00\x00\x00*',
            b'\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x01\x124Vx\x00\x00\x00\x0a',
            b'\x11\x11\x11\x11""""DDDD\x88\x88\x88\x88',
        ]+[struct.pack('BBB', (x>>16)&0xff, (x>>8)&0xff, (x>>0)&0xff ) for x in range(14*32, 15*32)])
    ]

def read_j(outdir:Path) -> {int:array}:
    ret = {}
    for ch in range(32):
        j = (outdir / f'CH{ch:02d}.j').read_bytes()
        j = array('i', j)
        assert j[0]==1, j
        ret[ch] = j
    return ret

def test_single(tmp_path:Path):
    'A single packet, not full'
    pkts = make_packets(32*2, seqno=0x01020304)
    indat = tmp_path / 'input.dat'
    indat.write_bytes(b''.join(pkts))

    errs = convert2j([str(indat)],tmp_path)
    assert errs == []

    assert read_j(tmp_path)=={
        n: array('i', [1, 0, 0, 8, 0, n, n+32])
        for n in range(32)
    }

def test_one(tmp_path:Path):
    'A single packet, full'
    pkts = make_packets(32*14, seqno=0x01020304)
    indat = tmp_path / 'input.dat'
    indat.write_bytes(b''.join(pkts))

    errs = convert2j([str(indat)],tmp_path)
    assert errs == []

    assert read_j(tmp_path)=={
            n: array('i', [1, 0, 0, 14*4, 0] + list(range(n, 32*14, 32)))
            for n in range(32)
        }

def test_two(tmp_path:Path):
    'two packets.  The second not full'
    pkts = make_packets(32*20, seqno=0x01020304)
    indat = tmp_path / 'input.dat'
    indat.write_bytes(b''.join(pkts))

    errs = convert2j([str(indat)],tmp_path)
    assert errs == []

    assert read_j(tmp_path)=={
        n: array('i', [1, 0, 0, 20*4, 0] + list(range(n, 32*20, 32)))
        for n in range(32)
    }

def test_parts(tmp_path:Path):
    'Several packets, split across two files'
    pkts = make_packets(32*100, seqno=0x01020304)
    indat1 = tmp_path / 'part1.dat'
    indat1.write_bytes(b''.join(pkts[:3]))
    indat2 = tmp_path / 'part2.dat'
    indat2.write_bytes(b''.join(pkts[3:]))

    errs = convert2j([
        str(indat1),
        str(indat2),
    ],tmp_path)
    assert errs == []

    assert read_j(tmp_path)=={
        n: array('i', [1, 0, 0, 100*4, 0] + list(range(n, 32*100, 32)))
        for n in range(32)
    }

def test_lost_one(tmp_path:Path):
    'A single missing packet'

    pkts = make_packets(32*98, seqno=1200)

    assert len(pkts)==7
    del pkts[3]

    indat = tmp_path / 'input.dat'
    indat.write_bytes(b''.join(pkts))

    errs = convert2j([str(indat)],tmp_path)
    assert errs == ['Missing 1 [1203, 1204)']
    expect = {
        n: array('i', [1, 0, 0, 98*4, 0] + list(range(n, 32*98, 32)))
        for n in range(32)
    }
    for exp in expect.values():
        pos = 5+3*14 # first placeholder sample
        exp[pos:(pos+14)] = array('i', [exp[pos-1]]*14)
    assert read_j(tmp_path)==expect

def test_lost_two(tmp_path:Path):
    'A single missing packet'

    pkts = make_packets(32*98, seqno=1200)

    assert len(pkts)==7
    del pkts[3:5]

    indat = tmp_path / 'input.dat'
    indat.write_bytes(b''.join(pkts))

    errs = convert2j([str(indat)],tmp_path)
    assert errs == ['Missing 2 [1203, 1205)']
    expect = {
        n: array('i', [1, 0, 0, 98*4, 0] + list(range(n, 32*98, 32)))
        for n in range(32)
    }
    for exp in expect.values():
        pos = 5+3*14 # first placeholder sample
        exp[pos:(pos+28)] = array('i', [exp[pos-1]]*28)
    assert read_j(tmp_path)==expect
