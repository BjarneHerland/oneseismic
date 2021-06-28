import pickle
from timeit import Timer
import random
import numpy as np
import msgpack
import oneseismic

from oneseismic.client import assembler_slice


def loadSlice():
    cubes = {
      'sverdrup': '4f1140e1c43ecf070bc9e2d324940044e8b6f480',
      }
    guid = cubes['sverdrup']
    with oneseismic.client.new() as cli:
        cube = cli.cubes[guid]
        ijk = cube.ijk
        return cube.slice(dim = 0, lineno = random.choice(ijk[0])).get_raw()

if __name__ == "__main__":
    rawSlice = None
    try:
        with open('/tmp/slice-raw.pickle', 'rb') as f:
            rawSlice = pickle.load(f)
        print("Got raw slice from local cache...")
    except:
        print("No raw slice in local cache... trying load from server")
        rawSlice = loadSlice()
        print("Got raw slice from server...")
        with open('/tmp/slice-raw.pickle','wb') as f:
            pickle.dump(rawSlice, f)
    
    if rawSlice is None:
        raise RuntimeError("Could not find slice...")

    parsedSlice = msgpack.unpackb(rawSlice) # Avoid including JITTING
    n = 10
    r = 5
    t = Timer(lambda: msgpack.unpackb(rawSlice, use_list=False))
    times = t.repeat(repeat=r, number=n)
    print("MsgUnpack: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))
    
    ass = assembler_slice(None,None,None) # Note params - not relevant for numpy
    ass.numpy(parsedSlice) # Avoid including time spent in JIT

    n = 10
    r = 5
    t = Timer(lambda: ass.numpy(parsedSlice))
    times = t.repeat(repeat=r, number=n)
    print("Numpy: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))
    
