import matplotlib.pyplot as plt
import pickle
from timeit import Timer
import random
import numpy as np
import msgpack
import oneseismic

from oneseismic.client import assembler_slice

import scikit_build_example

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

    parsedSlice = scikit_build_example.unpack(rawSlice, len(rawSlice)) # Avoid including JITTING

    parsedSlice = msgpack.unpackb(rawSlice) # Avoid including JITTING
    n = 1
    r = 5
    t = Timer(lambda: msgpack.unpackb(rawSlice, use_list=False))
    times = t.repeat(repeat=r, number=n)
    print("MsgUnpack: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))

    print(parsedSlice[0])
    ass = assembler_slice(None,None,None) # Note params - not relevant for numpy
    res = ass.numpy(parsedSlice) # Avoid including time spent in JIT

#    n = 100
#    r = 5
    t = Timer(lambda: ass.numpy(parsedSlice))
    times = t.repeat(repeat=r, number=n)
    print("Numpy: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))

    nativeRes = ass._numpyFromHeader(parsedSlice[0]['index'])
    print(nativeRes.shape)
    scikit_build_example.assemble(nativeRes, parsedSlice[1])
#    n = 100
#    r = 5
    t = Timer(lambda: scikit_build_example.assemble(nativeRes, parsedSlice[1]))
    times = t.repeat(repeat=r, number=n)
    print("Native: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))
    
    plt.figure("Normal numpy")
    plt.imshow(res.T)
    plt.figure("Native")
    plt.imshow(nativeRes.T)
    plt.show()
