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

    n = 5
    r = 10


    t = Timer(lambda: scikit_build_example.build(rawSlice, len(rawSlice)))
    times = t.repeat(repeat=r, number=n)
    print("Native allocate+unpack+assemble: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))
    nativeRes1 = scikit_build_example.build(rawSlice, len(rawSlice))
    plt.figure("Native allocate+unpack+assemble")
    plt.imshow(nativeRes1.T)

    # Just to grab dims from header and build numpy-arrays
    parsedSlice = msgpack.unpackb(rawSlice)
    ass = assembler_slice(None,None,None)   # Note params - not relevant for numpy
    numpyFromPython = ass._numpyFromHeader(parsedSlice[0]['index'])

    scikit_build_example.unpack(rawSlice, len(rawSlice), numpyFromPython) # Avoid including JITTING
    t = Timer(lambda: scikit_build_example.unpack(rawSlice, len(rawSlice), numpyFromPython))
    times = t.repeat(repeat=r, number=n)
    print("Native          unpack+assemble: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))
    plt.figure("Allocate in Python, native build")
    plt.imshow(numpyFromPython.T)


    t = Timer(lambda: msgpack.unpackb(rawSlice, use_list=False))
    times = t.repeat(repeat=r, number=n)
    print("Python          unpack         : {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))

    t = Timer(lambda: scikit_build_example.unpackOnly(rawSlice, len(rawSlice)))
    times = t.repeat(repeat=r, number=n)
    print("Native          unpack         : {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))

    res = ass.numpy(parsedSlice) # Avoid including time spent in JIT
    t = Timer(lambda: ass.numpy(parsedSlice))
    times = t.repeat(repeat=r, number=n)
    print("Python allocate+       assemble: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))

    numpyUsingPyDict = ass._numpyFromHeader(parsedSlice[0]['index'])
    scikit_build_example.assemble_usingpydict(numpyUsingPyDict, parsedSlice[1])
    t = Timer(lambda: scikit_build_example.assemble_usingpydict(numpyUsingPyDict, parsedSlice[1]))
    times = t.repeat(repeat=r, number=n)
    print("Native (PyDict) unpack+assemble: {} iterations {} repeats  min={:.3f}s  max={:.3f}s  avg={:.3f}s".format(n,r,
                                                                        min(times)/n,
                                                                        max(times)/n,
                                                                        np.mean(times)/n))
    
    plt.figure("Pure Python")
    plt.imshow(res.T)
    plt.figure("Native using PyDict")
    plt.imshow(numpyUsingPyDict.T)
    plt.show()
