import matplotlib.pyplot as plt
import time
import random
import oneseismic

cubes = {
  'sverdrup': '4f1140e1c43ecf070bc9e2d324940044e8b6f480',
  'stor': '0affd870b46dadabae1340d7984048a83b1f79a3'
  }
guid = cubes['sverdrup']

cli = oneseismic.client.new()
cube = cli.cubes[guid]
print(cube.shape)
ijk = cube.ijk
t0 = time.time()
slce = cube.slice(dim = 0, lineno = random.choice(ijk[0]))

res = slce.numpy()
print("Time: {:.2f}s".format(time.time() - t0))
plt.imshow(res.T)
plt.show()

