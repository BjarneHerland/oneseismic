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

# Trick to update pyplot image - see e.g. https://stackoverflow.com/a/43885275
fig, axis = plt.subplots()
def cb(array):
    print("Time={:.2f}s  progress: {:.2f}%".format(
        time.time()-t0,
        100*slce.progress))
    return
    img = getattr(cb, "_img", None)
    if img is None:
        cb._img = axis.imshow(array.T)
    else:
        cb._img.set_array(array.T)
        fig.canvas.draw_idle()
        fig.canvas.flush_events()

slce.numpy(asynchronous=cb)
plt.show()
