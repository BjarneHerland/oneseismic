import time
import collections
import numpy as np
import requests
import msgpack
import xarray
from http.client import HTTPException
from threading import Thread
from matplotlib import streamplot

class assembler:
    """Base for the assembler
    """
    kind = 'untyped'

    def __init__(self, src):
        self.sourcecube = src

    def __repr__(self):
        return self.kind

    def numpyFromHeader(self, header):
        raise NotImplementedError

    def numpyAddBundle(self, array, bundle):
        raise NotImplementedError

    def numpyAddTile(self, array, tile):
        raise NotImplementedError
    
    def numpy(self, unpacked):
        """Assemble numpy array

        Assemble a numpy array from a parsed response.

        Parameters
        ----------
        unpacked
            The result of msgpack.unpackb(slice.get())
        Returns
        -------
        a : numpy.array
            The result as a numpy array
        """
        raise NotImplementedError

    def xarray(self, unpacked):
        """Assemble xarray

        Assemble an xarray from a parsed response.

        Parameters
        ----------
        unpacked
            The result of msgpack.unpackb(slice.get())

        Returns
        -------
        xa : xarray.DataArray
            The result as an xarray
        """
        raise NotImplementedError

# Refer to https://numpy.org/doc/stable/user/basics.subclassing.html#basics-subclassing
class progressive_ndarray(np.ndarray):
    def __new__(cls, input_array, target_function):
        obj = np.asarray(input_array).view(cls)
#        obj = super(progressive_ndarray, cls).__new__(cls, *args, **kwargs)
        obj.__thread = Thread(target=target_function)
        obj.__thread.start()
        return obj
    def __array_finalize__(self, obj):
        if obj is None: return
        self.__thread = getattr(obj, '__thread', None)

    def finished_loading(self):
        if not isinstance(self.__thread, Thread):
            return True
        return not self.__thread.is_alive()

    def wait_for(self, timeout=None):
        if isinstance(self.__thread, Thread):
            self.__thread.join(timeout=timeout)
            self.__thread = None # We don´t need this anymore

class assembler_slice(assembler):
    kind = 'slice'

    def __init__(self, sourcecube, dimlabels, name):
        super().__init__(sourcecube)
        self.dims = dimlabels
        self.name = name

    def numpyFromHeader(self, header):
        dims0 = len(header[0])
        dims1 = len(header[1])
        return np.zeros([dims0, dims1], dtype = np.single)

    def numpyAddTile(self, array, tile):
        dst = tile['initial-skip']
        chunk_size = tile['chunk-size']
        src = 0
        v = tile['v']
        arr = np.ravel(array)
        for _ in range(tile['iterations']):
            arr[dst : dst + chunk_size] = v[src : src + chunk_size]
            src += tile['substride']
            dst += tile['superstride']

    def numpyAddBundle(self, array, bundle):
        for tile in bundle['tiles']:
            self.numpyAddTile(array, tile)

    def numpy(self, unpacked):
        index = unpacked[0]['index']
        result = self.numpyFromHeader(index)
        for bundle in unpacked[1]:
            self.numpyAddBundle(result, bundle)

        return result

    def xarray(self, unpacked):
        index = unpacked[0]['index']
        a = self.numpy(unpacked)
        # TODO: add units for time/depth
        return xarray.DataArray(
            data   = a,
            dims   = self.dims,
            name   = self.name,
            coords = index,
        )

class assembler_curtain(assembler):
    kind = 'curtain'

    def numpy(self, unpacked):
        # This function is very rough and does suggest that the message from the
        # server should be richer, to more easily allocate and construct a curtain
        # object
        header = unpacked[0]
        shape = header['shape']
        index = header['index']
        dims0 = len(index[0])
        dimsz = len(index[2])

        # pre-compute where to put traces based on the dim0/dim1 coordinates
        # note that the index is made up of zero-indexed coordinates in the volume,
        # not the actual line numbers
        xyindex = { (x, y): i for i, (x, y) in enumerate(zip(index[0], index[1])) }

        # allocate the result. The shape can be slightly larger than dims0 * dimsz
        # since the traces can be padded at the end. By allocating space for the
        # padded traces we can just put floats directly into the array
        xs = np.zeros(shape = shape, dtype = np.single)

        for bundle in unpacked[1]:
            for part in bundle['traces']:
                x, y, z = part['coordinates']
                v = part['v']
                xs[xyindex[(x, y)], z:z+len(v)] = v[:]

        return xs[:dims0, :dimsz]

    def xarray(self, unpacked):
        index = unpacked[0]['index']
        a = self.numpy(unpacked)
        ijk = self.sourcecube.ijk

        xs = [ijk[0][x] for x in index[0]]
        ys = [ijk[1][x] for x in index[1]]
        # TODO: address this inconsistency - zs is in 'real' sample offsets,
        # while xs/ys are cube indexed
        zs = index[2]
        da = xarray.DataArray(
            data = a,
            name = 'curtain',
            # TODO: derive labels from query, header, or manifest
            dims = ['xy', 'z'],
            coords = {
                'x': ('xy', xs),
                'y': ('xy', ys),
                'z': zs,
            }
        )

        return da

class cube:
    """ Cube handle

    Constructing a cube object does not trigger any http calls as all properties
    are fetched lazily.
    """
    def __init__(self, guid, session):
        self.session = session
        self.guid = guid
        self._shape = None
        self._ijk = None

    @property
    def shape(self):
        """ Shape of the cube

        N-element int-tuple.

        Notes
        -----
        The shape is immutable and the result may be cached.
        """
        if self._shape is not None:
            return self._shape

        self._shape = tuple(len(dim) for dim in self.ijk)
        return self._shape

    @property
    def ijk(self):
        """
        Notes
        -----
        The ijk is immutable and the result may be cached.

        The ijk name is temporary and will change without notice
        """
        if self._ijk is not None:
            return self._ijk

        resource = f'query/{self.guid}'
#        loop = asyncio.get_event_loop()
#        retval = loop.run_until_complete(asyncio.gather(self.session.get(resource)))
#        r = retval[0]
        r = self.session.get(resource)
        self._ijk = [
            [x for x in dim['keys']] for dim in r.json()['dimensions']
        ]
        return self._ijk

    def slice(self, dim, lineno):
        """ Fetch a slice

        Parameters
        ----------

        dim : int
            The dimension along which to slice
        lineno : int
            The line number we would like to fetch. This corresponds to the
            axis labels given in the dim<n> members. In order to fetch the nth
            surface allong the mth dimension use lineno = dim<m>[n].

        Returns
        -------

        slice : numpy.ndarray
        """
        resource = f"query/{self.guid}/slice/{dim}/{lineno}"
        # TODO: derive labels from query, header, or manifest
        labels = ['inline', 'crossline', 'time']
        name = f'{labels.pop(dim)} {lineno}'
        proc = schedule(
            session = self.session,
            resource = resource,
        )
        proc.assembler = assembler_slice(self, dimlabels = labels, name = name)
        return proc

    def curtain(self, intersections):
        """Fetch a curtain

        Parameters
        ----------

        Returns
        -------
        curtain : numpy.ndarray
        """

        resource = f'query/{self.guid}/curtain'
        body = {
            'intersections': intersections
        }
        import json
        proc = schedule(
            session = self.session,
            resource = resource,
            data = json.dumps(body),
        )

        proc.assembler = assembler_curtain(self)
        return proc

class process:
    """

    Maps conceptually to an observer of a process server-side. Comes with
    methods for querying status, completedness, and the final result.

    Parameters
    ----------
    host : str
        Hostname.
    session : request.Session
        A requests.Session-like with a get() method. Authorization headers
        should be set.
    pid : str
        The process id
    status_url : str
        Relative path to the status endpoint.
    result_url : str
        Relative path to the result endpoint.

    Notes
    -----
    Constructing a process manually is reserved for the implementation.

    See also
    --------
    schedule
    """
    def __init__(self, session, pid, status_url, result_url):
        self.session = session
        self.pid = pid
        self.assembler = None
        self.status_url = status_url
        self.result_url = result_url
        self.done = False

    def __repr__(self):
        return '\n\t'.join([
            'oneseismic.process',
                f'pid: {self.pid}',
                f'assembler: {repr(self.assembler)}'
        ])

    def status(self):
        """ Processs status

        Retuns
        ------
        status : str
            Returns one of { 'working', 'finished' }

        Notes
        -----
        This function simply returns what the server responds with, so code
        inspecting the status should always have a fall-through case, in case
        the server is updated and returns something new.
        """
        r = self.session.get(self.status_url)
        response = r.json()

        if r.status_code == 200:
            self.done = True
            return response['status']

        if r.status_code == 202:
            return response['status']

        raise AssertionError(f'Unhandled status code f{r.status_code}')

    def get_raw(self):
        """Get the unparsed response
        Get the raw response for the result. This function will block until the
        result is ready, and will start downloading data as soon as any is
        available.

        Returns
        -------
        reponse : bytes
            The (possibly cached) response
        """
        if hasattr(self,"_cached_raw"):
            return self._cached_raw

        url = f'{self.result_url}'
#        url = f'{self.result_url}/stream'
        r = self.session.get(url)
        self._cached_raw = r.content
        return self._cached_raw

    def stream_raw(self):
        """Get the raw response as a stream.
        """
        url = f'{self.result_url}/stream'
#        def _hook(resp, **kwargs):
#            print("in hook: {}".format(resp.headers.get("X-OnePac-Status", "unknown")))
#            return resp
#        response = self.session.get(stream, stream=True, hooks={"response": _hook})
#        response = self.session.get(stream, stream=True)
#        decoder = msgpack.Unpacker()
        with self.session.get(url) as response:
#            header = None
            response.raise_for_status()
            chunks = []
            num_bytes_downloaded = 0
            self.status = response.headers.get("X-OnePac-Status", "unknown")
            try:
                expLength = -1
                fragmentLen = 0
                for chunk in response.iter_content(chunk_size=None):
#                    print("Chunk len={}  downloaded={}".format(len(chunk), num_bytes_downloaded))
                    if expLength < 0:
                        try:
                            expLength = int(chunk.decode())
#                            print("received expLen={}".format(expLength))
                            continue
                        except Exception as ex:
                            return # TODO
                            if len(chunk) == 0:
                                raise HTTPException("Server closed connection prematurely")
                            raise ex #ServerError("Bad data from server: {}".format(str(ex)))

                    chunks.append(chunk)
                    fragmentLen += len(chunk)
                    num_bytes_downloaded += len(chunk)

                    if fragmentLen > expLength:
                        # SHould not happen...
                        # TODO: split chunk if it does?
                        print("expLen={}  fragmentLen={} ".format(expLength, fragmentLen))
                    elif fragmentLen == expLength:
                        f = b''.join(chunks) or b''
                        yield f
                        expLength = -1
                        fragmentLen = 0
                        chunks = []

                    self.status = str(num_bytes_downloaded)#response.headers.get("X-OnePac-Status", "unknown")

                self.status = str(num_bytes_downloaded)#response.headers.get("X-OnePac-Status", "unknown")
            except Exception as ex:
                self.status = str(ex)
                raise ex

    def stream(self):
        """
        Generate a stream from parsed response-blocks
        """
        decoder = msgpack.Unpacker()
        header = None
        for raw in self.stream_raw():
            decoder.feed(raw)
            time.sleep(0.2) # TODO: remove - just for demo-purposes
            if header is None:
                header = {}
                header['arrayheader1'] = decoder.read_array_header()
                header['mapheader'] = decoder.read_map_header()

                for _ in ("Bundles", "Shape", "Index"):
                    key, val = decoder.unpack(), decoder.unpack()
                    header[key] = val
                
                header['arrayheader2'] = decoder.read_array_header()
                yield header
            else:
                fragment = decoder.unpack()
                yield fragment

    def get(self):
        """Get the parsed response synchronously
        """
        try:
            return msgpack.unpackb(b''.join([b for b in self.stream_raw()]))
        except Exception:
            raise HTTPException("Invalid data returned from server")

    def numpy(self, callback = None):

        if hasattr(self,"_cached_numpy"):
            return self._cached_numpy

        if callback is None:
            self._cached_numpy = self.assembler.numpy(self.get())
            return self._cached_numpy

        assert callable(callback)
        
        gen = self.stream()  # create the async generator
        header = next(gen)
        r = self.assembler.numpyFromHeader(header["index"])
        def drain():
            for chunk in gen:
                self.assembler.numpyAddBundle(r, chunk)
                callback(r)

        self._cached_numpy = progressive_ndarray(r, drain)
        return self._cached_numpy

    def xarray(self):
        if hasattr(self,"_cached_xarray"):
            return self._cached_xarray

        self._cached_xarray = self.assembler.xarray(self.get())
        return self._cached_xarray

    def withcompression(self, kind = 'gz'):
        """Get response compressed if available

        Request that the response be sent compressed, if available.  Compressed
        responses are typically half the size of uncompressed responses, which
        can be faster if there is limited bandwidth to oneseismic. Compressed
        responses are typically not faster inside the data centre.

        If kind is None, compression will be disabled.

        Compression defaults to 'gz'.

        Parameters
        ----------
        kind : { 'gz', None }, optional
            Compression algorithm. Defaults to gz.

        Returns
        -------
        self : process

        Examples
        --------
        Read a compressed slice:
        >>> proc = cube.slice(dim = 0, lineno = 5024)
        >>> proc.withcompression(kind = 'gz')
        >>> s = proc.numpy()
        >>> proc = cube.slice(dim = 0, lineno = 5).withcompression(kind = 'gz')
        >>> s = proc.numpy()
        """
        self.session.withcompression(kind)
        return self

    def withgz(self):
        """process.withcompression(kind = 'gz')
        """
        return self.withcompression(kind = 'gz')

def schedule(session, resource, data = None):
    """Start a server-side process.

    This function centralises setting up a HTTP session and building the
    process object, whereas end-users should use methods on the outermost cube
    class.

    Parameters
    ----------
    session : requests.Session
        Session object with a get() for making http requests
    resource : str
        Resource to schedule, e.g. 'query/<id>/slice'

    Returns
    -------
    proc : process
        Process handle for monitoring status and getting the result

    Notes
    -----
    Scheduling a process manually is reserved for the implementation.
    """
    r = session.get(resource, params = data)

    body = r.json()
    auth = 'Bearer {}'.format(body['authorization'])
    s = http_session(session.base_url)
    s.headers.update({'Authorization': auth})

    pid = body['location'].split('/')[-1]
    return process(
        session = s,
        pid = pid,
        status_url = body['status'],
        result_url = body['location'],
    )

class http_session(requests.Session):
    """
    http_session provides some automation on top of the requests.Session type,
    to simplify http requests in more seismic-specific interfaces and logic.
    Methods also raise non-200 http status codes as exceptions.

    The http_session methods do not take absolute URLs, but relative URLs e.g.
    req.get(url = 'result/<pid>/status').

    Parameters
    ----------
    base_url : str
        The base url, schema + host, for the oneseismic service
    auth :
        Object to request up-to-date authorization headers from

    Notes
    -----
    This class is meant for internal use, to provide a clean boundary for
    low-level network-oriented code.
    """
    def __init__(self, base_url, tokens = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_url = base_url
        self.tokens = tokens

    def merge_auth_headers(self, kwargs):
        if self.tokens is None:
            return kwargs

        headers = self.tokens.headers()
        if 'headers' in kwargs:
            # unpack-and-set rather than just assigning the dictionary, in case
            # headers() starts returning more than just the Authorization
            # headers. This puts the power of definition where it belongs, and
            # keeps http_session oblivious to oneseismic specific header
            # expectations.
            #
            # If users at call-time explicitly set any of these headers,
            # respect them
            for k, v in headers.items():
                kwargs['headers'].setdefault(k, v)
        else:
            kwargs['headers'] = headers

        return kwargs

    def get(self, url, *args, **kwargs):
        """HTTP GET

        requests.Session.get, but raises exception for non-200 HTTP status
        codes. Authorization headers will be added to the request if
        http_session.tokens is available.

        This function will respect call-level custom headers, and only use
        http_session.tokens.headers() if not specified, similar to the requests
        API [1]_.

        Parameters
        ----------
        url : str
            Relative url to the resource, e.g. 'result/<pid>/status'

        Returns
        -------
        r : request.Response

        See also
        --------
        requests.get

        References
        ----------
        .. [1] https://requests.readthedocs.io/en/master/user/advanced/#session-objects

        Examples
        --------
        Defaulted and custom authorization:
        >>> session = http_session(url, tokens = tokens)
        >>> session.get('/needs-auth')
        >>> session.get('/needs-auth', headers = { 'Accept': 'text/html' })
        >>> session.get('/no-auth', headers = { 'Authorization': None })
        """
        
        kwargs = self.merge_auth_headers(kwargs)
        r = super().get(f'{self.base_url}/{url}', *args, **kwargs, stream=True)
        r.raise_for_status()
        return r

    def withcompression(self, kind):
        """Get response compressed if available

        Request that the response be sent compressed, if available.  Compressed
        responses are typically half the size of uncompressed responses, which
        can be faster if there is limited bandwidth to oneseismic. Compressed
        responses are typically not faster inside the data centre.

        If kind is None, compression will be disabled.

        Parameters
        ----------
        kind : { 'gz', None }
            Compression algorithm

        Returns
        -------
        self : http_session

        Notes
        -----
        This function does not accept defaults, and the http_session does not
        have withgz() or similar methods, since it is a lower-level class and
        not built for end-users.
        """
        if kind is None:
            self.params.pop('compression', None)
            return self

        kinds = ['gz']
        if kind not in kinds:
            msg = f'compression {kind} not one of {",".join(kinds)}'
            raise ValueError(msg)
        self.params['compression'] = kind
        return self

    @staticmethod
    def fromconfig(cache_dir = None):
        """Create a new session from on-disk config

        Create a new http_sesssion with parameters and auth read from disk.
        This is a convenient constructor for most programs and uses outside of
        testing.

        Parameters
        ----------
        cache_dir : path or str, optional
            Configuration cache directory

        Returns
        -------
        session : http_session
            A ready-to-use http_session with authorization headers set
        """
        from ..login.login import config, tokens
        cfg = config(cache_dir = cache_dir).load()
        # cfg.update(
        #     {'url': 'http://localhost:8080',
        #        'client_id': 'MY_ID',
        #        'auth_server': 'http://auth:8089/common',
        #        'scopes': ['api://5a00a74a-2af7-40e0-a5a6-af94581715ae/One.Read'],
        #        'timeout': 60})
        auth = tokens(cache_dir = cache_dir).load(cfg)
        return http_session(base_url = cfg['url'], tokens = auth)

def ls(session):
    """List available cubes

    List the cubes stored in oneseismic. The ids returned should all be valid
    arguments for the oneseismic.client.cube class.

    Parameters
    ----------
    session : oneseismic.http_session
        Session with authorization headers set

    Returns
    -------
    guids : iterable of str
        Cube GUIDs

    See also
    --------
    oneseismic.client.cube
    """
    return session.get('query').json()['links'].keys()
#    loop = asyncio.get_event_loop()
#    retval = loop.run_until_complete(asyncio.gather(session.get('query')))
#    return retval[0].json()['links'].keys()

class cubes(collections.abc.Mapping):
    """Dict-like interface to cubes in the oneseismic subscription

    Parameters
    ----------
    session : http_session
    """
    def __init__(self, session):
        self.session = session
        self.cache = None

    def __getitem__(self, guid):
        if guid not in self.guids:
            raise KeyError(guid)
        return cube(guid, self.session)

    def __iter__(self):
        yield from self.guids

    def __len__(self):
        return len(self.guids)

    def sync(self):
        """Synchronize the set of guids in the subscription.

        It is generally only necessary to call this function once, but it can
        be called manually to get new IDs that have been added to the
        subscription since the client was created. For programs, it is
        Generally a better idea to create a new client.

        This is intended for internal use.
        """
        self.cache = ls(self.session)

    @property
    def guids(self):
        """Guids of cubes in subscription

        This is for internal use.

        All other functions should use this property to interact with guids, as
        it manages the cache.
        """
        if self.cache is None:
            self.sync()
        return self.cache

class cli:
    """User friendly access to oneseismic

    Access oneseismic services in a user-friendly manner with the cli class,
    suitable for programs, REPLs, and notebooks.

    Parameters
    ----------
    session : http_session
    """
    def __init__(self, session):
        self.session = session

    @property
    def cubes(self):
        return cubes(self.session)
