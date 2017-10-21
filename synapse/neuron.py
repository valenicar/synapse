import os
import threading
import collections

import synapse.glob as s_glob
import synapse.common as s_common
import synapse.cortex as s_cortex

import synapse.lib.fifo as s_fifo
import synapse.lib.config as s_config
import synapse.lib.socket as s_socket
import synapse.lib.thishost as s_thishost

'''
A node tufo looks like so...

(iden, {
    'links':{
        dest:{info},
    },
})

# once ingested, a link tuple is like so...
( (iden0, iden1), {info} )

'''

class Node(s_config.Config):

    def __init__(self, conf):

        s_config.Config.__init__(self)
        self.setConfOpts(conf)
        self.reqConfOpts()

        self.lock = threading.RLock()

        dirn = self.getConfOpt('neur:dir')
        self.dirn = s_common.genpath(dirn)

        if not os.path.isdir(self.dirn):
            raise s_common.BadConfValu(name='neur:dir', valu=dirn, mesg='no such directory')

        path = self._getNeurPath('core.db')
        self.core = s_cortex.openurl('sqlite:///%s' % path)

        path = self._getNeurPath('fifo')
        conf = {'fifo:dir': self._getNeurPath('fifo')}

        os.makedirs(conf.get('fifo:dir'), mode=0o700, exist_ok=True)

        fcnf = self.getConfOpt('neur:fifo:conf')
        if fcnf is not None:
            conf.update(fcnf)

        self.fifo = s_fifo.Fifo(conf)

        self.iden = self.core.myfo[0]

        self.nodes = {
            self.iden: {
                'host': s_thishost.get('hostname'),
                'links': {},
            },
        }

        self.links = {}
        self.links_by_src = collections.defaultdict(list)
        self.links_by_dst = collections.defaultdict(list)

        self.routes = {}
        self.routes_by_link = collections.defaultdict(set)

        self.mesgfuncs = {
            'link': self._handLinkMesg,
            #'xmit': self._handXmitMesg,
        }

        # handlers for messages delivered *locally*
        #self.loclfuncs = {}

        opts = {}
        for url in self.getConfOpt('neur:links'):

            if type(url) in (list, tuple):
                url, opts = url

            link = s_link.chop(url, **opts)

        self.plex = s_socket.Plex()
        self.plex.on('link:sock:mesg', self._onSockMesg)

    def setNodeDef(self, node):
        '''
        Add a node definition to the neuron graph.

        Args:
            node ((str,dict)):  A neuron node tufo

        Returns:
            (bool): True if the set was an update
        '''
        with self.lock:

            iden = node[0]

            oldn = self.nodes.get(iden)
            if oldn is not None:

                if oldn[1].get('tick') >= node[1].get('tick'):
                    return False

                # check if there are removed links...
                newl = node[1].get('links')
                for link in self.links_by_src.get(iden, ()):
                    if newl.get(link[0][1]) is None:
                        self.delNodeLink(link)

            self.nodes[iden] = node

            # add or update links...
            for nexn, info in node[1].get('links').items():

                lkey = (iden, nexn)
                link = self.links.get(lkey)

                if link is None:
                    self.addNodeLink((lkey, info))

                else:
                    link[1].update(info)

            return True

    def _onSockMesg(self, evnt):
        # this is the plex thread.  gtfo.
        sock = evnt[1].get('sock')
        mesg = evnt[1].get('mesg')
        # use the global pool for message handling
        s_glob.pool.call(self._handSockMesg, sock, mesg)

    def _handSockMesg(self, sock, mesg):
        # handle a message from a socket (in a pool thread)
        try:

            func = self.mesgfuncs.get(mesg[0])
            if func is None:
                return

            func(sock, mesg)

        except Exception as e:
            logger.exception()

    def addNodeLink(self, link):
        with self.lock:
            self.links[link[0]] = link
            self.links_by_src[link[0][0]].append(link)
            self.links_by_dst[link[0][1]].append(link)

    def delNodeLink(self, link):

        with self.lock:

            link = self.links.pop(link[0], None)
            if link is None:
                return

            self.links_by_src[link[0][0]].remove(link)
            self.links_by_dst[link[0][1]].remove(link)

            rkeys = self.routes_by_link.pop(link[0], ())
            [self.routes.pop(rkey, None) for rkey in rkeys]

    def getNodeLink(self, lkey):
        return self.links.get(lkey)

    # a node is (iden, {'links':{}, ...})
    # a link is ( (iden0, iden1), {info} )
    # a rout is ( link, ... )

    def getRouteNext(self, iden):
        rout = self.getRoutTo(iden)
        if rout is None:
            return None

        # [ ((src,dst),{}), ... ]
        return rout[0][0][1]

    def getRouteTo(self, iden):

        with s_glob.lock:

            rkey = (self.iden, iden)

            rout = self.routes.get(rkey)
            if rout is not None:
                return rout

            done = set([self.iden])
            todo = collections.deque()

            for link in self.links_by_src.get(self.iden, ()):

                # optimize for our links first...
                rout = (link,)
                if iden == link[0][1]:
                    return self._addRouteCache(rkey, rout)

                todo.append(rout)

            # hopefully we can find one...
            while todo:

                rout = todo.popleft()

                tail = rout[-1][0][1]

                done.add(tail)

                for link in self.links_by_src.get(tail):

                    node = link[0][1]
                    if node == iden:
                        return self._addRouteCache(rkey, rout + (link, ))

                    if node in done:
                        continue

                    # put in work todo...
                    todo.append(rout + (link,))

            return None

    def _addRouteCache(self, rkey, rout):
        # must be called with self.lock
        self.routes[rkey] = rout
        [self.routes_by_link[l[0]].add(rkey) for l in rout]
        return rout

    def _handXmitSelf(self, sock, xmit):

        # xmit messages are the "application layer"
        # so lets unpack and handle the inner message
        mesg = s_msgpack.un(xmit[1].get('data'))

        # if we have a session, send it...
        iden = mesg[1].get('sess')
        if iden is not None:
            sess = self.sess.get(iden)
            if sess is not None:
                sess.xmit(mesg)
            return

    def _handXmitMesg(self, sock, mesg):

        # this routine is expected to defend the neuron
        # from xmit messages from non-authenticated socks
        # and other garbage...

        mesg[1]['ttl'] -= 1
        if mesg[1]['ttl'] < 0:
            return

        link = sock.get('neur:link')
        if link is not None:
            return self._routXmitMesg(sock, mesg)

        sess = sock.get('neur:sess')
        if sess is not None:
            mesg['orig'] = self.iden
            mesg['user'] = sock.get('syn:user')
            return self._routXmitMesg(sock, mesg)

    def _routXmitMesg(self, sock, mesg):

        dest = mesg[1].get('dest')
        if dest == self.iden:
            return self._handXmitSelf(sock, mesg)

        # TODO eventually want some kind of icmp like reply
        nexi = self.getRouteNext(dest)
        if nexi is None:
            return

        sock = self.getLinkSock(nexi)
        if sock is None:
            return

        sock.tx(mesg)

    def _handLinkMesg(self, sock, mesg):
        # another node would like to link with us
        iden = mesg[1].get('iden')

        user = sock.get('syn:user')

        # if this mesg is requests a reply, send one

    def _getNeurPath(self, *paths):
        ndir = self.getConfOpt('neur:dir')
        return s_common.genpath(ndir, *paths)

    #def listen(self, url, **opts):
    #def connect(self, url, **opts):

    @staticmethod
    @s_config.confdef(name='neuron')
    def _getNeurConf():
        return (
            ('neur:dir', {'type': 'str', 'req': 1, 'doc': 'The working directory for this node'}),
            ('neur:links', {'defval': [], 'doc': 'A list of link entries: url or (url,{})'}),
            ('neur:listen', {'doc': 'A list of link entries: url or (url,{})'}),
            ('neur:fifo:conf', {'doc': 'A nested config dict for our own fifo'}),

            ('neur:dend:ctors', {'doc': 'A list of (dynf,conf) tuples for the dendrites to load'}),
        )

#def open(url, **opts):

class Dendrite(s_config.Config):
    '''
    A Dendrite defines service endpoints on a Neuron.
    '''
    def __init__(self, conf):
        s_config.Config.__init__(self)
        self.setConfOpts(conf)
        self.reqConfOpts()

        self._svc_funcs = {}
        self.init()

    def add(self, name, func):
        '''
        Register a service endpoint name and callback.
        '''
        self._svc_funcs[name] = func

    def init(self):
        '''
        '''
        pass

    def exec(self, task, work):
        '''
        Synchronosly execute the given work as task.
        '''
        with task:

            try:

                func = self._svc_funcs.get(work[0])
                if func is None:
                    raise NoSuchName(name=name)

                func(task, work)

            except Exception as e:
                enfo = s_common.excinfo(e)
                task.err(enfo)

#class Nerve:
