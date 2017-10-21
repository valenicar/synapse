import copy
import time

from copy import deepcopy as cp

import synapse.common as s_common
import synapse.neuron as s_neuron

import synapse.lib.task as s_task

from synapse.tests.common import *

class FooBar(s_neuron.Dendrite):

    def init(self):
        self.add('foo:bar:woot', woot)
        self.add('foo:bar:boom', boom)

    def woot(self, task, work):
        name, args, kwargs = work
        task.retn(args[0] + args[1])

    def boom(self, task, work):
        raise Exception('boom')

def foobar(conf):
    return FooBar(conf)

class NeurTest(SynTest):

    def test_neuron_route(self):
        # stand alone test for neuron route APIs

        with self.getTestDir() as ndir:

            conf = {
                'neur:dir': ndir,
            }

            node = s_neuron.Node(conf)

            node0 = (s_common.guid(), {
                'links': {},
                'tick': s_common.now(),
            })

            node1 = (s_common.guid(), {
                'links': {},
                'tick': s_common.now(),
            })

            idenx = node.iden
            iden0 = node0[0]
            iden1 = node1[0]

            node0[1]['links'][node1[0]] = {}
            node1[1]['links'][node0[0]] = {}

            # manually add nodes we would get from peers
            node.setNodeDef(copy.deepcopy(node0))
            node.setNodeDef(copy.deepcopy(node1))

            self.nn(node.getNodeLink((iden0, iden1)))
            self.nn(node.getNodeLink((iden1, iden0)))
            self.none(node.getNodeLink((idenx, iden0)))

            # manually add a link we would get from sock

            link = ((idenx, iden0), {})
            node.addNodeLink(link)

            self.nn(node.getNodeLink((idenx, iden0)))

            rout = node.getRouteTo(iden1)
            nods = tuple((l[0][1] for l in rout))
            self.eq(nods, (iden0, iden1))

            # test that the route is cached
            self.true(node.getRouteTo(iden1) is rout)

            # check that the cache dumps on link del
            node.delNodeLink(link)
            self.none(node.getRouteTo(iden1))

            node1[1]['links'].pop(iden0)

            self.false(node.setNodeDef(cp(node1)))

            time.sleep(0.01)
            node1[1]['tick'] = s_common.now()

            self.true(node.setNodeDef(cp(node1)))

            # test that the node link was removed...
            self.none(node.getNodeLink((iden1, iden0)))

    def test_neuron_dendrite(self):

        conf = {}
        with s_neuron.Dendrite(conf) as dend:

            def woot(task, work):
                name, args, kwargs = work
                task.retn(args[0] + args[1])

            def boom(task, work):
                raise Exception('OMG')

            dend.add('lol:woot', woot)
            dend.add('lol:boom', boom)

            # working
            rets = []
            task = s_task.Task()
            task.onretn(rets.append)

            work = ('lol:woot', (10, 20), {})

            dend.exec(task, work)

            self.true(task.isfini)
            self.eq(rets[0], (True, 30))

            # boom
            rets = []
            task = s_task.Task()
            task.onretn(rets.append)

            work = ('lol:boom', (10, 20), {})

            dend.exec(task, work)

            self.true(task.isfini)
            self.eq(rets[0][0], False)
            self.eq(rets[0][1]['err'], 'Exception')
