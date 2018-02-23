import sys
import json
import pprint
import argparse
import logging

import synapse.common as s_common
import synapse.cryotank as s_cryotank

import synapse.lib.output as s_output
import synapse.lib.msgpack as s_msgpack

logger = logging.getLogger(__name__)

def main(argv, outp=s_output.stdout):

    pars = argparse.ArgumentParser(prog='cryo.cat', description='display data items from a cryo cell')
    pars.add_argument('cryocell', help='The cell descriptor and cryo tank path (cell://<host:port>/<name>).')
    pars.add_argument('--list', default=False, action='store_true', help='List tanks in the remote cell and return')
    pars.add_argument('--offset', default=0, type=int, help='Begin at offset index')
    pars.add_argument('--size', default=10, type=int, help='How many items to display')
    pars.add_argument('--timeout', default=10, type=int, help='The network timeout setting')
    pars.add_argument('--authfile', help='Path to your auth file for the remote cell')
    pars.add_argument('--jsonl', default=False, action='store_true', help='Output items in jsonl format')
    pars.add_argument('--verbose', '-v', default=False, action='store_true', help='Verbose output')

    # TODO: make input mode using stdin...
    # TODO: make --jsonl output form for writing to file
    # TODO: make --no-index option that prints just the item

    opts = pars.parse_args(argv)

    if opts.verbose:
        logger.setLevel(logging.INFO)

    if not opts.authfile:
        logger.error('Currently requires --authfile until neuron protocol is supported')
        return 1

    authpath = s_common.genpath(opts.authfile)

    auth = s_msgpack.loadfile(authpath)

    netw, path = opts.cryocell[7:].split('/', 1)
    host, portstr = netw.split(':')

    addr = (host, int(portstr))
    logger.info('connecting to: %r', addr)

    cryo = s_cryotank.CryoUser(auth, addr, timeout=opts.timeout)

    if opts.list:
        for name, info in cryo.list(timeout=opts.timeout):
            outp.printf('%s: %r' % (name, info))

        return 0

    for item in cryo.slice(path, opts.offset, opts.size, opts.timeout):
        if opts.jsonl:
            outp.printf(json.dumps(item[1], sort_keys=True))
        else:
            outp.printf(pprint.pformat(item))

    return 0

if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig()
    sys.exit(main(sys.argv[1:]))
