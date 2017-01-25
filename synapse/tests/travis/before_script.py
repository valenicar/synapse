
import os
import sys
import argparse
import subprocess

def parse_args(argv):
    parser = argparse.ArgumentParser()

    args = parser.parse_args(argv)

    return args

def main(argv):
    args = parse_args(argv)
    cmds = []

    if os.environ.get('SYN_CORE_RAM'):
        cmds = [
            'docker ps | grep -q core_ram',
            'nc -v -w 4 127.0.0.1 47322',
        ]
    if os.environ.get('SYN_CORE_SQLITE'):
        cmds = [
            'docker ps | grep -q core_sqlite',
            'nc -v -w 4 127.0.0.1 47322',
        ]
    if os.environ.get('SYN_CORE_PG95'):
        cmds = [
            'docker ps | grep -q core_pg95',
            'nc -v -w 8 127.0.0.1 47322',
            '''docker exec core_pg95 /bin/bash -c "psql -c 'create database syn_test;' -U postgres"''',
        ]

    for cmd in cmds:
        print('run: %r' % (cmd,))
        proc = subprocess.Popen(cmd, shell=True)
        proc.wait()

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

