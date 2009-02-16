#!/usr/bin/env python
from __future__ import with_statement
import getopt
import os
import signal
import sys
import time

def setup_environment():
    import config.importname
    local_config = __import__('config.%s' % config.importname.get(), {}, {}, [''])
    sys.path.insert(0, getattr(local_config, 'DJANGO_PATH_DIR', os.path.join(os.environ['HOME'], 'django-hg')))
    from django.core.management import setup_environ
    import settings
    setup_environ(settings)
    try:
        import config.importname
    except ImportError, e:
        pass


def run(spool, sleep_secs=1):
    while True:
        spool.process()
        time.sleep(sleep_secs)

def stop():
    from sigasync.sigasync_spooler import SPOOLER
    piddir = os.path.join(SPOOLER._base, 'run')
    for fn in os.listdir(piddir):
        pidfile = os.path.join(piddir, fn)
        try:
            with open(pidfile) as pf:
                pid = pf.read()
                print >> sys.stdout, "killing process %s" % pid
                os.kill(int(pid), signal.SIGINT)
        except OSError, e:
            print >> sts.stderr, "couldn't kill process %s" % pid
        os.remove(pidfile)

def start_daemonized(opts):
    from django.utils.daemonize import become_daemon
    kwargs = {
        'err_log': opts.get('-e', '/dev/null'),
        'out_log': opts.get('-o', '/dev/null'),
    }
    become_daemon(**kwargs)
    # make dir for pids
    from sigasync.sigasync_spooler import SPOOLER
    piddir = os.path.join(SPOOLER._base, 'run')
    if not os.path.isdir(piddir):
        os.mkdir(piddir)
    with open(os.path.join(piddir, '%s.pid' % os.getpid()), 'w') as pf:
        pf.write('%s' % os.getpid())
    run(SPOOLER, sleep_secs=opts.get('-s', 1))

def start(opts):
    from sigasync.sigasync_spooler import SPOOLER
    run(SPOOLER, sleeps_secs=opts.get('-s', 1))

def main(args):
    try:
        setup_environment()
        opts, args = getopt.getopt(args, 'Deos', [])
        opts = dict(opts)
        if 'stop' in args[0:1]:
            stop()
        elif 'start' in args[0:1]:
            if '-D' not in opts:
                start_daemonized(opts)
            else:
                start(opts)

    except getopt.GetoptError, e:
        raise


if __name__ == '__main__':
    main(sys.argv[1:])

#END

