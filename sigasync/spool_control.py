#!/usr/bin/env python
from __future__ import with_statement
import atexit
import getopt
import os
import signal
import sys
import time

GRACEFULINT = False
DO_PROCESS = True

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

# tea-leaf'd from django.utils.daemonize
def become_daemon(our_home_dir='.', out_log='/dev/null', err_log='/dev/null'):
    "Robustly turn into a UNIX daemon, running in our_home_dir."
    # First fork
    try:
        if os.fork() > 0:
            os._exit(0)     # kill off parent
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)
    os.setsid()
    os.chdir(our_home_dir)
    os.umask(0)

    # Second fork
    try:
        if os.fork() > 0:
            os._exit(0)
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        os._exit(1)

    si = open('/dev/null', 'r')
    so = open(out_log, 'a+', 0)
    se = open(err_log, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
    # Set custom file descriptors so that they get proper buffering.
    sys.stdout, sys.stderr = so, se


# this one lifted from eventlet.api with no hint of remorse
def named(name):
    """Return an object given its name.

    The name uses a module-like syntax, eg::

      os.path.join

    or::

      mulib.mu.Resource
    """
    toimport = name
    obj = None
    while toimport:
        try:
            obj = __import__(toimport)
            break
        except ImportError, err:
            # print 'Import error on %s: %s' % (toimport, err)  # debugging spam
            toimport = '.'.join(toimport.split('.')[:-1])
    if obj is None:
        raise ImportError('%s could not be imported' % (name, ))
    for seg in name.split('.')[1:]:
        try:
            obj = getattr(obj, seg)
        except AttributeError:
            dirobj = dir(obj)
            dirobj.sort()
            raise AttributeError('attribute %r missing from %r (%r) %r' % (
                seg, obj, dirobj, name))
    return obj


def run(spool, sleep_secs=1):
    while DO_PROCESS:
        spool.process()
        time.sleep(sleep_secs)
    sys.exit(0)

def remove_proc_dir(spooler):
    os.rmdir(spooler._processing)


def getspooler(opts):
    spooler = named(opts.get('-m', 'sigasync.sigasync_spooler.SPOOLER'))
    atexit.register(remove_proc_dir, spooler)
    return spooler

def getpids(opts):
    spooler = getspooler(opts)
    piddir = os.path.join(spooler._base, 'run')
    for fn in os.listdir(piddir):
        pidfile = os.path.join(piddir, fn)
        with open(pidfile) as pf:
            pid = pf.read()
            yield pidfile, int(pid)


def stop(opts):
    for pidfile, pid in getpids(opts):
        try:
            os.kill(pid, signal.SIGINT)
            print >> sys.stdout, "killing process %s" % pid
        except OSError, e:
            print >> sts.stderr, "couldn't kill process %s" % pid
        os.remove(pidfile)


def status(opts):
    spooler = getspooler(opts)
    prroot = os.walk(spooler._processing_base)

def start_daemonized(opts):
    kwargs = {
        'err_log': opts.get('-e', '/dev/null'),
        'out_log': opts.get('-o', '/dev/null'),
    }
    become_daemon(**kwargs)
    # make dir for pids
    spooler = getspooler(opts)
    piddir = os.path.join(spooler._base, 'run')
    if not os.path.isdir(piddir):
        os.mkdir(piddir)
    with open(os.path.join(piddir, '%s.pid' % os.getpid()), 'w') as pf:
        pf.write('%s' % os.getpid())
    run(spooler, sleep_secs=opts.get('-s', 1))

def start(opts):
    spooler = getspooler(opts)
    run(spooler, sleep_secs=opts.get('-s', 1))


class NoCommandError(Exception):
    pass

def main(args):
    try:
        opts, args = getopt.getopt(args, 'Dle:o:s:m:', ['nodjango'])
        opts = dict(opts)
        if '--nodjango' not in args:
            setup_environment()
        if 'stop' in args[0:1]:
            stop(opts)
        elif 'status' in args[0:1]:
            status(opts)
        elif 'start' in args[0:1]:
            if '-D' not in opts:
                start_daemonized(opts)
            else:
                start(opts)
        else:
            raise NoCommandError()

    except getopt.GetoptError, e:
        raise
    except NoCommandError, e:
        print >> sys.stdout, """usage: %s [options] start|stop|status
        options:
        -D:         do not daemonize
        -e:         error log file
        -o:         stdout log file
        -s <num>:   number of seconds for each sleep loop. default 1
        -m:         python path of spool instance. default sigasync.sigasync_spooler.SPOOLER
        --nodjango: do no load django environment

        """ % sys.argv[0]


if __name__ == '__main__':
    def exit(signum, frm):
        if GRACEFULINT:
            DO_PROCESS = False
        else:
            sys.exit(1)
    signal.signal(signal.SIGINT, exit)
    main(sys.argv[1:])

#END

