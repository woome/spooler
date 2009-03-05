#!/usr/bin/env python
from __future__ import with_statement
import atexit
import cgi
import getopt
from itertools import groupby
from operator import itemgetter
import os
import signal
from stat import ST_CTIME
from subprocess import Popen, PIPE
import sys
import time

GRACEFULINT = False
DO_PROCESS = True

def setup_environment(additional_settings=None):
    """setup our django 'app' environment"""
    import config.importname
    local_config = __import__('config.%s' % config.importname.get(), {}, {}, [''])
    sys.path.insert(0, getattr(local_config, 'DJANGO_PATH_DIR', os.path.join(os.environ['HOME'], 'django-hg')))
    from django.core.management import setup_environ
    import settings
    setup_environ(settings)
    if additional_settings:
        from django.conf import settings
        if type(additionalsettings, type(sys)): # if this is a module
            additional_settings = dict((k,v) for k,v in additional_settings.__dict__ if k == k.upper())
        settings.configure(default_settings=settings, **additional_settings)

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
    spool.close_transaction_after_execute = True
    while DO_PROCESS:
        spool.process()
        time.sleep(sleep_secs)
    sys.exit(0)

def remove_proc_dir(spooler):
    """remove processing dir for spooler"""
    os.rmdir(spooler._processing)

def get_arguments(optdict, key):
    """get kwargs from our opt dict

    >>> sorted(get_arguments({'-a': 'foo=bar'}, '-a').items())
    [('foo', 'bar')]
    >>> sorted(get_arguments({'-a': 'foo=bar&bar=baz'}, '-a').items())
    [('bar', 'baz'), ('foo', 'bar')]
    >>> sorted(get_arguments({'-a': ['foo=bar', 'bar=baz']}, '-a').items())
    [('bar', 'baz'), ('foo', 'bar')]
    >>> sorted(get_arguments({'-a': ['foo=bar&another=hello', 'bar=baz']}, '-a').items())
    [('another', 'hello'), ('bar', 'baz'), ('foo', 'bar')]

    """
    if key not in optdict:
        return {}
    val = optdict[key]
    args = {}
    if isinstance(val, list):
        val = '&'.join(val)
    return opts_to_dict(cgi.parse_qsl(val))

def opts_to_dict(opts):
    opts.sort(key=itemgetter(0))
    dict_ = {}
    for group, vals in groupby(opts, itemgetter(0)):
        val = [v for k,v in vals]
        if len(val) > 1:
            dict_[group] = val
        elif val:
            dict_[group] = val[0]
    return dict_

# get spooler for options dict
class Spooler(object):
    def __new__(cls, opts):
        if not hasattr(cls, 'spooler'):
            _Spool = named(opts.get('-m', 'sigasync.sigasync_spooler.get_spoolqueue'))
            cls.spooler = _Spool(**get_arguments(opts, '-a'))
            # register function to remove processing dir when we exit
            atexit.register(remove_proc_dir, cls.spooler)
        return cls.spooler

def getpids(opts):
    """get all pids available for spooler given in opts"""
    spooler = Spooler(opts)
    piddir = os.path.join(spooler._base, 'run')
    if not os.path.isdir(piddir):
        return
    for fn in os.listdir(piddir):
        pidfile = os.path.join(piddir, fn)
        with open(pidfile) as pf:
            pid = pf.read()
            yield pidfile, int(pid)


def stop(opts):
    """kill all spooler procs we have pids for"""
    for pidfile, pid in getpids(opts):
        try:
            os.kill(pid, signal.SIGINT)
            print >> sys.stdout, "killing process %s" % pid
        except OSError, e:
            print >> sys.stderr, "couldn't kill process %s" % pid
        os.remove(pidfile)

def _isprocessrunning(pid):
    ps = Popen(['ps', '-p', '%s' % pid], stdout=PIPE)
    return bool(Popen(['grep', '%s' % pid], stdin=ps.stdout, stdout=PIPE).communicate()[0])

def get_spoolname_from_pidfile(pidfile):
    fn = os.path.basename(pidfile)
    return os.path.splitext(fn)[0]

def get_spool_info(spooldir):
    jobs = os.listdir(spooldir)
    jobfn = lambda job: os.path.join(spooldir, job)
    def jctime(job):
        try:
            return os.path.getctime(jobfn(job))
        except OSError, e:
            return time.time()
    calcage = lambda ts: time.time() - ts
    maxage = lambda jobs: reduce(max, (calcage(jctime(job)) for job in jobs), 0)
    return len(jobs), int(maxage(jobs))

def status(opts):
    """get status of spooler by looking in processing directory"""
    spooler = Spooler(opts)
    pids = dict((get_spoolname_from_pidfile(pidfile), pid) for pidfile, pid in getpids(opts))
    prgen = os.walk(spooler._processing_base)
    root, spools, _ignore = prgen.next()
    print >> sys.stdout, "spool\t\tjobs\tmax age (s)\tstatus"
    if '--in' in opts:
        print >> sys.stdout, "in\t\t%s\t%s\t\t---" % get_spool_info(spooler._in)
    for spool in spools:
        if spool == os.path.basename(spooler._processing):
            continue # the spool we're seeing is the one created for us above
        if spool in pids:
            status = 'running' if _isprocessrunning(pids[spool]) else 'crashed - no process'
            del pids[spool]
        else:
            status = 'crashed - no pidfile'
        numjobs, maxage = get_spool_info(os.path.join(root, spool))
        print >> sys.stdout, "%s\t%s\t%s\t\t%s" % (spool, numjobs, maxage, status)
    if hasattr(spooler, '_failed') and os.path.exists(spooler._failed):
        print >> sys.stdout, "failed\t\t%s\t%s\t\t---" % (get_spool_info(spooler._failed)[0], '---')

    if pids:
        print >> sys.stdout, "\norphaned pid files:"
        for pidfile, pid in pids.iteritems():
            print >> sys.stdout, "%s.pid\t%s" % (pidfile, pid)

def flushpids(opts):
    """delete orphaned pidfiles"""
    spooler = Spooler(opts)
    pids = getpids(opts)
    for pidfile, pid in pids:
        if not _isprocessrunning(pid):
            spool = get_spoolname_from_pidfile(pidfile)
            if os.path.exists(os.path.join(spooler._processing_base, spool)):
                print >> sys.stdout, "crashed spool '%s' needs recovering" % spool
            else:
                os.remove(pidfile)
                print >> sys.stdout, "removed pidfile: %s" % pidfile

def recover(spool, opts):
    """move crashed spooler jobs back to in queue"""
    spooler = Spooler(opts)
    print >> sys.stdout, "attempting to recover spool '%s'" % spool
    spoolpath = os.path.join(spooler._processing_base, spool)
    if not os.path.exists(spoolpath):
        print >> sys.stderr, "error: spool dir '%s' does not exist" % spoolpath
        sys.exit(1)
    _pids = list(getpids(opts))
    pids = dict((get_spoolname_from_pidfile(pidfile), pid) for pidfile, pid in _pids)
    pidfiles = dict((get_spoolname_from_pidfile(pidfile), pidfile) for pidfile, pid in _pids)
    if spool in pids and _isprocessrunning(pids[spool]):
        print >> sys.stderr, "error: spool '%s' is running" % spool
        sys.exit(1)
    elif spool in pids:
        os.remove(pidfiles[spool])
    for job in os.listdir(spoolpath):
        os.rename(os.path.join(spoolpath, job), os.path.join(spooler._in, job))
        print >> sys.stdout, "moved job '%s' to in spool" % job
    os.rmdir(spoolpath)
    print >> sys.stdout, "successful!"

def start_daemonized(opts):
    kwargs = {
        'err_log': opts.get('-e', '/dev/null'),
        'out_log': opts.get('-o', '/dev/null'),
    }
    become_daemon(**kwargs)
    # make dir for pids
    spooler = Spooler(opts)
    piddir = os.path.join(spooler._base, 'run')
    if not os.path.isdir(piddir):
        os.mkdir(piddir)
    with open(os.path.join(piddir, '%s.pid' % os.path.basename(spooler._processing)), 'w') as pf:
        pf.write('%s' % os.getpid())
    run(spooler, sleep_secs=opts.get('-s', 1))

def start(opts):
    spooler = Spooler(opts)
    run(spooler, sleep_secs=opts.get('-s', 1))


class NoCommandError(Exception):
    pass

def main(args):
    try:
        opts, args = getopt.getopt(args, 'Dle:o:s:m:a:', ['nodjango', 'in'])
        opts = opts_to_dict(opts)
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
        elif 'recover' in args[0:1]:
            if not args[1:2]:
                print >> sys.stderr, "you must specify a spool to recover"
                sys.exit(1)
            for spool in args[1:]:
                recover(spool, opts)
        elif 'flushpids' in args[0:1]:
            flushpids(opts)
        else:
            raise NoCommandError()

    except getopt.GetoptError, e:
        raise
    except NoCommandError, e:
        print >> sys.stdout, """usage: %s [options] start|stop|status|recover|flushpids [spool to recover]
        options:
        -D:         do not daemonize
        -e:         error log file
        -o:         stdout log file
        -s <num>:   number of seconds for each sleep loop. default 1
        -m:         python path of spool to instantiate/factory method. 
                        default sigasync.sigasync_spooler.get_spoolqueue
        -a:         args to constructor/factory. key=value
                        can be used multiple times or qs style ('&' sep.)
        --nodjango: do no load django environment
        --in:       list incoming jobs in status

        example: %s -m sigasync.sigasync_spooler.get_spoolqueue -a name=default -D start

        """ % (sys.argv[0], sys.argv[0])


if __name__ == '__main__':
    # catch sigint and exit so that our atexit handler gets called to clean processing dir
    def exit(signum, frm):
        if GRACEFULINT:
            DO_PROCESS = False
        else:
            sys.exit(1)
    signal.signal(signal.SIGINT, exit)
    main(sys.argv[1:])

#END

