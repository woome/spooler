#!/usr/bin/python

"""Spooler stuff
"""

from __future__ import with_statement
import os
import sys
import tempfile
import signal
import atexit
import glob
import logging
from os.path import join as pathjoin
from os.path import exists as pathexists
from os.path import basename, dirname
from time import sleep
from datetime import datetime, timedelta
from multiprocessing import Process

SLEEP_TIME = 0.1  # seconds

# How frequently to check the load and adjust spool processes
ADJUST_INTERVAL = timedelta(seconds=60)

class SpoolExists(Exception):
    pass

class SpoolDoesNotExist(Exception):
    pass

class FailError(Exception):
    """fail the entry"""
    pass

class SpoolManager(object):
    """Provide hooks for a Spool."""

    def __init__(self):
        self.spool = None
        self._should_stop = False

    def stop(self, spool):
        self._should_stop = True

    # Hook methods, to be called by a Spool
    # All hook methods take the calling Spool as the first non-self argument

    def created_spool(self, spool, incoming, outgoing, failed):
        """Notify that the spool has been created."""
        pass

    def started_processing(self, spool, incoming):
        """Notify that processing on a list of entries has begun."""
        pass

    def created_processing(self, spool, processing):
        """Notify that the processing directory has been created."""
        logger = logging.getLogger("sigasync.spooler.SpoolManager")
        logger.info("Created processing dir %s" % processing, extra={'pid': os.getpid()})
        pass

    def processed_entry(self, spool, entry):
        """Notify that an entry has been successfully processed."""
        pass

    def failed_entry(self, spool, entry):
        """Notify that an entry has failed to process."""
        pass

    def finished_processing(self, spool):
        """Notify that the spool has finished processing a list."""
        pass

    def should_stop(self, spool):
        return self._should_stop


class SpoolContainer(object):
    """Spooler container
    Contains the spools, manages processes, etc."""
    
    def __init__(self, manager=SpoolManager, directory=None):
        self._children = set()
        self._base = directory
        self._should_exit = False
        self._last_adjusted = datetime.now()

        if isinstance(manager, type):
            self.manager = manager()
        else:
            self.manager = manager

        self._read_config()

    # TODO: Factor out Django-specific config
    def _read_config(self):
        """Pull data from config into local data structures."""
        from django.conf import settings
        # Allow base directory to be overridden (useful for testing)
        if self._base is None:
            self._base = settings.SPOOLER_DIRECTORY

        self._pid_base = settings.SPOOLER_PID_BASE

        # The keys are logical queues, the values are real queues
        queues = set(settings.SPOOLER_QUEUE_MAPPINGS.values())
        qdict = {}
        #import ipdb; ipdb.set_trace()
        defaults = settings.SPOOLER_DEFAULTS
        for queue in queues:
            queue_dir = pathjoin(self._base, queue)
            try:
                qconf = defaults.copy()
                qconf.update(getattr(settings, 'SPOOLER_%s' % queue.upper()))
            except AttributeError:
                qconf = defaults

            in_, out, retry15, retry60, fail = [pathjoin(queue_dir, d) for d in
                                 ['in', 'out', 'retry15', 'retry60', 'failed']]
            qdict[queue] = {'incoming': in_,
                            'outgoing': out,
                            'failure': retry15,
                            'minprocs': qconf['minprocs'],
                            'maxprocs': qconf['maxprocs'],
                            'nprocs': qconf['minprocs'],
                            'procs': []}
            qdict[queue+'_retry15'] = {'incoming': retry15,
                                       'outgoing': out,
                                       'failure': retry60,
                                       'minprocs': 1,
                                       'maxprocs': 2,
                                       'nprocs': 1,
                                       'procs': [],
                                       'filter': self._delay_filter(15)}
            qdict[queue+'_retry60'] = {'incoming': retry60,
                                       'outgoing': out,
                                       'failure': fail,
                                       'minprocs': 1,
                                       'maxprocs': 2,
                                       'nprocs': 1,
                                       'procs': [],
                                       'filter': self._delay_filter(60)}
        self._queues = qdict

    def _start_spools(self):
        for queue, conf in self._queues.iteritems():
            for n in range(conf['nprocs']):
                self._start_spool(queue)

    def _start_spool(self, queue):
        logger = logging.getLogger("sigasync.spooler.SpoolContainer")
        process = Process(target=self.spool_process,
                          args=(queue, self._queues[queue]))
        process.daemon = True
        process.start()
        self._queues[queue]['procs'].append(process)
        self._children.add(process)
        logger.info("Started spool process %s for %s queue."
                     % (process.pid, queue))
        return process

    def _adjust_spool(self, queue):
        logger = logging.getLogger("sigasync.spooler.SpoolContainer._adjust_spool")
        qd = self._queues[queue]
        try:
            entries = len(os.listdir(qd['incoming']))
        except OSError, e:
            logger.error("Error opening %s: %s" % (qd['incoming'], e))
            return
        if entries > 100 and qd['nprocs'] < qd['maxprocs']:
            logger.info("Spawning new process for %s" % queue)
            qd['nprocs'] += 1
        elif entries < 50 and qd['nprocs'] > qd['minprocs']:
            logger.info("Removing process for %s" % queue)
            qd['nprocs'] -= 1

    def _write_pid(self):
        with open(pathjoin(self._base, '%s%s.pid' % (self._pid_base, os.getpid())), 'w') as f:
            f.write("%s" % os.getpid())

    def _remove_pid(self):
        try:
            os.unlink(pathjoin(self._base, '%s%s.pid' % (self._pid_base, os.getpid())))
        except Exception, e:
            logging.getLogger("Spool").warning(
                    "Failed to remove pidfile %s%s.pid" % (self._pid_base, os.getpid()))

    def run(self):
        logger = logging.getLogger("sigasync.spooler.SpoolContainer.run")
        self._start_spools()
        self._write_pid()
        atexit.register(self._remove_pid)
        signal.signal(signal.SIGINT, self.exit)

        while not self._should_exit:
            adjusted = False
            # check status of spools
            for queue, dict_ in self._queues.iteritems():
                procs = dict_['procs']
                # Clean out dead processes in this queue
                for p in procs[:]:
                    if not p.is_alive():
                        if p.exitcode > 0:
                            logger.warning("Spool %s-%s exited with code %s"
                                    % (queue, p.pid, p.exitcode))
                        elif p.exitcode < 0:
                            logger.warning("Spool %s-%s was killed by signal %s"
                                    % (queue, p.pid, -1 * p.exitcode))
                        procs.remove(p)
                        self._children.remove(p)
                # Adjust the target number of processes based on load
                if datetime.now() - self._last_adjusted > ADJUST_INTERVAL:
                    self._adjust_spool(queue)
                    adjust = True
                # Create any new processes needed
                if not self._should_exit:
                    for i in range(dict_['nprocs'] - len(procs)):
                        self._start_spool(queue)
            if adjusted:
                self._last_adjusted = datetime.now()
            # Touch the pid file
            self._write_pid()
            sleep(SLEEP_TIME)

        # Clean up
        logger.info("Shutting down child processes...")
        for p in self._children:
            try:
                os.kill(p.pid, signal.SIGINT)
            except OSError:
                # Already died
                pass
        for p in self._children:
            p.join()

    def exit(self, sig, frame):
        logger = logging.getLogger("sigasync.spooler.SpoolContainer")
        logger.info("Shutting down spooler...")
        self._should_exit = True

    def create_spool(self, queue, queue_settings, spool_class=None):
        if spool_class is None:
            spool_class = Spool
        spool = spool_class(queue,
                            directory=self._base,
                            in_spool=queue_settings['incoming'],
                            out_spool=queue_settings['outgoing'],
                            failed_spool=queue_settings['failure'],
                            entry_filter=queue_settings.get('filter'))
        return spool

    def spool_process(self, queue, queue_settings):
        """Run a spool in a processing loop until it should die.

        A spool process should die after it has processed a certain number of
        jobs. We check it after each call to process(). If the spool caches
        its incoming jobs list, this means it will finish processing that list
        even if it exceeds the maximum number of jobs. This lets us put some
        bound on ordering.

        """
        spool = self.create_spool(queue, queue_settings)

        def _exit(sig, frame):
            logger = logging.getLogger("sigasync.spooler.SpoolContainer")
            logger.info("Shutting down spooler %s-%s" % (queue, os.getpid()))
            spool.manager.stop(spool)
        signal.signal(signal.SIGINT, _exit)

        while not spool.manager._should_stop:
            spool.process()
            sleep(SLEEP_TIME)

        spool.cleanup()

    # Utility functions

    def _delay_filter(self, minutes):
        """Return a filter removing jobs less than _minutes_ old."""
        delta = timedelta(minutes=minutes)

        def _filter(entry):
            try:
                st = os.lstat(entry)
            except Exception:
                return False
            diff = datetime.now() - datetime.fromtimestamp(st.st_ctime)
            return diff > delta

        return _filter


class Spool(object):
    """A generic spool manager.

    This manages 3 spool files:

       in
       processing
       out

    which are used to manage the processing of an executable function

       execute
    
    You submit jobs to the spool as filenames. They become symlinks to
    your filename in the IN directory.

    When you call process() the entries in the IN directory are processed by
    moving them to the processing directory and then running execute.

    If execute completes without exception the files are moved to the outgoing
    spool.

    If execute fails then the files are moved to the failed spool.

    == Submitting jobs to the spooler ==

    You can either use the submit function to submit jobs to the
    spooler or you can mount the incoming spool on a directory and
    have the filesystem cause the submission.

    In the former case it is not possible to preserve the filename
    using the spooler alone.

    In the latter case the spooler requires that the filenames in the
    incoming have unique basenames.

    """

    def __init__(self, name,
                 manager=None,
                 directory="/tmp",
                 in_spool=None,
                 out_spool=None,
                 failed_spool=None,
                 shard_in=False,
                 shard_out=False,
                 entry_filter=None):
        """Initialize a spool, creating any necessary directories.

        WARNING: All directories used by the spooler must exist on the same
        filesystem. The default directories will normally satisfy this
        constraint, but if you specify any of in_spool, out_spool, or
        failed_spool manually you MUST ensure that they and the base directory
        of the spool are on the same filesystem.

        It is valid for all, some, or none of the directories to exist before
        __init__ is called.

        Arguments:
        name - The name of the spool. Multiple spools can have the same name
               and directory, and they will share the queue.
        manager - A class of hooks that are called throughout processing. See
                  the SpoolManager class.

        Keyword arguments:
        directory - The directory containing all spool files and directories.
        in_spool  - The directory used for incoming entries.
        out_spool - The directory finished entries are moved to.
        failed_spool - The directory failed entries are moved to.
        shard_out - currently unused
        entry_filter - A function returning boolean that is used as a filter
                       on entries

        """
        if manager is None:
            self.manager = SpoolManager()
        else:
            self.manager = manager

        self._name = name
        self._base = pathjoin(directory, name)
        self._in = in_spool or pathjoin(self._base, "in")
        self._out = out_spool or pathjoin(self._base, "out")
        self._failed = failed_spool or pathjoin(self._base, "failed")

        self._processing_base = pathjoin(self._base, "processing")
        self._entry_filter = entry_filter
        self._shard_in = shard_in
        self._shard_out = shard_out

        # Ensure the directories are there
        for p in [self._in,
                  self._out,
                  self._processing_base,
                  self._failed]:
            if not pathexists(p):
                try:
                    os.makedirs(p)
                except OSError, e:
                    if pathexists(p):
                        # Another process must have created the directory
                        pass
                    else:
                        raise e

        self.manager.created_spool(self, self._in, self._out, self._failed)

    class _LazyProcessingDescriptor(object):
        """Prevent the spooler instance from pre-creating processing dir.

        Need a special class because I want a non-data descriptor,
        unlike property()
        """
        def __get__(self, obj, type=None):
            obj._processing = tempfile.mkdtemp(dir=obj._processing_base,
                                               prefix="%d_" % os.getpid())
            obj.manager.created_processing(obj, obj._processing)
            return obj._processing

    _processing = _LazyProcessingDescriptor()

    # You must implement this.
    def execute(self, processing_entry):
        """the null executer just passes"""
        pass

    # Informational methods
    def get_out_spool(self):
        """Returns the full path of the output spool.

        This is so you can use this to mount another spooler's
        incoming spool on this spooler's output spool.
        """
        return self._out
    
    # Helper and instance methods

    def _remove_processing_dir(self):
        logger = logging.getLogger("sigasync.spooler.Spool._remove_processing_dir")
        try:
            os.rmdir(self._processing)
        except OSError, e:
            logger.warning("Failed to remove dir %s: %s" %
                    (self._processing, e))
        else:
            logger.info("Removed processing dir %s" % self._processing)

    def _move_to(self, entry, dir):
        """Move an entry to the target directory.

        If the file no longer exists this will raise an OSError. This is
        normal when moving files out of incoming if multiple processes are
        operating on the same queue.

        Returns the new path of the entry on success.

        """
        target = pathjoin(dir, basename(entry))
        os.rename(entry, target)
        return target

    def _move_to_incoming(self, entry):
        return self._move_to(entry, self._in)

    def _move_to_processing(self, entry):
        return self._move_to(entry, self._processing)

    def _move_to_failed(self, entry):
        return self._move_to(entry, self._failed)
        
    def _move_to_outgoing(self, entry):
        return self._move_to(entry, self._out)

    def _make_datum_fname(self):
        """Return a filename (based in _in) suitable for the spooler.

        This is called by submit. Please override it if you want specific
        filenames in your spooler.

        The default implementation uses the datetime, the current pid, and a
        temp string to avoid name clashes and provide ordering by submission
        time.

        """
        t = datetime.now()
        pre = "%s%06d_%s_" % (t.strftime("%Y%m%d%H%M%S"),
                              t.microsecond,
                              os.getpid())
        return tempfile.mktemp(prefix=pre, dir=self._in)

    def submit_datum(self, datum):
        """Submit the datum to the spooler without having to worry about filenames.

        This just does the creation of the file on the user's behalf.  The
        datum is written into a temporary file and the file is submitted.

        """
        (tmpfd, tmpfname) = tempfile.mkstemp()
        try:
            os.write(tmpfd, datum)
        finally:
            os.close(tmpfd)

        self.submit(tmpfname, mv=True)

    def submit(self, filename, mv=False):
        """Push the file into the spooler's queue.

        If 'mv' is set True then filename is removed from it's src
        location; if 'mv' is False then it is simply symlinked.

        """
        target_name = self._make_datum_fname()
        if mv:
            os.rename(filename, target_name)
        else:
            os.symlink(filename, target_name)

    def _incoming(self):
        """Yield an iterator over incoming file entries."""
        try:
            entries = os.listdir(self._in)
        except OSError, e:
            if e.errno == 22:
                # HFS+ sometimes returns errno 22 EINVAL from readdir
                # after renaming files, so just retry on the next loop
                return
            else:
                raise e
        entries = [pathjoin(self._in, e) for e in entries]
        entries = filter(self._entry_filter, entries)
        for entry in entries:
            yield entry

    def process(self, function=None):
        """Process the spool.

        Entries to process are captured from the inspool and tested with the
        entry_filter (supplied as a keyword argument to __init__) Only entries
        present when this method is called will be processed.

        If they pass entry_filter they are moved one by one to the processing
        spool and the 'function' is called on each.

        On success entries are moved to the outgoing spool.

        On failure entries are moved to the failure spool.

        Keyword arguments: function - function to call on the entry, by default
        it is self.execute

        """
        logger = logging.getLogger("sigasync.spooler.Spool.process")
        if function is None:
            function = self.execute

        self.manager.started_processing(self, self._in)

        for entry in self._incoming():
            if self.manager.should_stop(self):
                return
            try:
                processing_entry = self._move_to_processing(entry)
            except OSError, e:
                if e.errno == 2:
                    # '[Errno 2] No such file or directory'
                    # The file was moved, probably by another spool process
                    pass
                else:
                    raise e
            else:
                try:
                    function(processing_entry)
                except Exception, e:
                    failed_entry = self._move_to_failed(processing_entry)
                    self.manager.failed_entry(self, failed_entry)
                    logger.exception("%s failed with error: %s" % (failed_entry, e))
                else:
                    processed_entry = self._move_to_outgoing(processing_entry)
                    self.manager.processed_entry(self, processed_entry)
                    logger.debug("processed entry %s" % processed_entry)
        
        self.manager.finished_processing(self)

    def cleanup(self):
        """Run any cleanup tasks."""
        self._remove_processing_dir()


# Utility functions for running from the command line and as a daemon

def daemonize(our_home_dir='.', out_log='/dev/null', err_log='/dev/null'):
    """Robustly turn into a UNIX daemon, running in our_home_dir.
    
    Borrowed from django.utils.daemonize.
    
    """
    # First fork
    try:
        if os.fork() > 0:
            os._exit(0)     # kill off parent
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)
    os.setsid()
    os.chdir(our_home_dir)
    os.umask(022)

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

def setup_django():
    woome_dir = os.path.abspath(os.path.dirname(__file__) + "/../../woome")
    sys.path.insert(0, woome_dir)
    import config.importname
    local_config = __import__('config.%s' % config.importname.get(), {}, {}, [''])
    try:
        django_dir = local_config.DJANGO_PATH_DIR
    except AttributeError:
        django_dir = config.discover_django_location()
    if django_dir:
        sys.path.insert(1, django_dir)
        from django.core.management import setup_environ
        import settings
        setup_environ(settings)
    else:
        raise ImportError("Unable to find Django")

    import global_signals_connector

def _import_object(name):
    parts = name.split('.')
    mod_name = '.'.join(parts[:-1])
    obj_name = parts[-1]
    mod = __import__(mod_name, globals(), locals(), [obj_name])
    try:
        obj = getattr(mod, obj_name)
    except AttributeError:
        raise ImportError("cannot import name %s" % obj_name)
    return obj

if __name__ == "__main__":
    ## Django command environment config

    import os
    import sys

    def discover_django_location():
        """This is a duplicate of a fn found in config_helper.py

        It finds django in some common locations."""

        try: 
            location = os.environ["DJANGO_PATH_DIR"]
        except KeyError:
            try:
                location = os.path.join(os.environ["HOME"], "django-hg")
            except KeyError:
                pass

        if os.path.exists(location):
            return location

    # Set the path so the cron environment can find django and woome
    sys.path = [os.path.dirname(__file__) + "/../../woome", discover_django_location()] + sys.path

    # Do the standard django/cron jig
    from django.core.management import setup_environ
    import settings
    setup_environ(settings)

    def main():
        import getopt
        opts, args = getopt.gnu_getopt(sys.argv[1:], 'De:o:s:m:', ['nodjango'])
        opts = dict(opts)
        if '--nodjango' not in opts:
            setup_django()
        if '-m' in opts:
            container = _import_object(opts['-m'])()
        else:
            container = SpoolContainer()
        err_log = opts.get('-e', '/dev/null')
        out_log = opts.get('-o', '/dev/null')

        if 'start' in args[0:1]:
            if glob.glob("%s/%s*.pid" % (container._base, settings.SPOOLER_PID_BASE)):
                print "Failed to start - pid files exist"
                sys.exit(1)

            if '-D' not in opts:
                daemonize(out_log=out_log, err_log=err_log)
            container.run()

        elif 'stop' in args[0:1]:
            for f in glob.glob("%s/%s*.pid" % (container._base, settings.SPOOLER_PID_BASE)):
                with open(pathjoin(container._base, f)) as fh:
                    pid = fh.read()
                try:
                    os.kill(int(pid), signal.SIGINT)
                    # Succesfull kill so remove the pid file
                    os.remove(pathjoin(container._base, f))
                    print "Killing process %s." % pid
                except OSError, e:
                    print "Failed to kill process %s: %s" % (pid, e)
                    sys.exit(1)
        else:
            print >> sys.stdout, """usage: %s [options] start
            options:
            -D:         do not daemonize
            -e:         error log file
            -o:         stdout log file
            -s <num>:   number of seconds for each sleep loop. (ignored)
            -m:         python path of spool container to instantiate/factory method. 
            --nodjango: do no load django environment

            example: %s -m sigasync.sigasync_spooler.SigAsyncContainer -D start

            """ % (sys.argv[0], sys.argv[0])

    main()


# End
