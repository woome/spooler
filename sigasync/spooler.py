#!/usr/bin/python

"""Spooler stuff
"""

from __future__ import with_statement
import os
import sys
import errno
import tempfile
import signal
import atexit
import glob
import logging
import itertools
import socket
from os import path
from time import sleep, time
from datetime import datetime, timedelta
from multiprocessing import Process
from pkg_resources import resource_stream
from configobj import ConfigObj, ConfigObjError, flatten_errors
from validate import Validator

SLEEP_TIME = 0.1  # seconds

# How frequently to check the load and adjust spool processes
ADJUST_INTERVAL = timedelta(seconds=60)

# Store version
SPOOLER_VERSION = 1
SHARD_DIR = "v%d" % (SPOOLER_VERSION,)
SHARD_SECONDS = 100
SHARD_PREFIX = "shard_"


class SpoolerError(Exception):
    """Base class for spooler errors."""
    pass

class SpoolExists(SpoolerError):
    pass

class SpoolDoesNotExist(SpoolerError):
    pass

class ConfigurationError(SpoolerError):
    pass

class FailError(SpoolerError):
    """fail the entry"""
    pass


def get_validation_errors(config, result):
    """Extract and format validation errors from configobj"""
    errors = []
    for sections, key, result in flatten_errors(config, result):
        # This really should aggregate the other way, i.e. list all
        # missing values in a particular section at once.
        secstr = '->'.join('[%s]' % sec for sec in sections)
        if key is None:
            errors.append("Missing required section %s" % secstr)
        elif result is False:
            # missing section or value
            if sections:
                errors.append("Missing required value '%s' in section %s"
                              % (key, secstr))
            else:
                errors.append("Missing required value '%s' at top level."
                              % key)
        else:
            # failed validation, result is the exception object
            errors.append("%s %s: %s" % (secstr, key, result))
    return errors


def _load_config(conf, cache=None):
    """Helper function for read_config to load files and process includes."""
    if cache is None:
        cache = dict()

    spec = resource_stream(__name__, 'configspec.ini')
    try:
        config = ConfigObj(conf, configspec=spec, file_error=True)
    except IOError, err:
        if hasattr(err, 'strerror') and err.strerror:
            message = err.strerror
        else:
            message = str(err)
        if conf not in message:
            message = "%s: %s" % (conf, message)
        raise ConfigurationError(message)
    except ConfigObjError, err:
        if hasattr(err, 'errors'):
            message = '\n'.join(str(e) for e in err.errors)
        else:
            message = str(err)
        raise ConfigurationError("Error parsing %s: %s" % (conf, message))

    # Make special values available for interpolation in the config
    if 'DEFAULT' not in config:
        config['DEFAULT'] = {}
    config['DEFAULT']['host'] = socket.gethostname()

    # includes have to happen before validation
    if 'include' in config:
        # precedence like include in a programming language
        new_config = ConfigObj(configspec=spec)
        for include in config.as_list('include'):
            if include not in cache:
                cache[include] = {}  # This prevents circular loading
                try:
                    cache[include] = _load_config(include, cache=cache)
                except ConfigurationError, err:
                    raise ConfigurationError(
                        "Error processing includes from %s: %s" % (conf, err))
            new_config.merge(cache[include])
        new_config.merge(config)
        config = new_config
    return config


def read_config(conf):
    """Pull data from config into local data structures."""
    config = _load_config(conf)
    validator = Validator()
    res = config.validate(validator, preserve_errors=True)
    if res is not True:
        raise ConfigurationError(get_validation_errors(config, res))

    # Merge default values into each queue
    defaults = config['defaults']

    def merge_defaults(section, key):
        if section[key] is None and key in defaults:
            section[key] = defaults[key]

    config['queues'].walk(merge_defaults)
    config['queues'] = make_queues(config)

    return config.dict()


def make_queues(config):
    queues = dict(config['queues'])
    # This loop modifies 'queues' and cannot use iteritems()
    for queue, qconf in queues.items():
        queue_dir = path.join(config['base_directory'], queue)
        qconf['incoming'] = path.join(queue_dir, 'in')
        qconf['outgoing'] = path.join(queue_dir, 'out')
        # Make sure retries are sane and in order
        qconf['retries'] = sorted(set(qconf['retries']))
        fail_dirs = [path.join(queue_dir, d) for d in
                    ['retry%d' % t for t in qconf['retries']] + ['failed']]
        qconf['failure'] = fail_dirs[0]
        # Start with the minimum number of processes
        qconf['nprocs'] = qconf['minprocs']
        # Initialize list for active processes
        qconf['procs'] = []

        # Set up the chain of retries
        delay_prev = 0
        for delay, in_dir, fail_dir in zip(qconf['retries'],
                                           fail_dirs, fail_dirs[1:]):
            retry_queue = queue + '_retry%d' % delay
            queues[retry_queue] = qconf.copy()
            queues[retry_queue].update({
                'incoming': in_dir,
                'failure': fail_dir,
                'minprocs': qconf['retry_minprocs'],
                'maxprocs': qconf['retry_maxprocs'],
                'nprocs': qconf['retry_minprocs'],
                'procs': [],
                'filter': delay - delay_prev,
                })
            delay_prev = delay
    return queues


class SpoolManager(object):
    """Provide hooks for a Spool."""

    def __init__(self):
        self.spool = None
        self._should_stop = False
        self.logger = logging.getLogger("sigasync.spooler.SpoolManager")

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
        self.logger.info("Created processing dir %s"
                          % processing, extra={'pid': os.getpid()})

    def submitted_entry(self, spool, entry):
        """Notify that an entry has been successfully submitted."""
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
    
    def __init__(self, manager=SpoolManager, directory=None,
                 conf='config/spooler.ini'):
        self.logger = logging.getLogger("sigasync.spooler.SpoolContainer")
        self._children = set()
        self._base = directory
        self._should_exit = False
        self._last_adjusted = datetime.now()

        self._manager = manager
        if callable(manager):
            self.manager = manager()
        else:
            self.manager = manager

        config = read_config(conf)
        if self._base is None:
            self._base = config['base_directory']
        self._pid_file = config['pid_file']
        self._queues = config['queues']

    def _start_spools(self):
        for queue, conf in self._queues.iteritems():
            if conf['enabled']:
                for n in range(conf['nprocs']):
                    self._start_spool(queue)

    def _start_spool(self, queue):
        container=self
        class DebugProcess(object):
            def __init__(self, group=None, target=None, name="", args=[], kwargs={}):
                self.target = target
                self.args = args
                self.kwargs = kwargs
                self.exitcode = -1
                self.daemon = False
                self.pid = False

            def start(self):
                pass

            def is_alive(self):
                try:
                    kwargs = self.kwargs.copy()
                    kwargs["once_only"] = True
                    self.target(*self.args, **kwargs)
                except Exception, e:
                    self.exitcode = 1
                else:
                    self.exitcode = 0

                return False
                
        process = Process(
            target=self.spool_process,
            args=(queue, self._queues[queue])
            )
        process.daemon = True
        process.start()
        self._queues[queue]['procs'].append(process)
        self._children.add(process)
        self.logger.info("Started spool process %s for %s queue."
                          % (process.pid, queue))
        return process

    def _adjust_spool(self, queue):
        qd = self._queues[queue]
        try:
            entries = len(os.listdir(qd['incoming']))
        except OSError, e:
            self.logger.error("Error opening %s: %s" % (qd['incoming'], e))
            return
        if entries > 100 and qd['nprocs'] < qd['maxprocs']:
            self.logger.info("Spawning new process for %s" % queue)
            qd['nprocs'] += 1
        elif entries < 50 and qd['nprocs'] > qd['minprocs']:
            self.logger.info("Removing process for %s" % queue)
            qd['nprocs'] -= 1

    def _write_pid(self):
        with open(self._pid_file, 'w') as f:
            f.write("%s" % os.getpid())

    def _remove_pid(self):
        try:
            os.unlink(self._pid_file)
        except Exception, e:
            self.logger.warning("Failed to remove %s - %s", self._pid_file, e)

    def run(self):
        self._start_spools()
        self._write_pid()
        atexit.register(self._remove_pid)
        signal.signal(signal.SIGINT, self.exit)

        while not self._should_exit:
            adjusted = False
            # check status of spools
            for queue, dict_ in self._queues.iteritems():
                if not dict_['enabled']:
                    continue
                procs = dict_['procs']
                # Clean out dead processes in this queue
                for p in procs[:]:
                    if not p.is_alive():
                        if p.exitcode > 0:
                            self.logger.warning("Spool %s-%s exited with code %s"
                                    % (queue, p.pid, p.exitcode))
                        elif p.exitcode < 0:
                            self.logger.warning("Spool %s-%s was killed by signal %s"
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
        self.logger.info("Shutting down child processes...")
        for p in self._children:
            try:
                os.kill(p.pid, signal.SIGINT)
            except OSError:
                # Already died
                pass
        for p in self._children:
            p.join()

    def exit(self, sig, frame):
        self.logger.info("Shutting down spooler...")
        self._should_exit = True

    def create_spool(self, queue, queue_settings, spool_class=None):
        if spool_class is None:
            spool_class = Spool
        spool = spool_class(queue,
                            manager=self._manager,
                            directory=self._base,
                            in_spool=queue_settings['incoming'],
                            out_spool=queue_settings['outgoing'],
                            failed_spool=queue_settings['failure'],
                            shard=queue_settings.get('shard'),
                            entry_filter=self._delay_filter(queue_settings.get('filter')),
                            )
        return spool

    def spool_process(self, queue, queue_settings, once_only=False):
        """Run a spool in a processing loop until it should die.

        A spool process should die after it has processed a certain number of
        jobs. We check it after each call to process(). If the spool caches
        its incoming jobs list, this means it will finish processing that list
        even if it exceeds the maximum number of jobs. This lets us put some
        bound on ordering.

        once_only = True   causes the processing loop to execute once only.
        """
        spool = self.create_spool(queue, queue_settings)

        def _exit(sig, frame):
            self.logger.info("Shutting down spooler %s-%s" % (queue, os.getpid()))
            spool.manager.stop(spool)
        signal.signal(signal.SIGINT, _exit)

        while not spool.manager._should_stop:
            spool.process()
            if once_only:
                break
            sleep(SLEEP_TIME)

        spool.cleanup()

    # Utility functions

    def _delay_filter(self, minutes):
        """Return a filter removing jobs less than _minutes_ old."""
        if minutes is None:
            return None

        delta = timedelta(minutes=minutes)

        def _filter(entry):
            """Filter out entries at less than %d minutes old by mtime."""
            try:
                st = os.lstat(entry)
            except Exception:
                return False
            diff = datetime.now() - datetime.fromtimestamp(st.st_mtime)
            return diff > delta

        _filter.__doc__ = _filter.__doc__ % minutes
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
                 manager=SpoolManager,
                 directory="/tmp",
                 in_spool=None,
                 out_spool=None,
                 failed_spool=None,
                 shard=False,
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
        shard - whether to shard the in, out, and failed directories.
        entry_filter - A function returning boolean that is used as a filter
                       on entries

        """
        self.logger = logging.getLogger("sigasync.spooler.Spool")

        if callable(manager):
            self.manager = manager()
        else:
            self.manager = manager

        self._name = name
        self._base = path.join(directory, name)
        self._in = in_spool or path.join(self._base, "in")
        self._out = out_spool or path.join(self._base, "out")
        self._failed = failed_spool or path.join(self._base, "failed")
        self._tmp = path.join(self._base, 'tmp')

        self._processing_base = path.join(self._base, "processing")
        self._entry_filter = entry_filter
        self._sharded = shard

        self.logger.debug("Initializing spool in %s with %s %s %s",
                          self._base, self._in, self._out, self._failed)
        if self._sharded:
            self.logger.debug("Spooler is sharded with interval %s seconds"
                              " using format version %s in dir %s",
                              SHARD_SECONDS, SPOOLER_VERSION, SHARD_DIR)

        dirs = [self._in,
                self._out,
                self._processing_base,
                self._failed,
                self._tmp]

        if not self._sharded:
            # check for existing shard store
            if any(path.exists(path.join(p, SHARD_DIR))
                   for p in (self._in, self._out)):
                self._sharded = True
                self.logger.info(
                    "Found sharded spool in %s, running in sharded mode",
                    self._base)

        if self._sharded:
            dirs.extend([path.join(self._in, SHARD_DIR),
                         path.join(self._out, SHARD_DIR),
                        ])

        # Ensure the directories are there
        for p in dirs:
            if not path.exists(p):
                try:
                    os.makedirs(p)
                except OSError, e:
                    if e.errno == errno.EEXIST:
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
        try:
            os.rmdir(self._processing)
        except OSError, e:
            self.logger.warning("Failed to remove dir %s: %s" %
                    (self._processing, e))
        else:
            self.logger.info("Removed processing dir %s" % self._processing)

    def _move_to(self, entry, dir):
        """Move an entry to the target directory.

        If the file no longer exists this will raise an OSError. This is
        normal when moving files out of incoming if multiple processes are
        operating on the same queue.

        Returns the new path of the entry on success.

        """
        target = path.join(dir, path.basename(entry))
        os.rename(entry, target)
        return target

    def _move_to_sharded(self, entry, target_dir):
        # Two tries, in case the current shard changes while we're
        # submitting.
        for attempt in [0, 1]:
            # get shard dir and create if necessary
            shard = self._get_current_shard(target_dir)
            target = path.join(shard, path.basename(entry))
            try:
                os.makedirs(shard)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            try:
                os.rename(entry, target)
            except OSError, e:
                if e.errno == errno.ENOENT and attempt < 1:
                    # Target directory disappeared, try again
                    continue
                else:
                    raise
            return target

    def _move_to_incoming(self, entry):
        if self._sharded:
            return self._move_to_sharded(entry, self._in)
        else:
            return self._move_to(entry, self._in)

    def _move_to_processing(self, entry):
        return self._move_to(entry, self._processing)

    def _move_to_failed(self, entry):
        if self._sharded:
            return self._move_to_sharded(entry, self._failed)
        else:
            return self._move_to(entry, self._failed)

    def _move_to_outgoing(self, entry):
        if self._sharded:
            return self._move_to_sharded(entry, self._out)
        else:
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

    def _submit_datum(self, datum):
        """Submit the datum to the spooler without having to worry about filenames.

        This just does the creation of the file on the user's behalf.  The
        datum is written into a temporary file and the file is submitted.

        """
        (tmpfd, tmpfname) = tempfile.mkstemp(dir=self._tmp)
        try:
            os.write(tmpfd, datum)
        finally:
            os.close(tmpfd)

        if self._sharded:
            self._submit_file_sharded(tmpfname, mv=True)
        else:
            self._submit_file(tmpfname, mv=True)

    def _submit_file(self, filename, mv=False):
        """Push the file into the spooler's queue.

        If 'mv' is set True then filename is removed from it's src
        location; if 'mv' is False then it is simply symlinked.

        """
        target_name = self._make_datum_fname()
        if mv:
            os.rename(filename, target_name)
        else:
            os.symlink(filename, target_name)
        self.manager.submitted_entry(self, target_name)

    def _submit_file_sharded(self, filename, mv=False):
        t = datetime.now()
        pre = "%s%06d_%s_" % (t.strftime("%Y%m%d%H%M%S"),
                              t.microsecond,
                              os.getpid())

        # Two tries, in case the current shard changes while we're
        # submitting.
        for attempt in [0, 1]:
            # get shard dir and create if necessary
            shard = self._get_current_shard(self._in)
            try:
                os.makedirs(shard)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            # mktemp seems not to care if the directory exists
            target = tempfile.mktemp(prefix=pre, dir=shard)
            try:
                if mv:
                    os.rename(filename, target)
                else:
                    os.symlink(filename, target)
            except OSError, e:
                if e.errno == errno.ENOENT and attempt < 1:
                    # Target directory disappeared, try again
                    continue
                else:
                    raise
            self.manager.submitted_entry(self, target)
            return

    def _incoming(self):
        """Yield an iterator over incoming file entries."""
        try:
            entries = os.listdir(self._in)
        except OSError, e:
            if e.errno == errno.EINVAL:
                # HFS+ sometimes returns EINVAL from readdir
                # after renaming files, so just retry on the next loop
                return
            else:
                raise e
        entries = [path.join(self._in, e) for e in entries]
        entries = filter(self._entry_filter, entries)
        for entry in entries:
            if not entry.startswith(path.join(self._in, SHARD_DIR)):
                yield entry

    def _incoming_shard(self):
        """Like _incoming but gives files from the first non-empty shard dir"""
        for shard in self._shards(self._in):
            entries = sorted(os.listdir(shard))
            if entries:
                return (path.join(shard, e) for e in entries)
            elif shard < self._get_current_shard(self._in):
                # Shard appears empty and older than the current shard
                # Try to remove it if it hasn't been modified recently
                try:
                    st = os.stat(shard)
                    if time() - st.st_mtime > SHARD_SECONDS:
                        try:
                            os.rmdir(shard)
                        except OSError, err:
                            if err.errno != errno.ENOTEMPTY:
                                raise
                except OSError, err:
                    if err.errno != errno.ENOENT:
                        raise
        return []

    def _shards(self, base):
        return sorted(glob.glob(path.join(base, SHARD_DIR, SHARD_PREFIX+'*')))

    def _get_current_shard(self, base):
        return path.join(base, SHARD_DIR, SHARD_PREFIX +
                         str(int(time() / SHARD_SECONDS) * SHARD_SECONDS))

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
        logger = self.logger
        if function is None:
            function = self.execute

        self.manager.started_processing(self, self._in)

        incoming = self._incoming()
        if self._sharded:
            incoming = itertools.chain(incoming, self._incoming_shard())

        for entry in incoming:
            if self.manager.should_stop(self):
                return
            try:
                processing_entry = self._move_to_processing(entry)
            except OSError, e:
                if e.errno == errno.ENOENT:
                    # The file was moved, probably by another spool process
                    pass
                else:
                    raise e
            else:
                try:
                    # Touch the file so we have a consistent way to measure
                    # time for retries
                    os.utime(processing_entry, None)
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

def run(conf):
    import getopt
    opts, args = getopt.gnu_getopt(sys.argv[1:], 'c:De:o:s:m:')
    opts = dict(opts)
    if '-c' in opts:
        conf_file = opts['-c']
    else:
        conf_file = conf

    if '-m' in opts:
        container_class = _import_object(opts['-m'])
    else:
        container_class = SpoolContainer
    container = container_class(conf=conf_file)
    err_log = opts.get('-e', '/dev/null')
    out_log = opts.get('-o', '/dev/null')

    if 'start' in args[0:1]:
        if path.exists(container._pid_file):
            print "Failed to start - pid file exists at %s" % container._pid_file
            sys.exit(1)

        if '-D' not in opts:
            daemonize(out_log=out_log, err_log=err_log)
        container.run()

    elif 'stop' in args[0:1]:
        with open(container._pid_file) as fh:
            pid = fh.read()
        try:
            os.kill(int(pid), signal.SIGINT)
            # Succesfull kill so remove the pid file
            os.remove(container._pid_file)
            print "Killing process %s." % pid
        except OSError, e:
            print "Failed to kill process %s: %s" % (pid, e)
            sys.exit(1)
    else:
        print >> sys.stdout, """usage: %s [options] start
        options:
        -c <file>:  load spooler configuration from <file>
        -D:         do not daemonize
        -e:         error log file
        -o:         stdout log file
        -s <num>:   number of seconds for each sleep loop. (ignored)
        -m:         python path of spool container to instantiate/factory method.

        example: %s -m sigasync.sigasync_spooler.SigAsyncContainer -D start

        """ % (sys.argv[0], sys.argv[0])

# End
