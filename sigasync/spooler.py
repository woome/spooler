#!/usr/bin/python

"""Spooler stuff
"""

from __future__ import with_statement
import tempfile
from datetime import datetime
import os
import os.path
from os.path import join as pathjoin
from os.path import exists as pathexists
from os.path import basename
from os.path import dirname
from glob import iglob 
from time import sleep
import signal
import atexit
import commands
import sys
import logging
import pdb
from itertools import repeat
from multiprocessing import Process

SLEEP_TIME = 0.1  # seconds

class SpoolExists(Exception):
    pass

class SpoolDoesNotExist(Exception):
    pass

class FailError(Exception):
    """fail the entry"""
    pass

class SpoolManager(object):
    """Provide hooks to manage the lifecycle of a Spool."""

    def __init__(self):
        self.spool = None
        self._should_stop = False

    def start_spool(self, queue):
        pass

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
    
    def __init__(self, manager, directory=None):
        self._children = set()
        self._base = directory
        self._should_exit = False

        if isinstance(manager, type):
            self.manager = manager()
        else:
            self.manager = manager

        self._read_config()
        self._start_spools()
        signal.signal(signal.SIGINT, self.exit)
        self.run()

    # TODO: Factor out Django-specific config
    def _read_config(self):
        """Pull data from config into local data structures."""
        from django.conf import settings
        # Allow base directory to be overridden (useful for testing)
        if self._base is None:
            self._base = settings.SPOOLER_DIRECTORY
        print "Setting up in directory: %s" % self._base
        # The keys are logical queues, the values are real queues
        queues = set(settings.SPOOLER_QUEUE_MAPPINGS.values())
        qdict = {}
        for queue in queues:
            queue_dir = pathjoin(self._base, queue)
            in_, out, retry, fail = [pathjoin(queue_dir, d) for d in
                                     ['in', 'out', 'retry', 'failed']]
            qdict[queue] = {'incoming': in_,
                            'outgoing': out,
                            'failure': retry,
                            'nprocs': 3, # TODO get from config
                            'procs': []}
            qdict[queue+'_retry'] = {'incoming': retry,
                                     'outgoing': out,
                                     'failure': fail,
                                     'nprocs': 1,
                                     'procs': []}
        self._queues = qdict

    def _start_spools(self):
        for queue, conf in self._queues.iteritems():
            for n in range(conf['nprocs']):
                self._start_spool(queue)

    def _start_spool(self, queue):
        process = Process(target=self.spool_process,
                          args=(queue, self._queues[queue], Spool))
        process.daemon = True
        process.start()
        self._queues[queue]['procs'].append(process)
        self._children.add(process)
        return process

    def run(self):
        while not self._should_exit:
            # check status of spools
            for queue, dict_ in self._queues.iteritems():
                procs = dict_['procs']
                for p in procs[:]:
                    if not p.is_alive():
                        if p.exitcode != 0:
                            print "Exited with code %s" % p.exitcode
                        procs.remove(p)
                        self._children.remove(p)
                        if not self._should_exit:
                            self._start_spool(queue)
            sleep(SLEEP_TIME)

        # Clean up
        print "Shutting down child processes..."
        for p in self._children:
            os.kill(p.pid, signal.SIGINT)
        for p in self._children:
            p.join()

    def exit(self, sig, frame):
        print "Shutting down spooler..."
        self._should_exit = True

    def spool_process(self, queue, queue_settings, spool_class):
        """Run a spool in a processing loop until it should die.

        A spool process should die after it has processed a certain number of
        jobs. We check it after each call to process(). If the spool caches
        its incoming jobs list, this means it will finish processing that list
        even if it exceeds the maximum number of jobs. This lets us put some
        bound on ordering.

        """
        spool = spool_class(queue,
                            directory=self._base,
                            in_spool=queue_settings['incoming'],
                            out_spool=queue_settings['outgoing'],
                            failed_spool=queue_settings['failure'])

        def _exit(sig, frame):
            print "Shutting down spooler %s-%s" % (queue, os.getpid())
            spool._should_exit = True
        signal.signal(signal.SIGINT, _exit)

        while not spool._should_exit:
            spool.process()
            sleep(SLEEP_TIME)
        #print "Exiting after %s jobs" % spool._processed_count

    def test_spooler(self, queue):
        self.spool_process(queue, Spool, self.basedir, 1)


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

    When you call process() the entries in the IN directory are
    processed by moving them to the processing directory and then running execute.

    If execute completes without exception the files are moved to the outgoing spool.

    If execute fails then the files are moved back to the incoming spool.

    == Submitting jobs to the spooler ==

    You can either use the submit function to submit jobs to the
    spooler or you can mount the incoming spool on a directory and
    have the filesystem cause the submission.

    In the former case it is not possible to preserve the filename
    using the spooler alone.

    In the latter case the spooler requires that the filenames in the
    incoming have unique basenames.

    Directories can be submitted as well as files.
    """

    def __init__(self, name, manager
                 directory="/tmp", 
                 in_spool=None,
                 out_spool=None,
                 failed_spool=None,
                 shard_in=False,
                 shard_out=False,
                 entry_filter=None):
        """Create a reference to a spooler.

        If the in_spool is specified it is used as the input spooler.
        WARNING!!! if you specify this you MUST ensure that the input
        spooler is ON THE SAME FILESYSTEM as the main spooler working files.
        In other words that:

           filesystem(directory) == filesystem(in_spool)

        If the spooler detects that the in_spool does not exist then
        the default is used.

        The spooler can also auto shard the output directory so that
        completed files are stored into a directory structure like:

           spoolerhome/output/YYYYMMDDHH

        This is most useful when the spooler is the last in a chain
        and the files are meerly notifications of success.

        Arguments:
        name is the name of the spooler, if it exists it will be reused.

        Keyword arguments:
        directory - the directory to make the spooler working directorys on.
        in_spool  - the directory to use as the in_spool, if None then a
                    default is used.
        shard_out - causes the output directories to be sharded

        """
        self.manager = manager

        self._base = pathjoin(directory, name)
        self._in = in_spool or pathjoin(self._base, "in")
        self._out = out_spool or pathjoin(self._base, "out")
        self._failed = failed_spool or pathjoin(self._base, "failed")

        self._processing_base = pathjoin(self._base, "processing")
        self._entry_filter = entry_filter
        self._shard_in = shard_in
        self._shard_out = shard_out

        self._processed_count = 0
        self._should_exit = False

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
            obj._processing = tempfile.mkdtemp(dir=obj._processing_base)
            atexit.register(obj._remove_processing_dir)
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
            logging.getLogger("Spool._remove_processing_dir").warning(
                    "Failed to remove dir %s: %s" % (self._processing, e))

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
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(datum)

        self.submit(f.name, mv=True)

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
        logger = logging.getLogger("Spool.process")
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
                    logger.error("failed with error: %s" % e)
                    failed_entry = self._move_to_failed(processing_entry)
                    self.manager.failed_entry(self, failed_entry)
                else:
                    processed_entry = self._move_to_outgoing(processing_entry)
                    self.manager.processed_entry(self, processed_entry)
                    self._call_hook('processed_entry')
        
        self.manager.finished_processing(self)


    # Functions for using hooks

    def _call_hook(self, event, *args, **kwargs):
        try:
            hooks = self._hooks[event]
        except (AttributeError, KeyError):
            pass
        else:
            for f in self._hooks[event]:
                try:
                    f(self, *args, **kwargs)
                except Exception:
                    # TODO: configurable failure behavior
                    raise

    def register_hook(self, event, function):
        try:
            self._hooks[event].append(function)
        except KeyError:
            self._hooks[event] = [function]
        except AttributeError:
            self._hooks = {event: [function]}

# End
