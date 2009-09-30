#!/usr/bin/python

"""Spooler stuff
"""

import tempfile
from datetime import datetime
import os
import os.path
from os.path import join as pathjoin
from os.path import exists as pathexists
from os.path import basename
from os.path import dirname
from glob import iglob 
from stat import *
import commands
import sys
import logging
import pdb

class SpoolExists(Exception):
    pass

class SpoolDoesNotExist(Exception):
    pass

class FailError(Exception):
    """fail the entry"""
    pass


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

    When you call process() the entrys in the IN directory are
    processed by moving them to the processing directory and then running execute.

    If execute completes without exception the files are moved to the outgoing spool.

    If execute fails then the files are moved back to the incomming spool.

    == Submitting jobs to the spooler ==

    You can either use the submit function to submit jobs to the
    spooler or you can mount the incomming spool on a directory and
    have the filesystem cause the submission.

    In the former case it is not possible to preserve the filename
    using the spooler alone.

    In the latter case the spooler requires that the filenames in the
    incomming have unique basenames.

    Directorys can be submitted as well as files.
    """

    def create(name, directory="/tmp"):
        base = os.path.join(directory, name)
        if pathexists(base):
            raise SpoolExists(base)
        else:
            os.makedirs(base)

    create = staticmethod(create)

    def __init__(self, name, 
                 directory="/tmp", 
                 in_spool=None,
                 shard_out=False,
                 entry_filter=lambda entry:entry):
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
        in_spool  - the directory to use as the in_spool, if None then a default is used.
        shard_out - causes the output directories to be sharded
        """

        self._base = pathjoin(directory, name)
        self._out = pathjoin(self._base, "out")
        self._processing_base = pathjoin(self._base, "processing")
        self._failed = pathjoin(self._base, "failed")
        self._entryfilter = entry_filter
        self._shard_out = shard_out
        if in_spool:
            self._in = in_spool
        else:
            self._in = pathjoin(self._base, "in")

        # Ensure the directories are there
        for p in [self._in,
                  self._out
                  self._processing_base,
                  self._failed]
            if pathexists(p):
                os.makedirs(p)

    class _LazyProcessingDescriptor(object):
        """Prevent the spooler instance from ore-creating processing dir.

        Need a special class because i want a non-data descriptor,
        unlike property()
        """
        def __get__(self, obj, type=None):
            obj._processing = tempfile.mkdtemp(dir=obj._processing_base)
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
        incomming spool on this spooler's output spool.
        """
        return self._out
    
    # Helper and instance methods

    def destroy(self):
        commands.getstatus("rm -rf %s" % self._base)

    def _move_to_incomming(self, entry):
        os.rename(entry, pathjoin(self._in, basename(entry)))

    def _move_to_processing(self, entry):
        # This can fail because the incoming has gone away
        # (maybe it was processed by another job)
        os.rename(entry, pathjoin(self._processing, basename(entry)))
        return pathjoin(self._processing, basename(entry))

    def _move_to_failed(self, entry):
        os.rename(entry, pathjoin(self._failed, basename(entry)))
        return pathjoin(self._failed, basename(entry))
        
    def _move_to_outgoing(self, entry):
        os.rename(entry, pathjoin(self._out, basename(entry)))

    def _make_datum_fname(self):
        """Return a filename (based in _in) suitable for the spooler.

        This is called by submit_datum. Please override it if you want
        specific filenames in your spooler.

        The default implementation uses a temp part and a date time
        representation.
        """
        t = datetime.now()
        return "%s__%s" % (
            tempfile.mktemp(dir=self._in),
            "%s%d" % (t.strftime("%Y%m%d%H%M%S"), t.microsecond)
            )

    def submit_datum(self, datum):
        """Submit the specified datum to the spooler without having to worry about filenames.

        This just does the creation of the file on the user's behalf.

        The datum is simply written into the created file.

        Warning
        This method is not atomic and so is slightly dangerous.
        """
        
        ### FIXME:: possibly this should create a file 
        ### somewhere else and then call submit on it.
        target_name = pathjoin(self._in, self._make_datum_fname())
        fd = None
        try:
            fd = open(target_name, "w")
            fd.write(datum)
        finally:
            if fd:
                try:
                    fd.close()
                except Exception, e:
                    print >>sys.stderr, "couldn't close the file on submit_datum"

    def submit(self, filename, mv=False):
        """Push the file into the spooler for being operated on.

        If 'mv' is set True then filename is removed from it's src
        location; if 'mv' is False then it is simply symlinked.
        """

        target_name = pathjoin(self._in, tempfile.mktemp(dir=self._in))
        if mv:
            os.rename(filename, target_name)
        else:
            os.symlink(filename, target_name)

    def _incoming(self):
        entries = iglob(pathjoin(self._in, "*"))
        count = 0
        while True:
            try:
                if count > 100:
                    raise StopIteration()
                entry = entries.next()
            except StopIteration:
                # Make a new one
                entries = iglob(pathjoin(self._in, "*"))
            else:
                count += 1
                yield entry

    def process(self):
        """Process the spool.

        Entrys to process are captured from the inspool and tested
        with the 'entryfilter'.

        If they pass the 'entryfilter' they are moved one by one to
        the processing spool and the 'function' is called on each.

        On success entrys are moved to the outgoing spool.

        On failure entrys are moved to the failure spool.

        Keyword arguments:

        function    - function to call on the entry, by default it is self.execute
        entryfilter - function to test each entry, by default lambda x: x
        """
        logger = logging.getLogger("Spool.process")
        if function == None:
            function = self.execute

        # change
        # this listdir needs to be changed so that it can run concurrently
        for entry in self._incoming():
#[pathjoin(self._in, direntry) \
                     #     for direntry in os.listdir(self._in) \
                     #     if entryfilter(direntry)]):
            try:
                processing_entry = self._move_to_processing(entry)
            except Exception, e:
                # Maybe the incoming entry was moved away by another job?
                print >>sys.stderr, "processing %s failed because it was gone" % entry
            else:
                # The entry was moved to our processing dir so try and execute the job
                try:
                    function(processing_entry)
                except Exception, e:
                    logger.error("failed because %s" % str(e))
                    self._move_to_failed(processing_entry)
                else:
                    self._move_to_outgoing(processing_entry)


def make_cp_fn(destination):
    """Make a process function that can be used to copy files to destination.

    For example:

    s=Spool("copier")
    s.submit(filename1)
    s.submit(filename2)
    s.submit(filename3)
    s.process(make_cp_fn("/home/nferrier/dest"))
    """

    def fn(entry):
        src_fd = open(entry)
        dest_fd = open(pathjoin(destination, basename(entry)), "w")
        data_buf = src_fd.read(5000)
        while data_buf != "":
            dest_fd.write(data_buf)
            data_buf = src_fd.read(5000)
        src_fd.close()
        dest_fd.close()
    return fn


import unittest

class SpoolerTest(unittest.TestCase):
    
    def test_standard_submit(self):
        """This tests the submit function of the spooler.

        The test make a file and then submits it to a spooler. The spooler
        processes the file with the default (no-op) processing.
        """

        try:
            Spool.create("test1")
        except SpoolExists:
            try:
                os.makedirs("dest")
            except:
                pass

        # Make a file to submit
        submit_filename = "/tmp/aaaa_spooler_test_file"
        try:
            fd = open(submit_filename, "w")
        except Exception, e:
            assert False, "test1: the submit file %s could not be created?" % submit_filename
        else:
            print >>fd, "hello!"
        finally:
            fd.close()

        # Make a spool
        s=Spool("test")
        # Submit the pre-existing file to it
        s.submit(pathjoin(os.getcwd(), submit_filename))
        # Process with the defaults
        s.process()
        # Now assert it's gone to the output
        dc = [pathjoin(s.get_out_spool(), direntry) for direntry in os.listdir(s.get_out_spool())]
        try:
            fd = open(dc[0])
            content = fd.read()
        except Exception:
            assert False, "test1: the processed file didn't arrive"
        else:
            assert content == "hello!\n" ## we print it, so it has a newline on the end
        finally:
            commands.getstatus("rm -rf %s" % submit_filename)
            fd.close()


    def test2():
        """Test the mounted spooler
        """


        spool_name = "test2"
        try:
            Spool.create(spool_name)
        except SpoolExists:
            pass

        # Make a directory to submit
        spooler_in_dir = "/tmp/test2_spooler_in"
        os.makedirs(spooler_in_dir)

        # Create the pre-existing file which the spooler will process
        submit_filename = pathjoin(spooler_in_dir, "aaaa_spooler_test_file")
        try:
            fd = open(submit_filename, "w")
        except Exception, e:
            assert False, "test1: the submit file %s could not be created?" % submit_filename
        else:
            print >>fd, "hello!"
        finally:
            fd.close()

        # Make a spool
        s=Spool(spool_name, in_spool=spooler_in_dir)
        # Process with the defaults
        s.process()
        # Now assert it's gone to the output
        dc = [pathjoin(s.get_out_spool(), direntry) for direntry in os.listdir(s.get_out_spool())]
        try:
            fd = open(dc[0])
            content = fd.read()
        except Exception:
            assert False, "test1: the processed file didn't arrive"
        else:
            assert content == "hello!\n" ## we print it, so it has a newline on the end
        finally:
            commands.getstatus("rm -rf %s" % spooler_in_dir)
            fd.close()



# Main program        
#if __name__ == "__main__":
#    if "test" in sys.argv:
#        #test1()
#        #test2()
#        pass

# End
