#!/usr/bin/python

"""Spooler stuff
"""

import tempfile
import os
import os.path
import commands
import sys

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
        if os.path.exists(base):
            raise SpoolExists(base)
        else:
            os.makedirs(base)

    create = staticmethod(create)

    def __init__(self, name, directory="/tmp", in_spool=None):
        """Create a reference to a spooler.

        If the in_spool is specified it is used as the input spooler.
        WARNING!!! if you specify this you MUST ensure that the input
        spooler is ON THE SAME FILESYSTEM as the main spooler working files.
        In other words that:

           filesystem(directory) == filesystem(in_spool)

        If the spooler detects that the in_spool does not exist then
        the default is used.

        Arguments:
        name is the name of the spooler, if it exists it will be reused.

        Keyword arguments:
        directory - the directory to make the spooler working directorys on.
        in_spool  - the directory to use as the in_spool, if None then a default is used.
        """

        # We shouldn't actually create a new spool every time
        # we want a fixed spool, the processing dir is the bit that changes
        #self._base = tempfile.mkdtemp(prefix=directory, dir=name)
        self._base = os.path.join(directory, name)
        if not os.path.exists(self._base):
            raise SpoolDoesNotExist(self._base)

        if in_spool and os.path.exists(in_spool):
            self._in = in_spool
        else:
            self._in = os.path.join(self._base, "in")
            if not os.path.exists(self._in):
                os.makedirs(self._in)

        self._out = os.path.join(self._base, "out")
        if not os.path.exists(self._out):
            os.makedirs(self._out)

        self._processing_base = os.path.join(self._base, 'processing')
        if not os.path.isdir(self._processing_base):
            os.makedirs(self._processing_base)

    class _LazyProcessingDescriptor(object):
        """this is here as a little hack to prevent the spooler instance from creating
        a processing directory automatically at instantiation.

        need a special class because i want a non-data descriptor, unlike property()

        """
        def __get__(self, obj, type=None):
            obj._processing = tempfile.mkdtemp(dir=obj._processing_base)
            return obj._processing
    _processing = _LazyProcessingDescriptor()


    # You must implement this.
    def execute(self, processing_entry):
        """the null executer just passes"""
        pass

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
        os.rename(entry, os.path.join(self._in, os.path.basename(entry)))

    def _move_to_processing(self, entry):
        # This can fail because the incoming has gone away
        # (maybe it was processed by another job)
        os.rename(entry, os.path.join(self._processing, os.path.basename(entry)))
        return os.path.join(self._processing, os.path.basename(entry))
        
    def _move_to_outgoing(self, entry):
        os.rename(entry, os.path.join(self._out, os.path.basename(entry)))

    def submit_datum(self, datum):
        """Submit the specified datum to the spooler without having to worry about filenames.

        This just does the creation of the file on the user's behalf.

        The datum is simply written into the created file.

        Warning
        This method is not atomic and so is slightly dangerous.
        """
        
        ### FIXME:: possibly this should create a file somewhere else and then call submit on it.
        target_name = os.path.join(self._in, tempfile.mktemp(dir=self._in))
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

        target_name = os.path.join(self._in, tempfile.mktemp(dir=self._in))
        if mv:
            os.rename(filename, target_name)
        else:
            os.symlink(filename, target_name)

    def process(self, 
                function=None,
                entryfilter=lambda entry: entry):
        """Process the spool.

        Entrys to process are captured from the inspool and tested
        with the 'entryfilter'.

        If they pass the 'entryfilter' they are moved one by one to
        the processing spool and the 'function' is called on each.

        On success entrys are moved to the outgoing spool.

        On failure entrys are moved back to incomming.

        Keyword arguments:

        function    - function to call on the entry, by default it is self.execute
        entryfilter - function to test each entry, by default lambda x: x
        """

        if function == None:
            function = self.execute

        for entry in filter(entryfilter,
                            [os.path.join(self._in, direntry) for direntry in os.listdir(self._in)]):
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
                    print >>sys.stderr, "encountered error: %s" % e
                    self._move_to_incomming(processing_entry)
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
        dest_fd = open(os.path.join(destination, os.path.basename(entry)), "w")
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
        s.submit(os.path.join(os.getcwd(), submit_filename))
        # Process with the defaults
        s.process()
        # Now assert it's gone to the output
        dc = [os.path.join(s.get_out_spool(), direntry) for direntry in os.listdir(s.get_out_spool())]
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

        pdb.set_trace()

        spool_name = "test2"
        try:
            Spool.create(spool_name)
        except SpoolExists:
            pass

        # Make a directory to submit
        spooler_in_dir = "/tmp/test2_spooler_in"
        os.makedirs(spooler_in_dir)

        # Create the pre-existing file which the spooler will process
        submit_filename = os.path.join(spooler_in_dir, "aaaa_spooler_test_file")
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
        dc = [os.path.join(s.get_out_spool(), direntry) for direntry in os.listdir(s.get_out_spool())]
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
