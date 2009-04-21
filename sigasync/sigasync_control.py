import os
import subprocess
import sys

CONTROL_PY = os.path.join(os.path.dirname(sys.argv[0]), 'spool_control.py')

def startall(spools):
    for name, queues in spools.iteritems():
        if not isinstance(queues, list):
            queues = [queues]
        print >> sys.stdout, "starting '%s' spool, containing queues: %s" % (name, ", ".join(queues))
        p = subprocess.Popen([sys.executable,
                CONTROL_PY,
                '-m', 'sigasync.sigasync_spooler.get_spoolqueue',
                '-a', 'name=%s' % queues[0],
                'start'
            ], env=os.environ)
        retcode = p.wait()
        if retcode:
            print >> sys.stderr, "ERROR: couldn't start!"
            sys.exit(1)

def stopall(spools):
    for name, queues in spools.iteritems():
        if not isinstance(queues, list):
            queues = [queues]
        print >> sys.stdout, "stopping '%s' spool, containing queues: %s" % (name, ", ".join(queues))
        p = subprocess.Popen([sys.executable,
                CONTROL_PY,
                '-m', 'sigasync.sigasync_spooler.get_spoolqueue',
                '-a', 'name=%s' % queues[0],
                'stop'
            ], env=os.environ)
        retcode = p.wait()
        if retcode:
            print >> sys.stderr, "ERROR: couldn't stop!"
            sys.exit(1)

def statusall(spools):
    import re
    statuslinere = re.compile(r'^([^\s]+)\s+([0-9]+)\s+([0-9-]+)\s(.+)$')
    in_data = []
    processing_data = []
    failed_data = []
    orphans = []

    for name, queues in spools.iteritems():
        if not isinstance(queues, list):
            queues = [queues]
        p = subprocess.Popen([sys.executable,
                CONTROL_PY,
                '-m', 'sigasync.sigasync_spooler.get_spoolqueue',
                '-a', 'name=%s' % queues[0],
                '--in',
                'status'
            ], env=os.environ, stdout=subprocess.PIPE)
        out = p.communicate()[0]
        if p.returncode:
            data.append((name, out))
        crashedpids = False
        for line in out.splitlines()[1:]:
            match = statuslinere.search(line)
            if match:
                spool, jobs, age, status = match.groups()
                if spool == 'in':
                    in_data.append((name, line))
                elif spool == 'failed':
                    failed_data.append((name, line))
                else:
                    processing_data.append((name, line))
            elif line.startswith('orphaned'):
                crashedpids = True
            elif crashedpids:
                orphans.appen((name, line))
    print >> sys.stdout, "qname\t\tspool\t\tjobs\tmax age (s)\tstatus"
    for cat in [in_data, processing_data, failed_data]:
        for i in cat:
            print >> sys.stdout, "%s\t\t%s" % i
    if orphans:
        print >> sys.stdout, "\n oprhaned pid files:"
        for i in orphans:
            print >> sys.stdout, "%s\t\t%s" % i

if __name__ == '__main__':
    commands = ('startall', 'stopall', 'statusall')
    if not os.path.exists(CONTROL_PY):
        print >> sys.stderr, "couldn't find control script. looked for it here: '%s'" % CONTROL_PY
    if not sys.argv[1:2]:
        print >> sys.stderr, "please enter a command. one of %s" % ", ".join(commands)
        sys.exit(1)
    command = sys.argv[1]
    if command not in commands:
        print >> sys.stderr, "command should be on of %s" % ", ".join(commands)
        sys.exit(1)
    from spool_control import setup_environment
    from spool_control import opts_to_dict
    setup_environment()
    from django.conf import settings
    spools = opts_to_dict([(v, k) for k,v in settings.SPOOLER_QUEUE_MAPPINGS.iteritems()])
    globals()[command](spools)

