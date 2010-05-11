from django.db.backends import signals
from sigasync import sigasync_spooler

signals.post_managed_commit.connect(sigasync_spooler.handle_commit)
signals.post_managed_rollback.connect(sigasync_spooler.handle_rollback)
