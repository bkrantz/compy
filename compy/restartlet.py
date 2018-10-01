#!/usr/bin/env python

from gevent.pool import Pool
from gevent import sleep
from gevent.greenlet import Greenlet

class RestartPool(Pool):
    """
    A simple extension of the gevent Pool class that, by default, adds a callback to all greenlet spawns that will
    restart the greenlet in the event of an exceptional exit

    Additional parameters:
        - restart       (bool):     (Default: True) This flag is designed to be used as a signifier to a linked exception callback
                                        as to whether or not the callback should spawn a new greenlet to replace the one that crashed.
                                        If true, the callback should try to spawn a new greenlet. If false, the greenlet function stays dead
        - sleep_interval (int):     (Default: 0) The amount of time to sleep between greenlet restarts. A value of 0 may not be appropriate, as it could bring
                                        an application or process to its knees
        - logger      (Logger):     (Default: None) This should be an instance of a logger that emulates the "warn, error, info" python log levels. If one is
                                        provided, the greenlet restart will be directed to that log file as well as STDOUT

    """

    def __init__(self, sleep_interval=0, restart=True, logger=None, *args, **kwargs):
        self.sleep_interval = sleep_interval
        self.restart = restart
        self.logger = logger
        super(RestartPool, self).__init__(greenlet_class=RestartableGreenlet, *args, **kwargs)

    def spawn(self, run, *args, **kwargs):
        """
        An override of the gevent.pool.Pool.spawn(). Exactly the same but uses the kwarg 'restart' to determine if this greenlet restarts.
        If not provided, it defaults to the pool default value
        """
        restart = kwargs.pop('restart', None) or self.restart
        new_greenlet = super(RestartPool, self).spawn(run, restart, *args, **kwargs)
        new_greenlet.link_exception(self._greenlet_monitor)
        return new_greenlet

    def _greenlet_monitor(self, exceptional_greenlet):
        function_name = exceptional_greenlet.run_target.__name__
        error_message = exceptional_greenlet.exception.message

        if self.logger:
            self.logger.error("Greenlet '{function_name}' has encountered an uncaught error: {err}".format(function_name=function_name,
                                                                                                         err=error_message))
        if exceptional_greenlet.restart:
            sleep(self.sleep_interval)
            if self.logger:
                self.logger.info("Restarting greenlet '{function_name}'...".format(function_name=function_name))

            self.spawn(exceptional_greenlet.run_target, *exceptional_greenlet.run_target_args, **exceptional_greenlet.run_target_kwargs)
        else:
            if self.logger:
                self.logger.info("Greenlet '{function_name}' does not desire a restart.".format(function_name=function_name))
            else:
                raise exceptional_greenlet.exception


class RestartableGreenlet(Greenlet):
    """
    A simple extension of the greenlet class that keeps track of the the run target
    and targets args/kwargs, as the base Greenlet deletes these values at the end of 'run'
    We can then use these properties in the callback to start a new greenlet to replace the failed greenlet automatically,
    if we so desire.

    Additional parameters:
        - restart (bool):   (Default: True) This flag is designed to be used as a signifier to a linked exception callback
                                        as to whether or not the callback should spawn a new greenlet to replace the one that crashed.
                                        If true, the callback should try to spawn a new greenlet. If false, the greenlet function stays dead

    """

    def __init__(self, run=None, restart=True, *args, **kwargs):
        self.run_target = run
        self.run_target_args = args
        self.run_target_kwargs = kwargs
        self.restart = restart
        Greenlet.__init__(self, run, *args, **kwargs)