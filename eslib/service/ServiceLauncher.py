#!/usr/bin/env python
# -*- coding: utf-8 -*-

# The purpose of this service is to launch and kill local services as requested by the service manager.
# Note: This service needs not enter "processing" mode, as there is no document processing or timer.

from eslib.service import HttpService, status
import datetime, os, signal, subprocess, sys


class ServiceLauncher(HttpService):

    def __init__(self, **kwargs):
        super(ServiceLauncher, self).__init__(**kwargs)

        self.config.set_default(
            management_endpoint     = "localhost:5000", # for this service...
            service_runner          = None,
            service_dir             = None,
        )

        self.add_route(self._mgmt_service_launc     , "POST|PUT", "/launch"     , None)
        self.add_route(self._mgmt_service_kill      , "DELETE"  , "/kill"       , None)


    def on_configure(self, credentials, config, global_config):
        self.config.set(
            management_endpoint     = config.get("management_endpoint") or self.config.management_endpoint,
            service_runner          = config.get("service_runner"),
            service_dir             = config.get("service_dir"),
        )

    #region Service interface commands

    def _mgmt_service_launch(self, request_handler, payload, **kwargs):
        id = payload.get("id") or []
        self.log.debug("called: launch service '%s'" % id)
        return self._run_services(
            id,
            payload.get("config_key"),
            payload["endpoint"],
            payload["manager_endpoint"],
            payload.get("start") or False
        )

    def _mgmt_service_kill(self, request_handler, payload, **kwargs):
        id = int(payload.get("id"))
        pid = int(payload.get("pid"))
        self.log.debug("called: kill service '%s', pid '%s'" % (id, pid))
        return self._kill_services(
            id,
            pid,
            payload.get("force") or False
        )

    #endregion Service interface commands

    #region Service interface helpers

    def _launch_service(self, id, config_key, addr, manager_endpoint, start):
        #runner = "/Users/htb/git/elasticsearch-eslib/bin/es-run"
        #run_dir = "/Users/htb/git/customer-nets/services"

        # This is the normal case, that it was started from es-run:
        runner = sys.argv[0]
        run_dir = os.path.normpath(os.path.join(os.getcwd(), "../.."))
        # But there may be reasons to override this, especially if NOT run by es-run:
        if self.config.service_runner:
            runner = self.config.service_runner
        if self.config.service_dir:
            run_dir = self.config.service_dir

        # print "***RUNNER=", runner
        # print "***RUN_DIR=", run_dir
        # print "***CONFIG_FILE=", self.config_file
        # print "***CONFIG_KEY=", service.config_key

        self.log.debug("Launching service '%s' at '%s'." % (id, addr))

        args = [
            sys.executable,  # Same python that is running this
            runner,
            "-d", run_dir,
            id,
            "-m", manager_endpoint,
            "-e", addr,
            "--daemon"  # Needed for logging to directories anyway..
        ]
        if start:
            args.append("--start")
        if self.config_file:
            args.extend(["-f", self.config_file])
        if config_key:
            args.extend(["-c", config_key])

        p = None
        try:
            p = subprocess.Popen(
                args,
                stdout = subprocess.PIPE,
                stderr = subprocess.PIPE
            )
            # TODO: Grab stdout/stderr
        except Exception as e:
            self.log.exception("Failed to launch service '%s' at '%s'." % (id, addr))
            return {
                "pid": 0,
                "error": "Failed to launch service '%s' at '%s': %s: %s" % (id, addr, e.__class__.__name__, e)
            }

        msg = "Service '%s' launched at '%s' with pid=%d." % (id, addr, p.pid)
        self.log.status(msg)

        return {"pid": p.pid, "message": msg}

    def _kill_services(self, id, pid, force):
        # This is only for extreme cases.
        # Normally, a shutdown should do the trick.

        dead = False

        try:
            if force:
                self.log.debug("Forcefully kill service '%s', pid=%d." % (id, pid))
            else:
                self.log.debug("Attempting to kill service '%s', pid=%d." % (id, pid))
            os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
        except OSError as e:
            self.log.debug("Killing service '%s', pid=%d, failed. errno=" % (id, pid, e.errno))
            if e.errno == 3:  # does not exist
                dead = True  # Although kill failed, consider it dead (because it does no longer exist)
        else:
            # SIGTERM kills should cause a shutdown which will later cause a 'goodbye' notification back to this manager.
            if force:
                dead = True  # Consider it gone

        return {"killed": dead}

    #endregion Service interface helpers
